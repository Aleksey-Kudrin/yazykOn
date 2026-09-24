import { execFileSync } from "node:child_process";
import { createHmac } from "node:crypto";
import { test, expect } from "@playwright/test";
import fs from "node:fs";
import path from "node:path";

const primary = process.env.SFU_PRIMARY_URL ?? "http://127.0.0.1:4100";
const secondary = process.env.SFU_SECONDARY_URL ?? "http://127.0.0.1:4200";
const composeFile = process.env.SFU_FAILOVER_COMPOSE ?? "e2e/docker-compose.sfu-failover.yml";
const roomId = process.env.SFU_LATENCY_ROOM ?? "SFU-LATENCY";
const secret = process.env.ROOM_ACCESS_SECRET ?? "integration-secret";
const ttlMs = Number(process.env.SFU_ROOM_OWNER_TTL_MS ?? 3000);
const cycles = Math.max(1, Number(process.env.SFU_LATENCY_CYCLES ?? 4));
const slaMs = Number(process.env.SFU_FAILOVER_SLA_MS ?? 30000);
const artifactDir = process.env.SFU_ARTIFACT_DIR ?? "artifacts";

function token(userId: string) {
  const payload = Buffer.from(JSON.stringify({
    roomId, userId, role: userId === "p1" ? "host" : "participant",
    exp: Math.floor(Date.now() / 1000) + 600
  })).toString("base64url");
  return payload + "." + createHmac("sha256", secret).update(payload).digest("base64url");
}

async function health(endpoint: string) {
  const response = await fetch(endpoint + "/health");
  expect(response.ok).toBeTruthy();
  return response.json();
}

test("SFU failover recovery latency is measured end-to-end", async ({ browser }) => {
  test.skip(!process.env.SFU_FAILOVER_LIVE, "Set SFU_FAILOVER_LIVE=1 for the live Docker run");

  const page = await browser.newPage({ permissions: ["camera", "microphone"] });
  await health(primary);

  const connected = await page.evaluate(async ({ endpoint, roomId, accessToken }) => {
    const stream = (globalThis as any).__latencyStream as MediaStream | undefined
      ?? await navigator.mediaDevices.getUserMedia({ video: true, audio: false });
    (globalThis as any).__latencyStream = stream;

    async function connect(target: string, reconnectPeerId = "") {
      const pc = new RTCPeerConnection();
      for (const track of stream.getTracks()) pc.addTrack(track, stream);
      const ws = new WebSocket(target.replace(/^http/, "ws") + "/ws");
      const queue: any[] = [];
      const waitFor = (type: string) => new Promise<any>((resolve, reject) => {
        const take = (message: any) => {
          if (message.type === type) {
            clearTimeout(timer);
            resolve(message);
          } else queue.push(message);
        };
        const timer = setTimeout(() => reject(new Error("timeout: " + type)), 15000);
        const index = queue.findIndex(message => message.type === type);
        if (index >= 0) {
          clearTimeout(timer);
          resolve(queue.splice(index, 1)[0]);
        } else ws.addEventListener("message", event => take(JSON.parse(event.data)));
      });

      await new Promise<void>((resolve, reject) => {
        ws.onopen = () => resolve();
        ws.onerror = () => reject(new Error("websocket failed"));
      });
      ws.send(JSON.stringify({
        type: "join",
        roomId,
        peerId: reconnectPeerId || undefined,
        data: { accessToken, reconnect: Boolean(reconnectPeerId) }
      }));
      const joined = await waitFor("joined");
      const offer = await pc.createOffer();
      await pc.setLocalDescription(offer);
      ws.send(JSON.stringify({ type: "offer", roomId, data: pc.localDescription }));
      const answer = await waitFor("answer");
      if (answer.data) await pc.setRemoteDescription(answer.data);
      return { peerId: joined.peerId, pc, ws };
    }

    const started = performance.now();
    const first = await connect(endpoint);
    (globalThis as any).__latencyConnection = first;
    return {
      peerId: first.peerId,
      initialMs: performance.now() - started,
      trackId: stream.getVideoTracks()[0]?.id ?? ""
    };
  }, { endpoint: primary, roomId, accessToken: token("p1") });

  expect(connected.peerId).toBeTruthy();
  expect(connected.trackId).toBeTruthy();

  const recoverySamples: number[] = [];
  const beforeMetrics = await fetch(primary + "/metrics").then(response => response.ok ? response.text() : "");

  try {
    for (let cycle = 0; cycle < cycles; cycle++) {
      const startedAt = Date.now();
      execFileSync("docker", ["compose", "-f", composeFile, "stop", "sfu-primary"], { stdio: "inherit" });

      let targetReady = false;
      for (let i = 0; i < 40; i++) {
        try {
          const response = await fetch(secondary + "/health");
          if (response.ok) {
            targetReady = true;
            break;
          }
        } catch {}
        await new Promise(resolve => setTimeout(resolve, 250));
      }
      expect(targetReady).toBeTruthy();

      await new Promise(resolve => setTimeout(resolve, ttlMs + 1000));

      const recovered = await page.evaluate(async ({ endpoint, roomId, accessToken, peerId }) => {
        const previous = (globalThis as any).__latencyConnection;
        if (previous) {
          previous.ws.close();
          previous.pc.close();
        }
        const stream = (globalThis as any).__latencyStream as MediaStream;
        const pc = new RTCPeerConnection();
        for (const track of stream.getTracks()) pc.addTrack(track, stream);
        const ws = new WebSocket(endpoint.replace(/^http/, "ws") + "/ws");
        const waitFor = (type: string) => new Promise<any>((resolve, reject) => {
          const timer = setTimeout(() => reject(new Error("timeout: " + type)), 15000);
          const handler = (event: MessageEvent) => {
            const message = JSON.parse(event.data);
            if (message.type === type) {
              clearTimeout(timer);
              ws.removeEventListener("message", handler);
              resolve(message);
            }
          };
          ws.addEventListener("message", handler);
        });
        await new Promise<void>((resolve, reject) => {
          ws.onopen = () => resolve();
          ws.onerror = () => reject(new Error("websocket failed"));
        });
        ws.send(JSON.stringify({
          type: "join", roomId, peerId,
          data: { accessToken, reconnect: true }
        }));
        const joined = await waitFor("joined");
        const offer = await pc.createOffer();
        await pc.setLocalDescription(offer);
        ws.send(JSON.stringify({ type: "offer", roomId, data: pc.localDescription }));
        const answer = await waitFor("answer");
        if (answer.data) await pc.setRemoteDescription(answer.data);
        (globalThis as any).__latencyConnection = { pc, ws };
        return { peerId: joined.peerId, trackId: stream.getVideoTracks()[0]?.id ?? "" };
      }, { endpoint: secondary, roomId, accessToken: token("p1"), peerId: connected.peerId });

      const recoveryMs = Date.now() - startedAt;
      recoverySamples.push(recoveryMs);
      expect(recovered.peerId).toBe(connected.peerId);
      expect(recovered.trackId).toBe(connected.trackId);

      execFileSync("docker", ["compose", "-f", composeFile, "start", "sfu-primary"], { stdio: "inherit" });
      for (let i = 0; i < 40; i++) {
        try {
          if ((await fetch(primary + "/health")).ok) break;
        } catch {}
        await new Promise(resolve => setTimeout(resolve, 250));
      }
    }
  } finally {
    await page.evaluate(() => {
      const connection = (globalThis as any).__latencyConnection;
      connection?.ws?.close();
      connection?.pc?.close();
      (globalThis as any).__latencyStream?.getTracks().forEach((track: MediaStreamTrack) => track.stop());
    }).catch(() => {});
    try {
      execFileSync("docker", ["compose", "-f", composeFile, "start", "sfu-primary", "sfu-secondary"], { stdio: "inherit" });
    } catch {}
    await page.close();
  }

  const afterMetrics = await fetch(primary + "/metrics").then(response => response.ok ? response.text() : "");
  const sorted = [...recoverySamples].sort((a, b) => a - b);
  const percentile = (p: number) => sorted[Math.min(sorted.length - 1, Math.ceil(sorted.length * p) - 1)];
  const report = {
    generatedAt: new Date().toISOString(), roomId, cycles,
    recoveryMs: { samples: recoverySamples, p50: percentile(0.5), p95: percentile(0.95), p99: percentile(0.99), max: Math.max(...recoverySamples) },
    slaMs,
    pass: recoverySamples.length === cycles && Math.max(...recoverySamples) <= slaMs,
    trackStable: connected.trackId.length > 0,
    metrics: { before: beforeMetrics, after: afterMetrics }
  };
  fs.mkdirSync(artifactDir, { recursive: true });
  fs.writeFileSync(path.join(artifactDir, "sfu-failover-latency-report.json"), JSON.stringify(report, null, 2));
  expect(report.pass).toBeTruthy();

  console.log(JSON.stringify({ initialConnectMs: Math.round(connected.initialMs), recoverySamples, roomId, cycles }));
});
