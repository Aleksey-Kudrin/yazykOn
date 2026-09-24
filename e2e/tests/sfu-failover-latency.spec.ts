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
  await page.goto(primary + "/health");

  const connected = await page.evaluate(async ({ endpoint, roomId, accessToken }) => {
    const stream = await navigator.mediaDevices.getUserMedia({ video: true, audio: false });
    const pc = new RTCPeerConnection();
    for (const track of stream.getTracks()) pc.addTrack(track, stream);

    const ws = new WebSocket(endpoint.replace(/^http/, "ws") + "/ws");
    const wait = (type: string) => new Promise<any>((resolve, reject) => {
      const timer = setTimeout(() => reject(new Error("timeout: " + type)), 15000);
      ws.addEventListener("message", event => {
        const message = JSON.parse(event.data);
        if (message.type === type) {
          clearTimeout(timer);
          resolve(message);
        }
      });
    });

    await new Promise<void>((resolve, reject) => {
      ws.onopen = () => resolve();
      ws.onerror = () => reject(new Error("websocket failed"));
    });
    const started = performance.now();
    ws.send(JSON.stringify({
      type: "join",
      roomId,
      data: { accessToken, reconnect: false }
    }));
    const joined = await wait("joined");
    const offer = await pc.createOffer();
    await pc.setLocalDescription(offer);
    ws.send(JSON.stringify({ type: "offer", roomId, data: pc.localDescription }));
    await wait("answer");

    return { peerId: joined.peerId, initialMs: performance.now() - started };
  }, { endpoint: primary, roomId, accessToken: token("p1") });

  expect(connected.peerId).toBeTruthy();

  const recoverySamples: number[] = [];

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
    const stream = await navigator.mediaDevices.getUserMedia({ video: true, audio: false });
    const pc = new RTCPeerConnection();
    for (const track of stream.getTracks()) pc.addTrack(track);
    const ws = new WebSocket(endpoint.replace(/^http/, "ws") + "/ws");
    const wait = (type: string) => new Promise<any>((resolve, reject) => {
      const timer = setTimeout(() => reject(new Error("timeout: " + type)), 15000);
      ws.addEventListener("message", event => {
        const message = JSON.parse(event.data);
        if (message.type === type) { clearTimeout(timer); resolve(message); }
      });
    });
    await new Promise<void>((resolve, reject) => {
      ws.onopen = () => resolve();
      ws.onerror = () => reject(new Error("websocket failed"));
    });
    ws.send(JSON.stringify({
      type: "join", roomId, peerId,
      data: { accessToken, reconnect: true }
    }));
    const joined = await wait("joined");
    const offer = await pc.createOffer();
    await pc.setLocalDescription(offer);
    ws.send(JSON.stringify({ type: "offer", roomId, data: pc.localDescription }));
    await wait("answer");
    return { peerId: joined.peerId };
  }, { endpoint: secondary, roomId, accessToken: token("p1"), peerId: connected.peerId });

    const recoveryMs = Date.now() - startedAt;
    recoverySamples.push(recoveryMs);
    expect(recovered.peerId).toBe(connected.peerId);

    await page.evaluate(() => undefined);
    execFileSync("docker", ["compose", "-f", composeFile, "start", "sfu-primary"], { stdio: "inherit" });
    for (let i = 0; i < 40; i++) {
      try {
        const response = await fetch(primary + "/health");
        if (response.ok) break;
      } catch {}
      await new Promise(resolve => setTimeout(resolve, 250));
    }
  }

  const sorted = [...recoverySamples].sort((a, b) => a - b);
  const percentile = (p: number) => sorted[Math.min(sorted.length - 1, Math.ceil(sorted.length * p) - 1)];
  const report = {
    generatedAt: new Date().toISOString(), roomId, cycles,
    recoveryMs: { samples: recoverySamples, p50: percentile(0.5), p95: percentile(0.95), p99: percentile(0.99), max: Math.max(...recoverySamples) },
    slaMs,
    pass: Math.max(...recoverySamples) <= slaMs
  };
  fs.mkdirSync(artifactDir, { recursive: true });
  fs.writeFileSync(path.join(artifactDir, "sfu-failover-latency-report.json"), JSON.stringify(report, null, 2));
  expect(report.pass).toBeTruthy();

  console.log(JSON.stringify({ initialConnectMs: Math.round(connected.initialMs), recoverySamples, roomId, cycles }));
  await page.close();
});
