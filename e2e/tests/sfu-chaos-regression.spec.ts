import { execFileSync } from "node:child_process";
import { createHmac } from "node:crypto";
import fs from "node:fs";
import path from "node:path";
import { test, expect } from "@playwright/test";

const primary = process.env.SFU_PRIMARY_URL ?? "http://127.0.0.1:4100";
const secondary = process.env.SFU_SECONDARY_URL ?? "http://127.0.0.1:4200";
const composeFile = process.env.SFU_FAILOVER_COMPOSE ?? "e2e/docker-compose.sfu-failover.yml";
const secret = process.env.ROOM_ACCESS_SECRET ?? "integration-secret";
const rounds = Math.max(3, Number(process.env.SFU_REGRESSION_CYCLES ?? 4));
const slaMs = Number(process.env.SFU_FAILOVER_SLA_MS ?? 30000);
const maxActiveDelta = Number(process.env.SFU_REGRESSION_MAX_ACTIVE_DELTA ?? 1);
const artifactDir = process.env.SFU_ARTIFACT_DIR ?? "artifacts";

type Metrics = Record<string, number>;

async function metrics(endpoint: string): Promise<Metrics> {
  const response = await fetch(endpoint + "/metrics");
  expect(response.ok).toBeTruthy();
  const result: Metrics = {};
  for (const line of (await response.text()).split("\n")) {
    if (!line || line.startsWith("#")) continue;
    const [name, value] = line.trim().split(/\s+/);
    const numeric = Number(value);
    if (name && Number.isFinite(numeric)) result[name] = numeric;
  }
  return result;
}

function token(roomId: string, userId: string) {
  const payload = Buffer.from(JSON.stringify({
    roomId, userId, role: "host", exp: Math.floor(Date.now() / 1000) + 600
  })).toString("base64url");
  return payload + "." + createHmac("sha256", secret).update(payload).digest("base64url");
}

async function waitHealth(endpoint: string) {
  for (let i = 0; i < 40; i++) {
    try {
      const response = await fetch(endpoint + "/health");
      if (response.ok) return;
    } catch {}
    await new Promise(resolve => setTimeout(resolve, 250));
  }
  throw new Error("SFU health timeout: " + endpoint);
}

async function join(page: import("@playwright/test").Page, endpoint: string, roomId: string, peerId?: string) {
  return await page.evaluate(async ({ endpoint, roomId, accessToken, peerId }) => {
    const pc = new RTCPeerConnection();
    const stream = await navigator.mediaDevices.getUserMedia({ video: true, audio: false });
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

    ws.send(JSON.stringify({
      type: "join",
      roomId,
      ...(peerId ? { peerId } : {}),
      data: { accessToken, reconnect: Boolean(peerId) }
    }));
    const joined = await wait("joined");

    const offer = await pc.createOffer();
    await pc.setLocalDescription(offer);
    ws.send(JSON.stringify({ type: "offer", roomId, data: pc.localDescription }));
    await wait("answer");

    return {
      peerId: joined.peerId,
      trackIds: stream.getTracks().map(track => track.id),
      close: () => {
        for (const track of stream.getTracks()) track.stop();
        ws.close();
        pc.close();
      }
    };
  }, { endpoint, roomId, accessToken: token(roomId, "regression"), peerId });
}

test("SFU chaos regression keeps recovery latency and active-state bounded", async ({ page }) => {
  test.skip(!process.env.SFU_FAILOVER_LIVE, "Set SFU_FAILOVER_LIVE=1 for the live Docker run");

  await page.goto(primary + "/health");
  const before = await Promise.all([metrics(primary), metrics(secondary)]);
  const roomId = "SFU-REGRESSION-" + Date.now();
  const recoveryMs: number[] = [];
  let peerId: string | undefined;

  try {
    await waitHealth(primary);
    await waitHealth(secondary);

    const initial = await join(page, primary, roomId);
    peerId = initial.peerId;
    expect(peerId).toBeTruthy();

    for (let i = 0; i < rounds; i++) {
      const started = Date.now();
      execFileSync("docker", ["compose", "-f", composeFile, "stop", "sfu-primary"], { stdio: "inherit" });

      const recovered = await join(page, secondary, roomId, peerId);
      const elapsed = Date.now() - started;
      recoveryMs.push(elapsed);
      expect(recovered.peerId).toBe(peerId);
      recovered.close();

      execFileSync("docker", ["compose", "-f", composeFile, "start", "sfu-primary"], { stdio: "inherit" });
      await waitHealth(primary);
    }

    initial.close();
    await new Promise(resolve => setTimeout(resolve, 1000));
    const after = await Promise.all([metrics(primary), metrics(secondary)]);
    const percentile = (values: number[], p: number) => {
      const sorted = [...values].sort((a, b) => a - b);
      return sorted[Math.min(sorted.length - 1, Math.ceil(values.length * p) - 1)];
    };

    const p95 = percentile(recoveryMs, 0.95);
    const max = Math.max(...recoveryMs);
    const activeBefore = before.map(m => m.yazykon_media_active_peers ?? 0);
    const activeAfter = after.map(m => m.yazykon_media_active_peers ?? 0);
    const activeDelta = after.map((m, i) => (m.yazykon_media_active_peers ?? 0) - activeBefore[i]);

    const report = {
      generatedAt: new Date().toISOString(),
      roomId,
      cycles: rounds,
      recoveryMs: {
        samples: recoveryMs,
        p95,
        max
      },
      slaMs,
      activePeers: {
        before: activeBefore,
        after: activeAfter,
        delta: activeDelta,
        maxAllowedDelta: maxActiveDelta
      },
      pass: p95 <= slaMs && max <= slaMs && activeDelta.every(delta => delta <= maxActiveDelta)
    };

    fs.mkdirSync(artifactDir, { recursive: true });
    fs.writeFileSync(
      path.join(artifactDir, "sfu-chaos-regression-report.json"),
      JSON.stringify(report, null, 2)
    );

    expect(p95).toBeLessThanOrEqual(slaMs);
    expect(max).toBeLessThanOrEqual(slaMs);
    for (const delta of activeDelta) {
      expect(delta).toBeLessThanOrEqual(maxActiveDelta);
    }
  } finally {
    execFileSync("docker", ["compose", "-f", composeFile, "start", "sfu-primary"], { stdio: "inherit" });
    await waitHealth(primary).catch(() => {});
  }
});
