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
const maxRoomDelta = Number(process.env.SFU_REGRESSION_MAX_ROOM_DELTA ?? 0);
const maxTrackDelta = Number(process.env.SFU_REGRESSION_MAX_TRACK_DELTA ?? 0);
const maxResourceGrowth = Number(process.env.SFU_REGRESSION_MAX_RESOURCE_GROWTH ?? 0.2);
const artifactDir = process.env.SFU_ARTIFACT_DIR ?? "artifacts";

type Metrics = Record<string, number>;
type Connection = { connectionId: string; peerId: string; trackIds: string[] };

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

async function join(page: import("@playwright/test").Page, endpoint: string, roomId: string, peerId?: string): Promise<Connection> {
  if (page.url() === "about:blank") await page.goto(process.env.BASE_URL ?? "http://localhost:5173");
  return page.evaluate(async ({ endpoint, roomId, accessToken, peerId }) => {
    const store = (globalThis as any).__sfuRegressionConnections ??= new Map();
    const connectionId = crypto.randomUUID?.() ?? Array.from(crypto.getRandomValues(new Uint8Array(16)), value => value.toString(16).padStart(2, "0")).join("");
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

    store.set(connectionId, { pc, stream, ws });
    return {
      connectionId,
      peerId: joined.peerId,
      trackIds: stream.getTracks().map(track => track.id)
    };
  }, { endpoint, roomId, accessToken: token(roomId, "regression"), peerId });
}

async function closeConnection(page: import("@playwright/test").Page, connectionId: string) {
  await page.evaluate((id) => {
    const store = (globalThis as any).__sfuRegressionConnections;
    const connection = store?.get(id);
    if (!connection) return;
    connection.stream.getTracks().forEach((track: MediaStreamTrack) => track.stop());
    connection.ws.close();
    connection.pc.close();
    store.delete(id);
  }, connectionId);
}

function percentile(values: number[], p: number) {
  const sorted = [...values].sort((a, b) => a - b);
  return sorted[Math.min(sorted.length - 1, Math.max(0, Math.ceil(sorted.length * p) - 1))];
}

function trackedResources(snapshot: Metrics) {
  const names = [
    "yazykon_media_active_rooms",
    "yazykon_media_active_peers",
    "yazykon_media_active_tracks",
    "yazykon_media_goroutines",
    "yazykon_media_heap_bytes",
    "go_goroutines",
    "go_memstats_heap_alloc_bytes",
    "process_resident_memory_bytes"
  ];
  return Object.fromEntries(names.filter(name => Number.isFinite(snapshot[name])).map(name => [name, snapshot[name]]));
}

test("SFU chaos regression keeps recovery latency and resource state bounded", async ({ page }) => {
  test.skip(!process.env.SFU_FAILOVER_LIVE, "Set SFU_FAILOVER_LIVE=1 for the live Docker run");

  const before = await Promise.all([metrics(primary), metrics(secondary)]);
  const roomId = "SFU-REGRESSION-" + Date.now();
  const recoveryMs: number[] = [];
  let active: Connection | undefined;

  try {
    await waitHealth(primary);
    await waitHealth(secondary);

    active = await join(page, primary, roomId);
    expect(active.peerId).toBeTruthy();

    for (let i = 0; i < rounds; i++) {
      const previous = active;
      const started = Date.now();
      execFileSync("docker", ["compose", "-f", composeFile, "stop", "sfu-primary"], { stdio: "inherit" });

      const recovered = await join(page, secondary, roomId, previous.peerId);
      const elapsed = Date.now() - started;
      recoveryMs.push(elapsed);

      expect(recovered.peerId).toBe(previous.peerId);
      expect(recovered.trackIds.length).toBeGreaterThan(0);
      await closeConnection(page, previous.connectionId);
      active = recovered;

      execFileSync("docker", ["compose", "-f", composeFile, "start", "sfu-primary"], { stdio: "inherit" });
      await waitHealth(primary);
    }

    if (active) await closeConnection(page, active.connectionId);
    active = undefined;

    await new Promise(resolve => setTimeout(resolve, 1000));
    const after = await Promise.all([metrics(primary), metrics(secondary)]);
    const latency = {
      p50: percentile(recoveryMs, 0.50),
      p95: percentile(recoveryMs, 0.95),
      p99: percentile(recoveryMs, 0.99),
      max: Math.max(...recoveryMs),
      min: Math.min(...recoveryMs)
    };

    const resourceBefore = before.map(trackedResources);
    const resourceAfter = after.map(trackedResources);
    const resourceDelta = after.map((snapshot, i) => {
      const baseline = resourceBefore[i];
      const current = resourceAfter[i];
      const result: Record<string, { before: number; after: number; delta: number; growth: number | null }> = {};
      for (const name of Object.keys(current)) {
        const b = baseline[name];
        const a = current[name];
        if (!Number.isFinite(b) || !Number.isFinite(a)) continue;
        result[name] = {
          before: b,
          after: a,
          delta: a - b,
          growth: b > 0 ? (a - b) / b : null
        };
      }
      return result;
    });

    const activePeerDelta = resourceDelta.map(s => s.yazykon_media_active_peers?.delta ?? 0);
    const activeRoomDelta = resourceDelta.map(s => s.yazykon_media_active_rooms?.delta ?? 0);
    const activeTrackDelta = resourceDelta.map(s => s.yazykon_media_active_tracks?.delta ?? 0);
    const boundedResourceGrowth = resourceDelta.flatMap(snapshot =>
      Object.entries(snapshot)
        .filter(([name, value]) =>
          ["yazykon_media_goroutines", "yazykon_media_heap_bytes", "go_goroutines", "go_memstats_heap_alloc_bytes", "process_resident_memory_bytes"].includes(name) &&
          value.growth !== null
        )
        .map(([name, value]) => ({ name, growth: value.growth as number }))
    );

    const report = {
      generatedAt: new Date().toISOString(),
      roomId,
      cycles: rounds,
      recoveryMs: { samples: recoveryMs, ...latency },
      slaMs,
      resources: { before: resourceBefore, after: resourceAfter, delta: resourceDelta },
      budgets: {
        maxActivePeerDelta,
        maxRoomDelta,
        maxTrackDelta,
        maxResourceGrowth,
        activePeerDelta,
        activeRoomDelta,
        activeTrackDelta,
        resourceGrowth: boundedResourceGrowth
      },
      pass:
        latency.p95 <= slaMs &&
        latency.p99 <= slaMs &&
        latency.max <= slaMs &&
        activePeerDelta.every(delta => delta <= maxActiveDelta) &&
        activeRoomDelta.every(delta => delta <= maxRoomDelta) &&
        activeTrackDelta.every(delta => delta <= maxTrackDelta) &&
        boundedResourceGrowth.every(item => item.growth <= maxResourceGrowth)
    };

    fs.mkdirSync(artifactDir, { recursive: true });
    fs.writeFileSync(
      path.join(artifactDir, "sfu-chaos-regression-report.json"),
      JSON.stringify(report, null, 2)
    );

    expect(latency.p95).toBeLessThanOrEqual(slaMs);
    expect(latency.p99).toBeLessThanOrEqual(slaMs);
    expect(latency.max).toBeLessThanOrEqual(slaMs);
    for (const delta of activePeerDelta) expect(delta).toBeLessThanOrEqual(maxActiveDelta);
    for (const delta of activeRoomDelta) expect(delta).toBeLessThanOrEqual(maxRoomDelta);
    for (const delta of activeTrackDelta) expect(delta).toBeLessThanOrEqual(maxTrackDelta);
    for (const item of boundedResourceGrowth) expect(item.growth).toBeLessThanOrEqual(maxResourceGrowth);
  } finally {
    if (active) await closeConnection(page, active.connectionId).catch(() => {});
    execFileSync("docker", ["compose", "-f", composeFile, "start", "sfu-primary"], { stdio: "inherit" });
    await waitHealth(primary).catch(() => {});
  }
});
