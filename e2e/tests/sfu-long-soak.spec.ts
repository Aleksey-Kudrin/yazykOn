import { createHmac } from "node:crypto";
import { execFileSync } from "node:child_process";
import fs from "node:fs";
import path from "node:path";
import { test, expect } from "@playwright/test";

const primary = process.env.SFU_PRIMARY_URL ?? "http://127.0.0.1:4100";
const secondary = process.env.SFU_SECONDARY_URL ?? "http://127.0.0.1:4200";
const composeFile = process.env.SFU_FAILOVER_COMPOSE ?? "e2e/docker-compose.sfu-failover.yml";
const secret = process.env.ROOM_ACCESS_SECRET ?? "integration-secret";
const durationMs = Math.max(10000, Number(process.env.SFU_SOAK_DURATION_MS ?? 120000));
const sampleMs = Math.max(250, Number(process.env.SFU_SOAK_SAMPLE_MS ?? 2000));
const cycleMs = Math.max(1000, Number(process.env.SFU_SOAK_CYCLE_MS ?? 10000));
const slaMs = Number(process.env.SFU_FAILOVER_SLA_MS ?? 30000);
const maxFailoverErrorRate = Number(process.env.SFU_SOAK_MAX_ERROR_RATE ?? 0.01);
const maxSampleErrorRate = Number(process.env.SFU_SOAK_MAX_SAMPLE_ERROR_RATE ?? 0.01);
const maxResourceGrowth = Number(process.env.SFU_SOAK_MAX_RESOURCE_GROWTH ?? 0.20);
const artifactDir = process.env.SFU_ARTIFACT_DIR ?? "artifacts";

type Metrics = Record<string, number>;
type Sample = { at: string; primary: Metrics; secondary: Metrics };
type Connection = { connectionId: string; peerId: string; trackIds: string[] };

function token(roomId: string, userId: string) {
  const payload = Buffer.from(JSON.stringify({
    roomId, userId, role: "host", exp: Math.floor(Date.now() / 1000) + 600
  })).toString("base64url");
  return payload + "." + createHmac("sha256", secret).update(payload).digest("base64url");
}

async function metrics(endpoint: string): Promise<Metrics> {
  const response = await fetch(endpoint + "/metrics");
  if (!response.ok) throw new Error("metrics unavailable: " + endpoint);
  const out: Metrics = {};
  for (const line of (await response.text()).split("\n")) {
    if (!line || line.startsWith("#")) continue;
    const [name, value] = line.trim().split(/\s+/);
    const numeric = Number(value);
    if (name && Number.isFinite(numeric)) out[name] = numeric;
  }
  return out;
}

async function waitHealth(endpoint: string) {
  for (let i = 0; i < 60; i++) {
    try {
      const response = await fetch(endpoint + "/health");
      if (response.ok) return;
    } catch {}
    await new Promise(resolve => setTimeout(resolve, 250));
  }
  throw new Error("health timeout: " + endpoint);
}

function delta(a: Metrics, b: Metrics, name: string) {
  return (b[name] ?? 0) - (a[name] ?? 0);
}

function growth(before: number, after: number) {
  if (!Number.isFinite(before) || !Number.isFinite(after) || before <= 0) return null;
  return (after - before) / before;
}

async function join(page: import("@playwright/test").Page, endpoint: string, roomId: string, peerId?: string): Promise<Connection> {
  return page.evaluate(async ({ endpoint, roomId, peerId, accessToken }) => {
    const registry = (globalThis as any).__sfuLongSoakConnections ??= new Map<string, any>();
    const connectionId = crypto.randomUUID();
    const ws = new WebSocket(endpoint.replace(/^http/, "ws") + "/ws");
    const pc = new RTCPeerConnection();
    const stream = await navigator.mediaDevices.getUserMedia({ audio: true, video: true });
    for (const track of stream.getTracks()) pc.addTrack(track, stream);

    const joined = new Promise<any>((resolve, reject) => {
      const timer = setTimeout(() => reject(new Error("join timeout")), 15000);
      ws.onopen = () => ws.send(JSON.stringify({
        type: "join",
        roomId,
        data: { accessToken, reconnect: Boolean(peerId), ...(peerId ? { peerId } : {}) }
      }));
      ws.onerror = () => { clearTimeout(timer); reject(new Error("websocket failed")); };
      ws.onmessage = event => {
        const message = JSON.parse(event.data);
        if (message.type === "joined") {
          clearTimeout(timer);
          resolve(message);
        }
      };
    });

    const joinedMessage = await joined;
    const stablePeerId = peerId ?? joinedMessage.peerId;
    const trackIds = stream.getTracks().map(track => track.id);
    registry.set(connectionId, { ws, pc, stream, peerId: stablePeerId, trackIds });

    return { connectionId, peerId: stablePeerId, trackIds };
  }, { endpoint, roomId, peerId, accessToken: token(roomId, "soak-" + Date.now()) });
}

async function closeConnection(page: import("@playwright/test").Page, connectionId?: string) {
  if (!connectionId) return;
  await page.evaluate(async (id) => {
    const registry = (globalThis as any).__sfuLongSoakConnections;
    const connection = registry?.get(id);
    if (!connection) return;
    for (const track of connection.stream.getTracks()) track.stop();
    connection.ws.close();
    connection.pc.close();
    registry.delete(id);
  }, connectionId);
}

function slope(first: number | null, last: number | null, elapsedMs: number) {
  if (first === null || last === null || elapsedMs <= 0) return null;
  return (last - first) / elapsedMs;
}

test("SFU long soak keeps WebRTC recovery, health and runtime state bounded", async ({ page }) => {
  test.skip(!process.env.SFU_FAILOVER_LIVE, "Set SFU_FAILOVER_LIVE=1 for the live Docker run");

  const started = Date.now();
  const deadline = started + durationMs;
  const samples: Sample[] = [];
  const failovers: Array<{
    cycle: number; source: string; target: string; recoveryMs: number;
    healthOk: boolean; reconnectOk: boolean; peerStable: boolean; tracksStable: boolean; ok: boolean;
  }> = [];
  let sampleErrors = 0;
  let sampleAttempts = 0;
  let cycle = 0;
  let active: Connection | undefined;
  let fatalError: string | undefined;
  const roomId = "SOAK-" + Date.now();

  const sample = async () => {
    sampleAttempts++;
    try {
      const [p, s] = await Promise.all([metrics(primary), metrics(secondary)]);
      samples.push({ at: new Date().toISOString(), primary: p, secondary: s });
    } catch {
      sampleErrors++;
    }
  };

  const restore = async () => {
    try { execFileSync("docker", ["compose", "-f", composeFile, "start", "sfu-primary"], { stdio: "inherit" }); } catch {}
    try { execFileSync("docker", ["compose", "-f", composeFile, "start", "sfu-secondary"], { stdio: "inherit" }); } catch {}
    await waitHealth(primary).catch(() => {});
    await waitHealth(secondary).catch(() => {});
  };

  try {
    await waitHealth(primary);
    await waitHealth(secondary);
    await sample();

    active = await join(page, primary, roomId);
    while (Date.now() < deadline) {
      const target = cycle % 2 === 0 ? secondary : primary;
      const source = cycle % 2 === 0 ? primary : secondary;
      const failed = cycle % 2 === 0 ? "sfu-primary" : "sfu-secondary";
      const old = active;
      const failoverStarted = Date.now();

      execFileSync("docker", ["compose", "-f", composeFile, "stop", failed], { stdio: "inherit" });

      let healthOk = false;
      try {
        await waitHealth(target);
        healthOk = true;
      } catch {}

      let reconnect: Connection | undefined;
      let reconnectOk = false;
      let peerStable = false;
      let tracksStable = false;
      try {
        reconnect = await join(page, target, roomId, old.peerId);
        reconnectOk = true;
        peerStable = reconnect.peerId === old.peerId;
        tracksStable = reconnect.trackIds.length === old.trackIds.length &&
          reconnect.trackIds.every(id => old.trackIds.includes(id));
      } catch {}

      await closeConnection(page, old.connectionId);
      active = reconnect;
      const recoveryMs = Date.now() - failoverStarted;
      const ok = healthOk && reconnectOk && peerStable && tracksStable && recoveryMs <= slaMs;
      failovers.push({
        cycle, source, target, recoveryMs, healthOk, reconnectOk,
        peerStable, tracksStable, ok
      });

      try {
        execFileSync("docker", ["compose", "-f", composeFile, "start", failed], { stdio: "inherit" });
        await waitHealth(source);
      } catch (error) {
        fatalError ??= String(error);
        break;
      }

      await sample();
      const holdUntil = Math.min(deadline, Date.now() + cycleMs);
      while (Date.now() < holdUntil) {
        await new Promise(resolve => setTimeout(resolve, Math.min(sampleMs, holdUntil - Date.now())));
        if (Date.now() < deadline) await sample();
      }
      cycle++;
    }
  } catch (error) {
    fatalError = String(error);
  } finally {
    await closeConnection(page, active?.connectionId).catch(() => {});
    await restore();
    await sample();
  }

  const failedFailovers = failovers.filter(item => !item.ok);
  const failoverErrorRate = failovers.length ? failedFailovers.length / failovers.length : 1;
  const sampleErrorRate = sampleAttempts ? sampleErrors / sampleAttempts : 1;
  const recoveries = failovers.map(item => item.recoveryMs);
  const maxRecoveryMs = recoveries.length ? Math.max(...recoveries) : 0;
  const first = samples[0];
  const last = samples[samples.length - 1];
  const elapsedMs = Math.max(1, (first && last) ? Date.parse(last.at) - Date.parse(first.at) : Date.now() - started);

  const runtimeGrowth = first && last ? {
    primaryGoroutinesDelta: delta(first.primary, last.primary, "yazykon_media_goroutines"),
    secondaryGoroutinesDelta: delta(first.secondary, last.secondary, "yazykon_media_goroutines"),
    primaryHeapDelta: delta(first.primary, last.primary, "yazykon_media_heap_bytes"),
    secondaryHeapDelta: delta(first.secondary, last.secondary, "yazykon_media_heap_bytes"),
    primaryRoomsDelta: delta(first.primary, last.primary, "yazykon_media_active_rooms"),
    secondaryRoomsDelta: delta(first.secondary, last.secondary, "yazykon_media_active_rooms"),
    primaryPeersDelta: delta(first.primary, last.primary, "yazykon_media_active_peers"),
    secondaryPeersDelta: delta(first.secondary, last.secondary, "yazykon_media_active_peers"),
    primaryGoroutinesGrowth: growth(first.primary.yazykon_media_goroutines ?? 0, last.primary.yazykon_media_goroutines ?? 0),
    secondaryGoroutinesGrowth: growth(first.secondary.yazykon_media_goroutines ?? 0, last.secondary.yazykon_media_goroutines ?? 0),
    primaryHeapGrowth: growth(first.primary.yazykon_media_heap_bytes ?? 0, last.primary.yazykon_media_heap_bytes ?? 0),
    secondaryHeapGrowth: growth(first.secondary.yazykon_media_heap_bytes ?? 0, last.secondary.yazykon_media_heap_bytes ?? 0),
    primaryGoroutinesSlope: slope(first.primary.yazykon_media_goroutines ?? null, last.primary.yazykon_media_goroutines ?? null, elapsedMs),
    secondaryGoroutinesSlope: slope(first.secondary.yazykon_media_goroutines ?? null, last.secondary.yazykon_media_goroutines ?? null, elapsedMs),
    primaryHeapSlope: slope(first.primary.yazykon_media_heap_bytes ?? null, last.primary.yazykon_media_heap_bytes ?? null, elapsedMs),
    secondaryHeapSlope: slope(first.secondary.yazykon_media_heap_bytes ?? null, last.secondary.yazykon_media_heap_bytes ?? null, elapsedMs)
  } : null;

  const requiredMetrics = [
    "yazykon_media_goroutines", "yazykon_media_heap_bytes",
    "yazykon_media_active_peers", "yazykon_media_active_rooms"
  ];
  const missingMetrics = first ? requiredMetrics.filter(name =>
    first.primary[name] === undefined || first.secondary[name] === undefined) : requiredMetrics;

  const metricExtremes = samples.length ? {
    primaryMaxGoroutines: Math.max(...samples.map(s => s.primary.yazykon_media_goroutines ?? 0)),
    secondaryMaxGoroutines: Math.max(...samples.map(s => s.secondary.yazykon_media_goroutines ?? 0)),
    primaryMaxHeap: Math.max(...samples.map(s => s.primary.yazykon_media_heap_bytes ?? 0)),
    secondaryMaxHeap: Math.max(...samples.map(s => s.secondary.yazykon_media_heap_bytes ?? 0)),
    primaryMaxPeers: Math.max(...samples.map(s => s.primary.yazykon_media_active_peers ?? 0)),
    secondaryMaxPeers: Math.max(...samples.map(s => s.secondary.yazykon_media_active_peers ?? 0)),
    primaryMaxRooms: Math.max(...samples.map(s => s.primary.yazykon_media_active_rooms ?? 0)),
    secondaryMaxRooms: Math.max(...samples.map(s => s.secondary.yazykon_media_active_rooms ?? 0))
  } : null;

  const resourceGrowthValues = runtimeGrowth ? [
    runtimeGrowth.primaryGoroutinesGrowth, runtimeGrowth.secondaryGoroutinesGrowth,
    runtimeGrowth.primaryHeapGrowth, runtimeGrowth.secondaryHeapGrowth
  ].filter((value): value is number => value !== null && Number.isFinite(value)) : [];
  const maxObservedResourceGrowth = resourceGrowthValues.length ? Math.max(...resourceGrowthValues) : Infinity;

  const pass = !fatalError &&
    failovers.length > 0 &&
    samples.length > 1 &&
    missingMetrics.length === 0 &&
    failedFailovers.length === 0 &&
    failoverErrorRate <= maxFailoverErrorRate &&
    sampleErrorRate <= maxSampleErrorRate &&
    maxObservedResourceGrowth <= maxResourceGrowth;

  const report = {
    generatedAt: new Date().toISOString(),
    durationMs: Date.now() - started,
    configuredDurationMs: durationMs,
    cycles: cycle,
    samples: samples.length,
    sampleAttempts,
    sampleErrors,
    roomId,
    fatalError: fatalError ?? null,
    failovers,
    summary: {
      failedFailovers: failedFailovers.length,
      maxRecoveryMs,
      failoverErrorRate,
      maxAllowedFailoverErrorRate: maxFailoverErrorRate,
      sampleErrorRate,
      maxAllowedSampleErrorRate: maxSampleErrorRate,
      slaMs,
      maxObservedResourceGrowth,
      maxAllowedResourceGrowth: maxResourceGrowth
    },
    runtimeGrowth,
    metricExtremes,
    missingMetrics,
    pass
  };

  fs.mkdirSync(artifactDir, { recursive: true });
  fs.writeFileSync(path.join(artifactDir, "sfu-long-soak-report.json"), JSON.stringify(report, null, 2));

  expect(fatalError, "soak fatal error").toBeUndefined();
  expect(missingMetrics, "required runtime metrics must be present").toEqual([]);
  expect(failedFailovers).toHaveLength(0);
  expect(failoverErrorRate).toBeLessThanOrEqual(maxFailoverErrorRate);
  expect(sampleErrorRate).toBeLessThanOrEqual(maxSampleErrorRate);
  expect(maxObservedResourceGrowth).toBeLessThanOrEqual(maxResourceGrowth);
  expect(samples.length).toBeGreaterThan(1);
});
