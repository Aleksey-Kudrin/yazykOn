import { createHmac } from "node:crypto";
import fs from "node:fs";
import path from "node:path";
import { test, expect } from "@playwright/test";

const endpoint = process.env.SFU_PRIMARY_URL ?? "http://127.0.0.1:4100";
const redisUrl = process.env.REDIS_URL ?? "redis://127.0.0.1:6379";
const secret = process.env.ROOM_ACCESS_SECRET ?? "integration-secret";
const rounds = Math.max(5, Number(process.env.SFU_CHURN_ROUNDS ?? 12));
const artifactDir = process.env.SFU_ARTIFACT_DIR ?? "artifacts";
const maxActivePeerDelta = Number(process.env.SFU_CHURN_MAX_ACTIVE_DELTA ?? 1);
const maxRoomDelta = Number(process.env.SFU_CHURN_MAX_ROOM_DELTA ?? 0);
const maxTrackDelta = Number(process.env.SFU_CHURN_MAX_TRACK_DELTA ?? 0);
const maxResourceGrowth = Number(process.env.SFU_CHURN_MAX_RESOURCE_GROWTH ?? 0.2);

function token(roomId: string, userId: string) {
  const payload = Buffer.from(JSON.stringify({
    roomId, userId, role: "host", exp: Math.floor(Date.now() / 1000) + 600
  })).toString("base64url");
  return `${payload}.${createHmac("sha256", secret).update(payload).digest("base64url")}`;
}

async function runtimeMetrics() {
  try {
    const response = await fetch(endpoint + "/metrics");
    if (!response.ok) return {};
    const body = await response.text();
    const names = [
      "yazykon_media_active_peers",
      "yazykon_media_active_rooms",
      "yazykon_media_active_tracks",
      "yazykon_media_goroutines",
      "yazykon_media_heap_bytes"
    ];
    return Object.fromEntries(names.map(name => {
      const line = body.split("\n").find(value => value.startsWith(name + " "));
      return [name, line ? Number(line.trim().split(/\s+/)[1]) : null];
    }));
  } catch {
    return {};
  }
}

function joinAndClose(roomId: string, userId: string): Promise<string> {
  return new Promise((resolve, reject) => {
    const ws = new WebSocket(endpoint.replace(/^http/, "ws") + "/ws");
    const timer = setTimeout(() => reject(new Error(`join timeout: ${roomId}`)), 10000);
    ws.onopen = () => ws.send(JSON.stringify({
      type: "join", roomId,
      data: { accessToken: token(roomId, userId), reconnect: false }
    }));
    ws.onmessage = event => {
      const message = JSON.parse(event.data);
      if (message.type !== "joined") return;
      clearTimeout(timer);
      resolve(message.peerId);
      setTimeout(() => ws.close(), 50);
    };
    ws.onerror = () => {
      clearTimeout(timer);
      reject(new Error(`websocket failed: ${roomId}`));
    };
  });
}

async function roomKeys(client: any, roomId: string) {
  const [owner, state, tracks, peers] = await Promise.all([
    client.exists(`yazykon:sfu:owner:${roomId}`),
    client.exists(`yazykon:sfu:state:${roomId}`),
    client.exists(`yazykon:sfu:tracks:${roomId}`),
    client.keys(`yazykon:sfu:peer:${roomId}:*`)
  ]);
  return { owner, state, tracks, peers };
}

test("SFU repeated room churn leaves no Redis peer/session state", async () => {
  test.skip(!process.env.SFU_FAILOVER_LIVE, "Set SFU_FAILOVER_LIVE=1 for the live Docker run");

  const { createClient } = await import("redis");
  const client = createClient({ url: redisUrl });
  await client.connect();

  const roomIds: string[] = [];
  const cleanupMs: number[] = [];
  const metricsBefore = await runtimeMetrics();
  const started = Date.now();
  try {
    for (let i = 0; i < rounds; i++) {
      const roomId = `CHURN-LEAK-${Date.now()}-${i}`;
      roomIds.push(roomId);
      const peerId = await joinAndClose(roomId, `churn-${i}`);
      expect(peerId).toBeTruthy();
    }

    for (const roomId of roomIds) {
      let cleaned = false;
      const cleanupStarted = Date.now();
      for (let attempt = 0; attempt < 40; attempt++) {
        const keys = await roomKeys(client, roomId);
        if (keys.owner === 0 && keys.state === 0 && keys.tracks === 0 && keys.peers.length === 0) {
          cleaned = true;
          cleanupMs.push(Date.now() - cleanupStarted);
          break;
        }
        await new Promise(resolve => setTimeout(resolve, 250));
      }
      expect(cleaned, `Redis state leaked for ${roomId}`).toBeTruthy();
    }
    fs.mkdirSync(artifactDir, { recursive: true });
    const metricsAfter = await runtimeMetrics();
    const metric = (name: string) => {
      const before = Number(metricsBefore[name]);
      const after = Number(metricsAfter[name]);
      return Number.isFinite(before) && Number.isFinite(after) ? { before, after, delta: after - before } : null;
    };
    const peers = metric("yazykon_media_active_peers");
    const roomsMetric = metric("yazykon_media_active_rooms");
    const tracks = metric("yazykon_media_active_tracks");
    const goroutines = metric("yazykon_media_goroutines");
    const heap = metric("yazykon_media_heap_bytes");
    const heapGrowth = heap && heap.before > 0 ? (heap.after - heap.before) / heap.before : 0;
    const resourceChecks = {
      activePeers: !peers || peers.delta <= maxActivePeerDelta,
      activeRooms: !roomsMetric || roomsMetric.delta <= maxRoomDelta,
      activeTracks: !tracks || tracks.delta <= maxTrackDelta,
      heapGrowth: !heap || heapGrowth <= maxResourceGrowth
    };
    const pass = cleanupMs.length === roomIds.length && Object.values(resourceChecks).every(Boolean);
    fs.writeFileSync(path.join(artifactDir, "sfu-churn-leak-report.json"), JSON.stringify({
      generatedAt: new Date().toISOString(),
      rounds, rooms: roomIds.length, durationMs: Date.now() - started,
      cleanupMs, maxCleanupMs: Math.max(0, ...cleanupMs),
      budgets: { maxActivePeerDelta, maxRoomDelta, maxTrackDelta, maxResourceGrowth },
      metricsBefore, metricsAfter,
      deltas: { peers, rooms: roomsMetric, tracks, goroutines, heap, heapGrowth },
      resourceChecks, pass
    }, null, 2));
    expect(resourceChecks.activePeers).toBeTruthy();
    expect(resourceChecks.activeRooms).toBeTruthy();
    expect(resourceChecks.activeTracks).toBeTruthy();
    expect(resourceChecks.heapGrowth).toBeTruthy();
  } finally {
    for (const roomId of roomIds) {
      const keys = await roomKeys(client, roomId);
      const fixed = [
        `yazykon:sfu:owner:${roomId}`,
        `yazykon:sfu:state:${roomId}`,
        `yazykon:sfu:tracks:${roomId}`
      ];
      if (keys.peers.length || keys.owner || keys.state || keys.tracks) await client.del(...fixed, ...keys.peers);
    }
    await client.quit();
  }
});
