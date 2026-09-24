import { createHmac } from "node:crypto";
import { execFileSync } from "node:child_process";
import fs from "node:fs";
import path from "node:path";
import { test, expect } from "@playwright/test";

const primary = process.env.SFU_PRIMARY_URL ?? "http://127.0.0.1:4100";
const secondary = process.env.SFU_SECONDARY_URL ?? "http://127.0.0.1:4200";
const composeFile = process.env.SFU_FAILOVER_COMPOSE ?? "e2e/docker-compose.sfu-failover.yml";
const redisUrl = process.env.REDIS_URL ?? "redis://127.0.0.1:6379";
const rooms = Math.min(6, Math.max(2, Number(process.env.SFU_ROOM_FAILOVER_ROOMS ?? 3)));
const ttlMs = Number(process.env.SFU_ROOM_OWNER_TTL_MS ?? 3000);
const secret = process.env.ROOM_ACCESS_SECRET ?? "integration-secret";
const artifactDir = process.env.SFU_ARTIFACT_DIR ?? "artifacts";
const maxActivePeerDelta = Number(process.env.SFU_MULTI_ROOM_MAX_ACTIVE_DELTA ?? rooms);
const maxRoomDelta = Number(process.env.SFU_MULTI_ROOM_MAX_ROOM_DELTA ?? rooms);
const maxTrackDelta = Number(process.env.SFU_MULTI_ROOM_MAX_TRACK_DELTA ?? rooms * 2);

function token(roomId: string, userId: string) {
  const payload = Buffer.from(JSON.stringify({
    roomId, userId, role: "host", exp: Math.floor(Date.now() / 1000) + 600
  })).toString("base64url");
  return payload + "." + createHmac("sha256", secret).update(payload).digest("base64url");
}

async function waitHealth(endpoint: string) {
  for (let i = 0; i < 40; i++) {
    try {
      if ((await fetch(endpoint + "/health")).ok) return;
    } catch {}
    await new Promise(resolve => setTimeout(resolve, 250));
  }
  throw new Error("SFU health timeout: " + endpoint);
}

async function runtimeMetrics(endpoint: string) {
  try {
    const response = await fetch(endpoint + "/metrics");
    if (!response.ok) return {};
    const text = await response.text();
    const names = [
      "yazykon_media_active_peers",
      "yazykon_media_active_rooms",
      "yazykon_media_active_tracks",
      "yazykon_media_goroutines",
      "yazykon_media_heap_bytes"
    ];
    return Object.fromEntries(names.map(name => {
      const line = text.split("\n").find(value => value.startsWith(name + " "));
      return [name, line ? Number(line.trim().split(/\s+/)[1]) : null];
    }));
  } catch {
    return {};
  }
}

async function redisOwners(roomIds: string[]) {
  const { createClient } = await import("redis");
  const client = createClient({ url: redisUrl });
  await client.connect();
  const owners = await Promise.all(roomIds.map(async roomId => {
    const value = await client.get("yazykon:sfu:owner:" + roomId);
    return { roomId, value: value ? JSON.parse(value) : null };
  }));
  await client.quit();
  return owners;
}

async function redisRoomState(roomIds: string[]) {
  const { createClient } = await import("redis");
  const client = createClient({ url: redisUrl });
  await client.connect();
  const states = await Promise.all(roomIds.map(async roomId => ({
    roomId,
    owner: await client.exists("yazykon:sfu:owner:" + roomId),
    state: await client.exists("yazykon:sfu:state:" + roomId),
    tracks: await client.exists("yazykon:sfu:tracks:" + roomId),
    peers: (await client.keys("yazykon:sfu:peer:" + roomId + ":*")).length
  })));
  await client.quit();
  return states;
}

async function joinRoom(page: import("@playwright/test").Page, endpoint: string, roomId: string, peerId?: string) {
  return page.evaluate(async ({ endpoint, roomId, accessToken, peerId }) => {
    const store = (globalThis as any).__multiRoomMedia ??= new Map();
    const stream = store.get(roomId)?.stream as MediaStream | undefined
      ?? await navigator.mediaDevices.getUserMedia({ video: true, audio: false });
    const previous = store.get(roomId);
    previous?.ws?.close();
    previous?.pc?.close();

    const pc = new RTCPeerConnection();
    for (const track of stream.getTracks()) pc.addTrack(track, stream);

    const ws = new WebSocket(endpoint.replace(/^http/, "ws") + "/ws");
    const queue: any[] = [];
    const waiters = new Map<string, Array<(message: any) => void>>();

    const waitFor = (type: string) => new Promise<any>((resolve, reject) => {
      const timer = setTimeout(() => reject(new Error("timeout: " + type)), 15000);
      const queued = queue.findIndex(message => message.type === type);
      if (queued >= 0) {
        clearTimeout(timer);
        resolve(queue.splice(queued, 1)[0]);
        return;
      }
      const list = waiters.get(type) ?? [];
      list.push(message => { clearTimeout(timer); resolve(message); });
      waiters.set(type, list);
    });

    ws.onmessage = event => {
      const message = JSON.parse(event.data);
      if (message.type === "ice" && message.data) void pc.addIceCandidate(message.data);
      const list = waiters.get(message.type);
      if (list?.length) {
        list.shift()!(message);
        if (!list.length) waiters.delete(message.type);
      } else queue.push(message);
    };

    pc.onicecandidate = event => {
      if (event.candidate && ws.readyState === WebSocket.OPEN) {
        ws.send(JSON.stringify({ type: "ice", roomId, data: event.candidate.toJSON() }));
      }
    };

    await new Promise<void>((resolve, reject) => {
      ws.onopen = () => resolve();
      ws.onerror = () => reject(new Error("websocket failed: " + endpoint));
    });

    ws.send(JSON.stringify({
      type: "join", roomId,
      data: { accessToken, reconnect: Boolean(peerId), ...(peerId ? { peerId } : {}) }
    }));
    const joined = await waitFor("joined");

    const offer = await pc.createOffer();
    await pc.setLocalDescription(offer);
    ws.send(JSON.stringify({ type: "offer", roomId, data: pc.localDescription }));
    const answer = await waitFor("answer");
    if (answer.data) await pc.setRemoteDescription(answer.data);

    store.set(roomId, { pc, ws, stream, peerId: joined.peerId });
    await new Promise(resolve => setTimeout(resolve, 300));

    return {
      peerId: joined.peerId,
      roomId,
      trackIds: stream.getTracks().map(track => track.id),
      trackStates: stream.getTracks().map(track => track.readyState),
      connectionState: pc.connectionState,
      iceConnectionState: pc.iceConnectionState
    };
  }, { endpoint, roomId, peerId, accessToken: token(roomId, "owner-" + roomId) });
}

async function closeRoomMedia(page: import("@playwright/test").Page, roomId: string, stopTracks = false) {
  await page.evaluate(({ roomId, stopTracks }) => {
    const store = (globalThis as any).__multiRoomMedia;
    const connection = store?.get(roomId);
    if (!connection) return;
    connection.ws?.close();
    connection.pc?.close();
    if (stopTracks) connection.stream?.getTracks().forEach((track: MediaStreamTrack) => track.stop());
    store.delete(roomId);
  }, { roomId, stopTracks });
}

async function mediaState(page: import("@playwright/test").Page, roomId: string) {
  return page.evaluate((roomId) => {
    const connection = (globalThis as any).__multiRoomMedia?.get(roomId);
    if (!connection) return null;
    return {
      peerId: connection.peerId,
      trackIds: connection.stream.getTracks().map((track: MediaStreamTrack) => track.id),
      trackStates: connection.stream.getTracks().map((track: MediaStreamTrack) => track.readyState),
      connectionState: connection.pc.connectionState,
      iceConnectionState: connection.pc.iceConnectionState
    };
  }, roomId);
}


test("multiple active rooms transfer ownership and reconnect on secondary", async ({ browser }) => {
  test.skip(!process.env.SFU_FAILOVER_LIVE, "Set SFU_FAILOVER_LIVE=1 for the live Docker run");

  await waitHealth(primary);
  await waitHealth(secondary);

  const pages = await Promise.all(
    Array.from({ length: rooms }, () => browser.newPage({ permissions: ["camera", "microphone"] }))
  );
  const roomIds = Array.from({ length: rooms }, (_, i) => "FAILOVER-ROOM-" + (i + 1));
  const joined = await Promise.all(roomIds.map((roomId, i) => joinRoom(pages[i], primary, roomId)));
  const initialMedia = await Promise.all(roomIds.map((roomId, i) => mediaState(pages[i], roomId)));
  for (const state of initialMedia) {
    expect(state?.trackIds.length).toBeGreaterThan(0);
    expect(state?.trackStates.every((value: string) => value === "live")).toBeTruthy();
  }
  const metricsBefore = await runtimeMetrics(primary);
  expect(new Set(joined.map(item => item.peerId)).size).toBe(rooms);

  const before = await redisOwners(roomIds);
  for (const owner of before) expect(owner.value?.nodeId).toBe("integration-primary");

  const failoverStarted = Date.now();
  execFileSync("docker", ["compose", "-f", composeFile, "stop", "sfu-primary"], { stdio: "inherit" });
  try {
    await new Promise(resolve => setTimeout(resolve, ttlMs + 1500));
    const after = await redisOwners(roomIds);
    for (const owner of after) {
      expect(owner.value?.nodeId).toBe("integration-secondary");
      expect(owner.value?.endpoint).toContain("4200");
    }

    await waitHealth(secondary);
    const reconnectStarted = Date.now();
    const reconnectResults = await Promise.all(joined.map((item, i) =>
      joinRoom(pages[i], secondary, item.roomId, item.peerId)
    ));
    const recoveredMedia = await Promise.all(roomIds.map((roomId, i) => mediaState(pages[i], roomId)));
    for (let i = 0; i < recoveredMedia.length; i++) {
      expect(recoveredMedia[i]?.peerId).toBe(joined[i].peerId);
      expect(recoveredMedia[i]?.trackIds).toEqual(initialMedia[i]?.trackIds);
      expect(recoveredMedia[i]?.trackStates.every((value: string) => value === "live")).toBeTruthy();
    }
    const reconnectMs = Date.now() - reconnectStarted;
    const metricsAfter = await runtimeMetrics(secondary);
    const redisAfter = await redisRoomState(roomIds);
    expect(reconnectResults.every((item, i) => item.peerId === joined[i].peerId)).toBeTruthy();
    expect(redisAfter.every(item => item.owner === 1 && item.state === 1)).toBeTruthy();
    const metricDelta = (name: string) => {
      const beforeValue = Number(metricsBefore[name]);
      const afterValue = Number(metricsAfter[name]);
      return Number.isFinite(beforeValue) && Number.isFinite(afterValue) ? afterValue - beforeValue : null;
    };
    const deltas = {
      activePeers: metricDelta("yazykon_media_active_peers"),
      activeRooms: metricDelta("yazykon_media_active_rooms"),
      activeTracks: metricDelta("yazykon_media_active_tracks")
    };
    const resourceChecks = {
      activePeers: deltas.activePeers === null || deltas.activePeers <= maxActivePeerDelta,
      activeRooms: deltas.activeRooms === null || deltas.activeRooms <= maxRoomDelta,
      activeTracks: deltas.activeTracks === null || deltas.activeTracks <= maxTrackDelta
    };

    expect(Object.values(resourceChecks).every(Boolean)).toBeTruthy();

    // Reverse failover validates that the recovered media sessions are not
    // permanently pinned to the secondary node.
    execFileSync("docker", ["compose", "-f", composeFile, "start", "sfu-primary", "sfu-secondary"], { stdio: "inherit" });
    await waitHealth(primary);
    await waitHealth(secondary);
    execFileSync("docker", ["compose", "-f", composeFile, "stop", "sfu-secondary"], { stdio: "inherit" });
    await new Promise(resolve => setTimeout(resolve, ttlMs + 1500));

    const reverseOwners = await redisOwners(roomIds);
    for (const owner of reverseOwners) {
      expect(owner.value?.nodeId).toBe("integration-primary");
      expect(owner.value?.endpoint).toContain("4100");
    }

    const reverseStarted = Date.now();
    const reverseResults = await Promise.all(reconnectResults.map((item, i) =>
      joinRoom(pages[i], primary, item.roomId, item.peerId)
    ));
    const reverseMedia = await Promise.all(roomIds.map((roomId, i) => mediaState(pages[i], roomId)));
    const reverseMs = Date.now() - reverseStarted;

    for (let i = 0; i < reverseMedia.length; i++) {
      expect(reverseResults[i].peerId).toBe(joined[i].peerId);
      expect(reverseMedia[i]?.peerId).toBe(joined[i].peerId);
      expect(reverseMedia[i]?.trackIds).toEqual(initialMedia[i]?.trackIds);
      expect(reverseMedia[i]?.trackStates.every((value: string) => value === "live")).toBeTruthy();
    }

    const reverseRedis = await redisRoomState(roomIds);
    expect(reverseRedis.every(item => item.owner === 1 && item.state === 1)).toBeTruthy();

    for (let i = 0; i < roomIds.length; i++) {
      expect(recoveredMedia[i]?.trackIds).toEqual(initialMedia[i]?.trackIds);
      expect(reverseMedia[i]?.trackIds).toEqual(initialMedia[i]?.trackIds);
    }

    const report = {
      generatedAt: new Date().toISOString(),
      rooms, roomIds,
      failoverMs: Date.now() - failoverStarted,
      reconnectMs,
      metricsBefore,
      metricsAfter,
      redisAfter,
      ownersBefore: before.map(item => item.value),
      ownersAfter: after,
      reconnectResults,
      reverseResults,
      media: { initial: initialMedia, recovered: recoveredMedia, reverse: reverseMedia },
      reverseFailoverMs: reverseMs,
      deltas,
      budgets: { maxActivePeerDelta, maxRoomDelta, maxTrackDelta },
      resourceChecks,
      reverseRedis,
      pass: reconnectResults.every((item, i) => item.peerId === joined[i].peerId)
        && redisAfter.every(item => item.owner === 1 && item.state === 1)
        && reverseResults.every((item, i) => item.peerId === joined[i].peerId)
        && reverseRedis.every(item => item.owner === 1 && item.state === 1)
        && Object.values(resourceChecks).every(Boolean)
    };
    fs.mkdirSync(artifactDir, { recursive: true });
    fs.writeFileSync(path.join(artifactDir, "sfu-multi-room-failover-report.json"), JSON.stringify(report, null, 2));
  } finally {
    execFileSync("docker", ["compose", "-f", composeFile, "start", "sfu-primary"], { stdio: "inherit" });
    await waitHealth(primary);
    await Promise.all(pages.map((page, i) => closeRoomMedia(page, roomIds[i], true).catch(() => {})));
    await Promise.all(pages.map(page => page.close()));
  }
});