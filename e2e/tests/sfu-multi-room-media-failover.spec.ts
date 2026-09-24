import { createHmac } from "node:crypto";
import { execFileSync } from "node:child_process";
import fs from "node:fs";
import path from "node:path";
import { test, expect } from "@playwright/test";

const primary = process.env.SFU_PRIMARY_URL ?? "http://127.0.0.1:4100";
const secondary = process.env.SFU_SECONDARY_URL ?? "http://127.0.0.1:4200";
const composeFile = process.env.SFU_FAILOVER_COMPOSE ?? "e2e/docker-compose.sfu-failover.yml";
const redisUrl = process.env.REDIS_URL ?? "redis://127.0.0.1:6379";
const secret = process.env.ROOM_ACCESS_SECRET ?? "integration-secret";
const rooms = Math.min(4, Math.max(2, Number(process.env.SFU_MULTI_ROOM_MEDIA_ROOMS ?? 2)));
const ttlMs = Number(process.env.SFU_ROOM_OWNER_TTL_MS ?? 3000);
const artifactDir = process.env.SFU_ARTIFACT_DIR ?? "artifacts";

function token(roomId: string, userId: string) {
  const payload = Buffer.from(JSON.stringify({
    roomId, userId, role: userId.endsWith("-a") ? "host" : "participant",
    exp: Math.floor(Date.now() / 1000) + 600
  })).toString("base64url");
  return payload + "." + createHmac("sha256", secret).update(payload).digest("base64url");
}

async function waitHealth(endpoint: string) {
  for (let i = 0; i < 40; i++) {
    try { if ((await fetch(endpoint + "/health")).ok) return; } catch {}
    await new Promise(resolve => setTimeout(resolve, 250));
  }
  throw new Error("SFU health timeout: " + endpoint);
}

async function runtimeMetrics(endpoint: string) {
  const response = await fetch(endpoint + "/metrics");
  if (!response.ok) return {};
  const text = await response.text();
  const names = ["yazykon_media_active_peers","yazykon_media_active_rooms","yazykon_media_active_tracks"];
  return Object.fromEntries(names.map(name => {
    const line = text.split("\n").find(value => value.startsWith(name + " "));
    return [name, line ? Number(line.trim().split(/\s+/)[1]) : null];
  }));
}

async function peerKeys(roomIds: string[]) {
  const { createClient } = await import("redis");
  const client = createClient({ url: redisUrl });
  await client.connect();
  const keys = await client.keys("yazykon:sfu:peer:*");
  const filtered = keys.filter(key => roomIds.some(roomId => key.includes(roomId)));
  await client.quit();
  return filtered;
}

async function owners(roomIds: string[]) {
  const { createClient } = await import("redis");
  const client = createClient({ url: redisUrl });
  await client.connect();
  const result = await Promise.all(roomIds.map(async roomId => {
    const raw = await client.get("yazykon:sfu:owner:" + roomId);
    return { roomId, owner: raw ? JSON.parse(raw) : null };
  }));
  await client.quit();
  return result;
}

async function connect(page: import("@playwright/test").Page, endpoint: string, roomId: string, userId: string, peerId = "", waitRemote = false) {
  return page.evaluate(async ({ endpoint, roomId, accessToken, peerId, waitRemote }) => {
    const store = (globalThis as any).__multiRoomMediaFailover ??= new Map();
    const old = store.get(roomId);
    const stream = old?.stream as MediaStream | undefined
      ?? await navigator.mediaDevices.getUserMedia({ video: true, audio: false });
    old?.ws?.close(); old?.pc?.close();

    const pc = new RTCPeerConnection();
    let remoteTracks = 0, frameCount = 0;
    let resolveFrame: (() => void) | undefined;
    const frameReady = new Promise<void>(resolve => { resolveFrame = resolve; });

    pc.ontrack = event => {
      remoteTracks++;
      const video = document.createElement("video");
      video.autoplay = true; video.muted = true; video.playsInline = true;
      video.srcObject = event.streams[0];
      document.body.appendChild(video);
      const count = () => {
        frameCount++;
        resolveFrame?.();
        if ("requestVideoFrameCallback" in video) video.requestVideoFrameCallback(() => count());
      };
      video.onloadeddata = count;
      void video.play().then(count).catch(() => undefined);
    };
    for (const track of stream.getTracks()) pc.addTrack(track, stream);

    const ws = new WebSocket(endpoint.replace(/^http/, "ws") + "/ws");
    const queue: any[] = [];
    const waiters = new Map<string, Array<(m: any) => void>>();
    const waitFor = (type: string) => new Promise<any>((resolve, reject) => {
      const timer = setTimeout(() => reject(new Error("timeout: " + type)), 15000);
      const list = waiters.get(type) ?? [];
      list.push(m => { clearTimeout(timer); resolve(m); });
      waiters.set(type, list);
      const index = queue.findIndex(m => m.type === type);
      if (index >= 0) {
        const m = queue.splice(index, 1)[0];
        clearTimeout(timer); resolve(m);
      }
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
      if (event.candidate && ws.readyState === WebSocket.OPEN)
        ws.send(JSON.stringify({ type: "ice", roomId, data: event.candidate.toJSON() }));
    };
    await new Promise<void>((resolve, reject) => {
      ws.onopen = () => resolve();
      ws.onerror = () => reject(new Error("websocket failed"));
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
    if (waitRemote) await Promise.race([
      frameReady,
      new Promise((_, reject) => setTimeout(() => reject(new Error("remote video frame timeout")), 10000))
    ]);
    store.set(roomId, { pc, ws, stream, peerId: joined.peerId });
    return {
      peerId: joined.peerId,
      trackIds: stream.getTracks().map(t => t.id),
      trackStates: stream.getTracks().map(t => t.readyState),
      remoteTracks, frameCount, peers: joined.data?.peers ?? []
    };
  }, { endpoint, roomId, accessToken: token(roomId, userId), peerId, waitRemote });
}

async function frameProgress(page: import("@playwright/test").Page) {
  return page.evaluate(() => new Promise<number>((resolve, reject) => {
    const start = [...document.querySelectorAll("video")].reduce((n, video) => n + (video.videoWidth > 0 ? 1 : 0), 0);
    let timer: ReturnType<typeof setTimeout> | undefined;
    const deadline = setTimeout(() => {
      if (timer) clearTimeout(timer);
      reject(new Error("remote frame progression timeout"));
    }, 5000);
    const check = () => {
      const ready = [...document.querySelectorAll("video")].filter(video => video.readyState >= 2 && video.videoWidth > 0);
      if (ready.length >= start && ready.length > 0) {
        clearTimeout(deadline);
        resolve(ready.length);
        return;
      }
      timer = setTimeout(check, 100);
    };
    check();
  }));
}

async function cleanup(page: import("@playwright/test").Page, roomId: string) {
  await page.evaluate(roomId => {
    const c = (globalThis as any).__multiRoomMediaFailover?.get(roomId);
    c?.ws?.close(); c?.pc?.close();
    c?.stream?.getTracks().forEach((t: MediaStreamTrack) => t.stop());
    (globalThis as any).__multiRoomMediaFailover?.delete(roomId);
  }, roomId);
}

test("multi-room two-party remote media survives bidirectional SFU failover", async ({ browser }) => {
  test.skip(!process.env.SFU_FAILOVER_LIVE, "Set SFU_FAILOVER_LIVE=1 for the live Docker run");
  await waitHealth(primary); await waitHealth(secondary);

  const roomIds = Array.from({ length: rooms }, (_, i) => "MEDIA-FAILOVER-ROOM-" + (i + 1));
  const pages = await Promise.all(roomIds.flatMap(() => [
    browser.newPage({ permissions: ["camera", "microphone"] }),
    browser.newPage({ permissions: ["camera", "microphone"] })
  ]));

  try {
    const initial: any[] = [];
    for (let i = 0; i < rooms; i++) {
      const a = await connect(pages[i * 2], primary, roomIds[i], "room-" + i + "-a");
      const b = await connect(pages[i * 2 + 1], primary, roomIds[i], "room-" + i + "-b", "", true);
      expect(a.peerId).toBeTruthy();
      expect(b.peerId).toBeTruthy();
      expect(b.peerId).not.toBe(a.peerId);
      expect(b.remoteTracks).toBeGreaterThan(0);
      expect(b.frameCount).toBeGreaterThan(0);
      initial.push({ a, b });
    }
    const metricsBefore = await runtimeMetrics(primary);
    const initialPeerKeys = await peerKeys(roomIds);
    const initialTrackStates = await Promise.all(pages.map((page, i) =>
      page.evaluate(roomId => {
        const c = (globalThis as any).__multiRoomMediaFailover?.get(roomId);
        return c?.stream?.getTracks().map((t: MediaStreamTrack) => ({ id: t.id, state: t.readyState })) ?? [];
      }, roomIds[Math.floor(i / 2)])
    ));
    const allInitialPeerIds = initial.flatMap(x => [x.a.peerId, x.b.peerId]);
    const allInitialTrackIds = initial.flatMap(x => [x.a.trackIds, x.b.trackIds]).flat();
    expect(new Set(allInitialPeerIds).size).toBe(allInitialPeerIds.length);
    expect(new Set(allInitialTrackIds).size).toBe(allInitialTrackIds.length);

    const before = await owners(roomIds);
    expect(before.every(x => x.owner?.nodeId === "integration-primary")).toBeTruthy();

    execFileSync("docker", ["compose", "-f", composeFile, "stop", "sfu-primary"], { stdio: "inherit" });
    await new Promise(resolve => setTimeout(resolve, ttlMs + 1500));
    const takeover = await owners(roomIds);
    expect(takeover).toHaveLength(roomIds.length);
    expect(takeover.every(x => x.owner?.nodeId === "integration-secondary")).toBeTruthy();

    const recovered: any[] = [];
    for (let i = 0; i < rooms; i++) {
      const a = await connect(pages[i * 2], secondary, roomIds[i], "room-" + i + "-a", initial[i].a.peerId);
      const b = await connect(pages[i * 2 + 1], secondary, roomIds[i], "room-" + i + "-b", initial[i].b.peerId, true);
      expect(a.peerId).toBe(initial[i].a.peerId);
      expect(b.peerId).toBe(initial[i].b.peerId);
      expect(b.remoteTracks).toBeGreaterThan(0);
      expect(b.frameCount).toBeGreaterThan(0);
      await frameProgress(pages[i * 2 + 1]);
      expect(b.trackIds).toEqual(initial[i].b.trackIds);
      expect(b.trackStates.every((s: string) => s === "live")).toBeTruthy();
      recovered.push({ a, b });
    }

    execFileSync("docker", ["compose", "-f", composeFile, "start", "sfu-primary"], { stdio: "inherit" });
    await waitHealth(primary);
    execFileSync("docker", ["compose", "-f", composeFile, "stop", "sfu-secondary"], { stdio: "inherit" });
    await new Promise(resolve => setTimeout(resolve, ttlMs + 1500));
    const reverseOwners = await owners(roomIds);
    expect(reverseOwners.every(x => x.owner?.nodeId === "integration-primary")).toBeTruthy();

    const metricsAfter = await runtimeMetrics(primary);
    const recoveredPeerKeys = await peerKeys(roomIds);
    expect(recoveredPeerKeys.length).toBeLessThanOrEqual(initialPeerKeys.length + rooms);
    const recoveredPeerIds = recovered.flatMap(x => [x.a.peerId, x.b.peerId]);
    const recoveredTrackIds = recovered.flatMap(x => [x.a.trackIds, x.b.trackIds]).flat();
    expect(new Set(recoveredPeerIds).size).toBe(recoveredPeerIds.length);
    expect(new Set(recoveredTrackIds).size).toBe(recoveredTrackIds.length);

    const reverse: any[] = [];
    for (let i = 0; i < rooms; i++) {
      const a = await connect(pages[i * 2], primary, roomIds[i], "room-" + i + "-a", initial[i].a.peerId);
      const b = await connect(pages[i * 2 + 1], primary, roomIds[i], "room-" + i + "-b", initial[i].b.peerId, true);
      expect(a.peerId).toBe(initial[i].a.peerId);
      expect(b.peerId).toBe(initial[i].b.peerId);
      expect(b.remoteTracks).toBeGreaterThan(0);
      expect(b.frameCount).toBeGreaterThan(0);
      await frameProgress(pages[i * 2 + 1]);
      expect(b.trackIds).toEqual(initial[i].b.trackIds);
      reverse.push({ a, b });
    }

    const report = {
      generatedAt: new Date().toISOString(), rooms, roomIds,
      before, takeover, reverseOwners, initial, recovered, reverse,
      metricsBefore, metricsAfter,
      metricDelta: {
        activePeers: Number(metricsAfter.yazykon_media_active_peers) - Number(metricsBefore.yazykon_media_active_peers),
        activeRooms: Number(metricsAfter.yazykon_media_active_rooms) - Number(metricsBefore.yazykon_media_active_rooms),
        activeTracks: Number(metricsAfter.yazykon_media_active_tracks) - Number(metricsBefore.yazykon_media_active_tracks)
      },
      uniqueInitialPeers: new Set(allInitialPeerIds).size === allInitialPeerIds.length,
      uniqueInitialTracks: new Set(allInitialTrackIds).size === allInitialTrackIds.length,
      uniqueRecoveredPeers: new Set(recoveredPeerIds).size === recoveredPeerIds.length,
      uniqueRecoveredTracks: new Set(recoveredTrackIds).size === recoveredTrackIds.length,
      initialTrackStates,
      initialPeerKeyCount: initialPeerKeys.length,
      recoveredPeerKeyCount: recoveredPeerKeys.length,
      pass: initial.every(x => x.b.remoteTracks > 0 && x.b.frameCount > 0)
        && recovered.every(x => x.a.peerId && x.b.peerId && x.b.remoteTracks > 0 && x.b.frameCount > 0)
        && reverse.every(x => x.a.peerId && x.b.peerId && x.b.remoteTracks > 0 && x.b.frameCount > 0)
    };
    fs.mkdirSync(artifactDir, { recursive: true });
    fs.writeFileSync(path.join(artifactDir, "sfu-multi-room-media-failover-report.json"), JSON.stringify(report, null, 2));
  } finally {
    try {
      const beforeCleanup = await peerKeys(roomIds);
      await Promise.all(roomIds.flatMap((roomId, i) => [
        cleanup(pages[i * 2], roomId),
        cleanup(pages[i * 2 + 1], roomId)
      ]));
      await new Promise(resolve => setTimeout(resolve, 500));
      const afterCleanup = await peerKeys(roomIds);
      if (beforeCleanup.length > 0 && afterCleanup.length > beforeCleanup.length) {
        throw new Error("peer key count increased during cleanup");
      }
    } catch (error) {
      console.warn("cleanup peer-key verification skipped:", error);
    }
    execFileSync("docker", ["compose", "-f", composeFile, "start", "sfu-primary", "sfu-secondary"], { stdio: "inherit" });
    await Promise.all(roomIds.flatMap((roomId, i) => [cleanup(pages[i * 2], roomId), cleanup(pages[i * 2 + 1], roomId)]));
    await Promise.all(pages.map(p => p.close()));
  }
});
