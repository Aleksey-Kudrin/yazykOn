import { createHmac } from "node:crypto";
import { execFileSync } from "node:child_process";
import fs from "node:fs";
import path from "node:path";
import { test, expect } from "@playwright/test";

const primary = process.env.SFU_PRIMARY_URL ?? "http://127.0.0.1:4100";
const secondary = process.env.SFU_SECONDARY_URL ?? "http://127.0.0.1:4200";
const compose = process.env.SFU_FAILOVER_COMPOSE ?? "e2e/docker-compose.sfu-failover.yml";
const redisUrl = process.env.REDIS_URL ?? "redis://127.0.0.1:6379";
const secret = process.env.ROOM_ACCESS_SECRET ?? "integration-secret";
const ttl = Number(process.env.SFU_ROOM_OWNER_TTL_MS ?? 3000);
const roomCount = Math.min(4, Math.max(2, Number(process.env.SFU_CONCURRENT_CHAOS_ROOMS ?? 2)));
const members = 3;

function accessToken(roomId: string, userId: string) {
  const payload = Buffer.from(JSON.stringify({ roomId, userId, role: userId.endsWith("-0") ? "host" : "participant", exp: Math.floor(Date.now() / 1000) + 600 })).toString("base64url");
  return payload + "." + createHmac("sha256", secret).update(payload).digest("base64url");
}

async function health(url: string) {
  for (let i = 0; i < 40; i++) {
    try { if ((await fetch(url + "/health")).ok) return; } catch {}
    await new Promise(r => setTimeout(r, 250));
  }
  throw new Error("health timeout: " + url);
}

async function metrics(url: string) {
  const response = await fetch(url + "/metrics");
  const body = await response.text();
  const read = (name: string) => {
    const line = body.split("\n").find(x => x.startsWith(name + " "));
    return line ? Number(line.trim().split(/\s+/)[1]) : 0;
  };
  return {
    peers: read("yazykon_media_active_peers"),
    rooms: read("yazykon_media_active_rooms"),
    tracks: read("yazykon_media_active_tracks")
  };
}

async function redisState(roomIds: string[]) {
  const { createClient } = await import("redis");
  const client = createClient({ url: redisUrl });
  await client.connect();
  const owners = await Promise.all(roomIds.map(async roomId => {
    const raw = await client.get("yazykon:sfu:owner:" + roomId);
    return raw ? JSON.parse(raw) : null;
  }));
  const peers = (await Promise.all(roomIds.map(roomId => client.keys("yazykon:sfu:peer:" + roomId + ":*")))).flat();
  await client.quit();
  return { owners, peerCount: peers.length };
}

async function connect(page: import("@playwright/test").Page, endpoint: string, roomId: string, userId: string, peerId = "") {
  return page.evaluate(async ({ endpoint, roomId, accessToken, peerId }) => {
    const state = (globalThis as any).__concurrentChaos ??= { stream: undefined, pc: undefined, ws: undefined };
    state.ws?.close(); state.pc?.close();
    state.stream ??= await navigator.mediaDevices.getUserMedia({ video: true, audio: false });
    const pc = new RTCPeerConnection();
    let remoteFrames = 0;
    let remoteTracks = 0;
    pc.ontrack = event => {
      remoteTracks++;
      const video = document.createElement("video");
      video.autoplay = true; video.muted = true; video.playsInline = true;
      video.srcObject = event.streams[0] ?? new MediaStream([event.track]);
      document.body.appendChild(video);
      const tick = () => {
        remoteFrames++;
        if ("requestVideoFrameCallback" in video) video.requestVideoFrameCallback(tick);
      };
      video.onloadeddata = tick;
      void video.play().then(tick).catch(() => undefined);
    };
    for (const track of state.stream.getTracks()) pc.addTrack(track, state.stream);
    let ws: WebSocket | undefined;
    let lastWsError: Error | undefined;
    for (let attempt = 0; attempt < 5; attempt++) {
      try {
        ws = new WebSocket(endpoint.replace(/^http/, "ws") + "/ws");
        await new Promise<void>((resolve, reject) => {
          const timer = setTimeout(() => reject(new Error("websocket open timeout")), 5000);
          ws!.onopen = () => { clearTimeout(timer); resolve(); };
          ws!.onerror = () => { clearTimeout(timer); reject(new Error("websocket failed")); };
        });
        break;
      } catch (error) {
        lastWsError = error as Error;
        try { ws?.close(); } catch {}
        await new Promise(resolve => setTimeout(resolve, 250 * (attempt + 1)));
      }
    }
    if (!ws) throw lastWsError ?? new Error("websocket failed");
    const queue: any[] = [];
    const waiters = new Map<string, ((m: any) => void)[]>();
    const wait = (type: string) => new Promise<any>((resolve, reject) => {
      const timer = setTimeout(() => reject(new Error("timeout " + type)), 15000);
      const queued = queue.findIndex(m => m.type === type);
      if (queued >= 0) { clearTimeout(timer); return resolve(queue.splice(queued, 1)[0]); }
      const list = waiters.get(type) ?? [];
      list.push(m => { clearTimeout(timer); resolve(m); });
      waiters.set(type, list);
    });
    ws.onmessage = event => {
      const message = JSON.parse(event.data);
      if (message.type === "ice" && message.data) void pc.addIceCandidate(message.data);
      const list = waiters.get(message.type);
      if (list?.length) list.shift()!(message); else queue.push(message);
    };
    await new Promise<void>((resolve, reject) => {
      ws.onopen = () => resolve();
      ws.onerror = () => reject(new Error("websocket failed"));
    });
    pc.onicecandidate = event => {
      if (event.candidate && ws.readyState === WebSocket.OPEN)
        ws.send(JSON.stringify({ type: "ice", roomId, data: event.candidate.toJSON() }));
    };
    ws.send(JSON.stringify({ type: "join", roomId, peerId: peerId || undefined, data: { accessToken, reconnect: Boolean(peerId) } }));
    const joined = await wait("joined");
    const offer = await pc.createOffer();
    await pc.setLocalDescription(offer);
    ws.send(JSON.stringify({ type: "offer", roomId, data: pc.localDescription }));
    const answer = await wait("answer");
    if (answer.data) await pc.setRemoteDescription(answer.data);
    await new Promise<void>((resolve, reject) => {
      const started = Date.now();
      const poll = () => {
        if (remoteTracks > 0 && remoteFrames > 0) return resolve();
        if (Date.now() - started > 10000) return reject(new Error("remote media timeout"));
        setTimeout(poll, 50);
      };
      poll();
    }).catch(() => undefined);
    state.pc = pc; state.ws = ws;
    return {
      peerId: joined.peerId,
      tracks: state.stream.getTracks().map((t: MediaStreamTrack) => ({ id: t.id, state: t.readyState })),
      remoteTracks, remoteFrames
    };
  }, { endpoint, roomId, accessToken: accessToken(roomId, userId), peerId });
}

test("concurrent rooms survive simultaneous SFU outage and reconnect", async ({ browser }) => {
  test.skip(!process.env.SFU_FAILOVER_LIVE, "Set SFU_FAILOVER_LIVE=1 for the live Docker run");
  await health(primary); await health(secondary);

  const rooms = Array.from({ length: roomCount }, (_, i) => "CONCURRENT-CHAOS-" + (i + 1));
  const pages = await Promise.all(Array.from({ length: roomCount * members }, async () => {
    const page = await browser.newPage({ permissions: ["camera", "microphone"] });
    await page.goto(primary + "/health");
    return page;
  }));
  const at = (room: number, member: number) => pages[room * members + member];
  const ids = new Map<string, string>();
  const tracks = new Map<string, string[]>();

  try {
    const initial = await Promise.all(rooms.flatMap((roomId, room) =>
      Array.from({ length: members }, async (_, member) => {
        const user = room + "-" + member;
        const result = await connect(at(room, member), primary, roomId, user);
        ids.set(user, result.peerId);
        tracks.set(user, result.tracks.map(t => t.id));
        return result;
      })
    ));
    expect(new Set(initial.map(x => x.peerId)).size).toBe(rooms.length * members);
    expect(new Set([...tracks.values()].flat()).size).toBe(rooms.length * members);

    const baseMetrics = await metrics(primary);
    const baseRedis = await redisState(rooms);
    expect(baseRedis.owners.every((owner: any) => owner?.nodeId === "integration-primary")).toBeTruthy();

    execFileSync("docker", ["compose", "-f", compose, "stop", "sfu-primary"], { stdio: "inherit" });
    await new Promise(r => setTimeout(r, ttl + 1500));

    const takeover = await redisState(rooms);
    expect(takeover.owners.every((owner: any) => owner?.nodeId === "integration-secondary")).toBeTruthy();

    const recovered = await Promise.all(rooms.flatMap((roomId, room) =>
      Array.from({ length: members }, (_, member) => {
        const user = room + "-" + member;
        return connect(at(room, member), secondary, roomId, user, ids.get(user));
      })
    ));
    expect(recovered.map(x => x.peerId).sort()).toEqual([...ids.values()].sort());
    expect(new Set(recovered.flatMap(x => x.tracks.map(t => t.id))).size).toBe(rooms * members);
    expect(recovered.every(x => x.tracks.every(t => t.state === "live"))).toBeTruthy();
    expect(recovered.every(x => x.remoteTracks > 0 && x.remoteFrames > 0)).toBeTruthy();

    const recoveredMetrics = await metrics(secondary);
    const recoveredRedis = await redisState(rooms);
    expect(recoveredRedis.owners.every((owner: any) => owner?.nodeId === "integration-secondary")).toBeTruthy();
    expect(recoveredRedis.peerCount).toBeGreaterThanOrEqual(rooms * members);
    expect(recoveredMetrics.peers).toBeLessThanOrEqual(baseMetrics.peers + rooms * members);
    expect(recoveredMetrics.rooms).toBeLessThanOrEqual(baseMetrics.rooms + rooms);
    expect(recoveredMetrics.tracks).toBeLessThanOrEqual(baseMetrics.tracks + rooms * members * 2);

    execFileSync("docker", ["compose", "-f", compose, "start", "sfu-primary"], { stdio: "inherit" });
    await health(primary);
    execFileSync("docker", ["compose", "-f", compose, "stop", "sfu-secondary"], { stdio: "inherit" });
    await new Promise(r => setTimeout(r, ttl + 1500));

    const reverse = await redisState(rooms);
    expect(reverse.owners.every((owner: any) => owner?.nodeId === "integration-primary")).toBeTruthy();

    const reverseRecovered = await Promise.all(rooms.flatMap((roomId, room) =>
      Array.from({ length: members }, (_, member) => {
        const user = room + "-" + member;
        return connect(at(room, member), primary, roomId, user, ids.get(user));
      })
    ));
    expect(reverseRecovered.map(x => x.peerId).sort()).toEqual([...ids.values()].sort());
    expect(reverseRecovered.flatMap(x => x.tracks).every(t => t.state === "live")).toBeTruthy();
    expect(reverseRecovered.every(x => x.remoteTracks > 0 && x.remoteFrames > 0)).toBeTruthy();

    const finalMetrics = await metrics(primary);
    const finalRedis = await redisState(rooms);
    expect(finalRedis.owners.every((owner: any) => owner?.nodeId === "integration-primary")).toBeTruthy();
    expect(finalRedis.peerCount).toBeGreaterThanOrEqual(rooms * members);
    expect(finalMetrics.peers).toBeLessThanOrEqual(baseMetrics.peers + rooms * members);
    expect(finalMetrics.rooms).toBeLessThanOrEqual(baseMetrics.rooms + rooms);
    expect(finalMetrics.tracks).toBeLessThanOrEqual(baseMetrics.tracks + rooms * members * 2);

    const artifactDir = process.env.SFU_ARTIFACT_DIR ?? "artifacts";
    fs.mkdirSync(artifactDir, { recursive: true });
    fs.writeFileSync(path.join(artifactDir, "sfu-concurrent-media-chaos-report.json"), JSON.stringify({
      generatedAt: new Date().toISOString(),
      rooms,
      members,
      roomIds: rooms,
      baseMetrics,
      recoveredMetrics,
      finalMetrics,
      baseRedis,
      takeover,
      recoveredRedis,
      reverse,
      finalRedis,
      peerIdentityStable: recovered.map(x => x.peerId).sort().join("|") === [...ids.values()].sort().join("|"),
      trackIdentityCount: new Set(recovered.flatMap(x => x.tracks.map(t => t.id))).size,
      pass: true
    }, null, 2));
  } finally {
    try { execFileSync("docker", ["compose", "-f", compose, "start", "sfu-primary", "sfu-secondary"], { stdio: "inherit" }); } catch {}
    await Promise.all(pages.map(page => page.close()));
  }
});
