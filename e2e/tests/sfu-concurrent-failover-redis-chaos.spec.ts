import { createHmac } from "node:crypto";
import { execFileSync } from "node:child_process";
import fs from "node:fs";
import path from "node:path";
import { test, expect, type Page } from "@playwright/test";
import { waitForClusterReady } from "./helpers/sfu-health";

const primary = process.env.SFU_PRIMARY_URL ?? "http://127.0.0.1:4100";
const secondary = process.env.SFU_SECONDARY_URL ?? "http://127.0.0.1:4200";
const compose = process.env.SFU_FAILOVER_COMPOSE ?? "e2e/docker-compose.sfu-failover.yml";
const redisUrl = process.env.REDIS_URL ?? "redis://127.0.0.1:6379";
const secret = process.env.ROOM_ACCESS_SECRET ?? "integration-secret";
const ttlMs = Number(process.env.SFU_ROOM_OWNER_TTL_MS ?? 3000);
const roomsCount = Math.min(4, Math.max(2, Number(process.env.SFU_CHAOS_ROOMS ?? 3)));
const members = 3;

function accessToken(roomId: string, userId: string) {
  const payload = Buffer.from(JSON.stringify({
    roomId, userId, role: userId.endsWith("-0") ? "host" : "member",
    exp: Math.floor(Date.now() / 1000) + 900
  })).toString("base64url");
  return payload + "." + createHmac("sha256", secret).update(payload).digest("base64url");
}

async function redisSnapshot(roomIds: string[]) {
  const { createClient } = await import("redis");
  const client = createClient({ url: redisUrl });
  await client.connect();
  const result = [];
  for (const roomId of roomIds) {
    const ownerRaw = await client.get("yazykon:sfu:owner:" + roomId);
    const state = await client.exists("yazykon:sfu:state:" + roomId);
    const tracks = await client.exists("yazykon:sfu:tracks:" + roomId);
    const peers = await client.keys("yazykon:sfu:peer:" + roomId + ":*");
    result.push({
      roomId,
      owner: ownerRaw ? JSON.parse(ownerRaw) : null,
      state,
      tracks,
      peerCount: peers.length,
      peerKeys: peers.sort()
    });
  }
  await client.quit();
  return result;
}

async function waitHealthOutage(endpoint: string) {
  for (let i = 0; i < 40; i++) {
    try {
      const response = await fetch(endpoint + "/health");
      if (response.ok) {
        const body = await response.json() as { cluster: boolean };
        if (body.cluster === false) return body;
      }
    } catch {}
    await new Promise(r => setTimeout(r, 250));
  }
  throw new Error("SFU cluster did not enter outage state: " + endpoint);
}

async function connect(page: Page, endpoint: string, roomId: string, userId: string, peerId = "") {
  if (page.url() === "about:blank") await page.goto(process.env.BASE_URL ?? "http://localhost:5173");
  return page.evaluate(async ({ endpoint, roomId, userId, accessToken, peerId }) => {
    const state = (globalThis as any).__concurrentChaosState ??= { stream: undefined, ws: undefined, pc: undefined };
    state.ws?.close();
    state.pc?.close();
    state.stream ??= await navigator.mediaDevices.getUserMedia({ video: true, audio: false });

    const pc = new RTCPeerConnection();
    let remoteTracks = 0;
    let frames = 0;
    pc.ontrack = event => {
      remoteTracks++;
      const video = document.createElement("video");
      video.autoplay = true; video.muted = true; video.playsInline = true;
      video.srcObject = event.streams[0] ?? new MediaStream([event.track]);
      document.body.appendChild(video);
      const tick = () => {
        frames++;
        if ("requestVideoFrameCallback" in video) video.requestVideoFrameCallback(tick);
      };
      video.onloadeddata = tick;
      void video.play().then(tick).catch(() => undefined);
    };

    for (const track of state.stream.getTracks()) pc.addTrack(track, state.stream);

    const ws = new WebSocket(endpoint.replace(/^http/, "ws") + "/ws");
    const queue: any[] = [];
    const waiters = new Map<string, ((message: any) => void)[]>();
    const wait = (type: string) => new Promise<any>((resolve, reject) => {
      const timer = setTimeout(() => reject(new Error("timeout " + type)), 15000);
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
      if (message.type === "ice" && message.data) {
        const candidate = message.data;
        if (pc.remoteDescription) {
          void pc.addIceCandidate(candidate);
        } else {
          const pending = ((globalThis as any).__pendingIce ??= []);
          pending.push(candidate);
        }
      }
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

    ws.send(JSON.stringify({
      type: "join", roomId,
      data: { accessToken, reconnect: Boolean(peerId), ...(peerId ? { peerId } : {}) }
    }));

    const joined = await wait("joined");
    const offer = await pc.createOffer();
    await pc.setLocalDescription(offer);
    ws.send(JSON.stringify({ type: "offer", roomId, data: pc.localDescription }));
    const answer = await wait("answer");
    if (answer.data) {
      await pc.setRemoteDescription(answer.data);
      const pending = ((globalThis as any).__pendingIce ?? []) as RTCIceCandidateInit[];
      while (pending.length) {
        const candidate = pending.shift();
        if (candidate) await pc.addIceCandidate(candidate);
      }
    }

    state.ws = ws;
    state.pc = pc;

    return {
      peerId: joined.peerId,
      trackIds: state.stream.getTracks().map((track: MediaStreamTrack) => track.id),
      trackStates: state.stream.getTracks().map((track: MediaStreamTrack) => track.readyState),
      remoteTracks,
      frames
    };
  }, { endpoint, roomId, userId, accessToken: accessToken(roomId, userId), peerId });
}

test("concurrent SFU crash plus Redis outage converges without ghost media state", async ({ browser }) => {
  test.skip(!process.env.SFU_FAILOVER_LIVE, "Set SFU_FAILOVER_LIVE=1 for the live Docker run");

  await Promise.all([waitForClusterReady(primary), waitForClusterReady(secondary)]);

  const rooms = Array.from({ length: roomsCount }, (_, index) => "REDIS-CHAOS-" + Date.now() + "-" + index);
  const pages: Page[] = [];
  const participants: { page: Page; roomId: string; userId: string; peerId: string; trackIds: string[] }[] = [];

  try {
    for (const roomId of rooms) {
      for (let member = 0; member < members; member++) {
        const page = await browser.newPage({ permissions: ["camera", "microphone"] });
        // getUserMedia is only exposed in a secure/trusted browsing context.
        // Opening the health endpoint first also makes this deterministic in headless CI.
        await page.goto(primary + "/health");
        pages.push(page);
        participants.push({ page, roomId, userId: roomId + "-user-" + member, peerId: "", trackIds: [] });
      }
    }

    const initial = await Promise.all(participants.map(p => connect(p.page, primary, p.roomId, p.userId)));
    initial.forEach((result, index) => {
      participants[index].peerId = result.peerId;
      participants[index].trackIds = result.trackIds;
    });

    expect(new Set(participants.map(p => p.peerId)).size).toBe(participants.length);
    expect(new Set(participants.flatMap(p => p.trackIds)).size).toBe(participants.length);
    const baseline = await redisSnapshot(rooms);
    expect(baseline.every(item => item.owner?.nodeId === "integration-primary")).toBeTruthy();
    expect(baseline.every(item => item.peerCount === members)).toBeTruthy();

    const outageStarted = Date.now();

    execFileSync("docker", ["compose", "-f", compose, "stop", "sfu-primary"], { stdio: "inherit" });
    await new Promise(r => setTimeout(r, 500));

    execFileSync("docker", ["compose", "-f", compose, "stop", "redis"], { stdio: "inherit" });
    await waitHealthOutage(secondary);

    await new Promise(r => setTimeout(r, ttlMs + 1000));

    execFileSync("docker", ["compose", "-f", compose, "start", "redis"], { stdio: "inherit" });
    await waitForClusterReady(secondary);

    let takeover: Awaited<ReturnType<typeof redisSnapshot>> = [];
    for (let attempt = 0; attempt < 20; attempt++) {
      takeover = await redisSnapshot(rooms);
      if (takeover.every(item => item.owner?.nodeId === "integration-secondary")) break;
      await new Promise(r => setTimeout(r, 250));
    }
    expect(takeover.every(item => item.owner?.nodeId === "integration-secondary")).toBeTruthy();

    const recovered = await Promise.all(
      participants.map(p => connect(p.page, secondary, p.roomId, p.userId, p.peerId))
    );
    recovered.forEach((result, index) => {
      expect(result.peerId).toBe(participants[index].peerId);
      expect(result.trackIds).toEqual(participants[index].trackIds);
      expect(result.trackStates.every((state: string) => state === "live")).toBeTruthy();
    });

    const secondaryState = await redisSnapshot(rooms);
    expect(secondaryState.every(item => item.owner?.nodeId === "integration-secondary")).toBeTruthy();
    expect(secondaryState.every(item => item.peerCount === members)).toBeTruthy();

    execFileSync("docker", ["compose", "-f", compose, "start", "sfu-primary"], { stdio: "inherit" });
    await waitForClusterReady(primary);
    execFileSync("docker", ["compose", "-f", compose, "stop", "sfu-secondary"], { stdio: "inherit" });
    await waitHealthOutage(secondary);
    await new Promise(r => setTimeout(r, ttlMs + 500));

    const reverse = await redisSnapshot(rooms);
    expect(reverse.every(item => item.owner?.nodeId === "integration-primary")).toBeTruthy();

    const final = await Promise.all(
      participants.map(p => connect(p.page, primary, p.roomId, p.userId, p.peerId))
    );
    final.forEach((result, index) => {
      expect(result.peerId).toBe(participants[index].peerId);
      expect(result.trackIds).toEqual(participants[index].trackIds);
      expect(result.trackStates.every((state: string) => state === "live")).toBeTruthy();
    });

    await Promise.all(pages.map(page => page.close()));
    pages.length = 0;
    await new Promise(r => setTimeout(r, ttlMs + 1000));

    const cleaned = await redisSnapshot(rooms);
    expect(cleaned.every(item => item.peerCount === 0)).toBeTruthy();
    expect(cleaned.every(item => item.owner === null || item.owner.nodeId === "integration-primary")).toBeTruthy();

    const artifactDir = process.env.SFU_ARTIFACT_DIR ?? "artifacts";
    fs.mkdirSync(artifactDir, { recursive: true });
    fs.writeFileSync(
      path.join(artifactDir, "sfu-concurrent-failover-redis-chaos-report.json"),
      JSON.stringify({
        generatedAt: new Date().toISOString(),
        rooms,
        participants: participants.length,
        outageMs: Date.now() - outageStarted,
        baseline,
        takeover,
        secondaryState,
        reverse,
        cleaned,
        peerIdentityStable: recovered.every((result, index) => result.peerId === participants[index].peerId) &&
          final.every((result, index) => result.peerId === participants[index].peerId),
        trackIdentityStable: recovered.every((result, index) => JSON.stringify(result.trackIds) === JSON.stringify(participants[index].trackIds)) &&
          final.every((result, index) => JSON.stringify(result.trackIds) === JSON.stringify(participants[index].trackIds)),
        ghostPeerKeys: cleaned.reduce((sum, item) => sum + item.peerCount, 0),
        pass: true
      }, null, 2)
    );
  } finally {
    try { execFileSync("docker", ["compose", "-f", compose, "start", "redis", "sfu-primary", "sfu-secondary"], { stdio: "inherit" }); } catch {}
    await Promise.all(pages.map(page => page.close()));
  }
});
