import { createHmac } from "node:crypto";
import { execFileSync } from "node:child_process";
import fs from "node:fs";
import path from "node:path";
import { test, expect } from "@playwright/test";
import { waitForClusterReady } from "./helpers/sfu-health";

const primary = process.env.SFU_PRIMARY_URL ?? "http://127.0.0.1:4100";
const secondary = process.env.SFU_SECONDARY_URL ?? "http://127.0.0.1:4200";
const compose = process.env.SFU_FAILOVER_COMPOSE ?? "e2e/docker-compose.sfu-failover.yml";
const redisUrl = process.env.REDIS_URL ?? "redis://127.0.0.1:6379";
const secret = process.env.ROOM_ACCESS_SECRET ?? "integration-secret";
const roomTtl = Number(process.env.SFU_ROOM_OWNER_TTL_MS ?? 3000);

function token(roomId: string, userId: string) {
  const payload = Buffer.from(JSON.stringify({
    roomId, userId, role: userId.endsWith("-0") ? "host" : "participant",
    exp: Math.floor(Date.now() / 1000) + 600
  })).toString("base64url");
  return payload + "." + createHmac("sha256", secret).update(payload).digest("base64url");
}

async function health(endpoint: string) {
  for (let i = 0; i < 40; i++) {
    try {
      const response = await fetch(endpoint + "/health");
      if (response.ok) return await response.json() as { cluster: boolean; nodeId: string };
    } catch {}
    await new Promise(r => setTimeout(r, 250));
  }
  throw new Error("health timeout: " + endpoint);
}

async function owner(roomId: string) {
  const { createClient } = await import("redis");
  const client = createClient({ url: redisUrl });
  await client.connect();
  const raw = await client.get("yazykon:sfu:owner:" + roomId);
  await client.quit();
  return raw ? JSON.parse(raw) : null;
}

async function connect(page: import("@playwright/test").Page, endpoint: string, roomId: string, userId: string, peerId = "") {
  if (page.url() === "about:blank") await page.goto(process.env.BASE_URL ?? "http://localhost:5173");
  return page.evaluate(async ({ endpoint, roomId, accessToken, peerId }) => {
    const state = (globalThis as any).__redisRecovery ??= { stream: undefined, ws: undefined, pc: undefined };
    state.ws?.close(); state.pc?.close();
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
      const timer = setTimeout(() => reject(new Error("timeout: " + type)), 15000);
      const queued = queue.findIndex(message => message.type === type);
      if (queued >= 0) {
        clearTimeout(timer);
        return resolve(queue.splice(queued, 1)[0]);
      }
      const list = waiters.get(type) ?? [];
      list.push(message => { clearTimeout(timer); resolve(message); });
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
    ws.send(JSON.stringify({
      type: "join", roomId,
      data: { accessToken, reconnect: Boolean(peerId), ...(peerId ? { peerId } : {}) }
    }));
    const joined = await wait("joined");
    const offer = await pc.createOffer();
    await pc.setLocalDescription(offer);
    ws.send(JSON.stringify({ type: "offer", roomId, data: pc.localDescription }));
    const answer = await wait("answer");
    if (answer.data) await pc.setRemoteDescription(answer.data);
    state.ws = ws; state.pc = pc;
    return {
      peerId: joined.peerId,
      trackIds: state.stream.getTracks().map((track: MediaStreamTrack) => track.id),
      trackStates: state.stream.getTracks().map((track: MediaStreamTrack) => track.readyState),
      remoteTracks, frames
    };
  }, { endpoint, roomId, accessToken: token(roomId, userId), peerId });
}

test("media reconnect converges after Redis control-plane recovery", async ({ browser }) => {
  test.skip(!process.env.SFU_FAILOVER_LIVE, "Set SFU_FAILOVER_LIVE=1 for the live Docker run");
  await health(primary);
  await health(secondary);

  const roomId = "REDIS-MEDIA-RECOVERY-" + Date.now();
  const pages = [
    await browser.newPage({ permissions: ["camera", "microphone"] }),
    await browser.newPage({ permissions: ["camera", "microphone"] })
  ];
  await Promise.all(pages.map(page => page.goto(primary + "/health")));

  try {
    const first = await connect(pages[0], primary, roomId, "redis-recovery-0");
    const second = await connect(pages[1], primary, roomId, "redis-recovery-1");
    expect(first.peerId).not.toBe(second.peerId);
    expect(new Set([...first.trackIds, ...second.trackIds]).size).toBe(first.trackIds.length + second.trackIds.length);

    const initialOwner = await owner(roomId);
    expect(initialOwner?.nodeId).toBe("integration-primary");

    const started = Date.now();
    execFileSync("docker", ["compose", "-f", compose, "stop", "redis"], { stdio: "inherit" });

    await new Promise(r => setTimeout(r, Math.max(3500, roomTtl + 500)));
    const primaryDuringOutage = await health(primary);
    const secondaryDuringOutage = await health(secondary);
    expect(primaryDuringOutage.cluster).toBeFalsy();
    expect(secondaryDuringOutage.cluster).toBeFalsy();

    execFileSync("docker", ["compose", "-f", compose, "start", "redis"], { stdio: "inherit" });
    const restoredPrimary = await health(primary);
    const restoredSecondary = await health(secondary);
    expect(restoredPrimary.cluster).toBeTruthy();
    expect(restoredSecondary.cluster).toBeTruthy();

    let recoveredOwner: any = null;
    for (let i = 0; i < 20; i++) {
      recoveredOwner = await owner(roomId);
      if (recoveredOwner?.nodeId === "integration-primary") break;
      await new Promise(r => setTimeout(r, 250));
    }
    expect(recoveredOwner?.nodeId).toBe("integration-primary");

    const recovered = await Promise.all([
      connect(pages[0], primary, roomId, "redis-recovery-0", first.peerId),
      connect(pages[1], primary, roomId, "redis-recovery-1", second.peerId)
    ]);
    expect(recovered[0].peerId).toBe(first.peerId);
    expect(recovered[1].peerId).toBe(second.peerId);
    expect(recovered[0].trackIds).toEqual(first.trackIds);
    expect(recovered[1].trackIds).toEqual(second.trackIds);
    expect(recovered.every(result => result.trackStates.every((state: string) => state === "live"))).toBeTruthy();

    fs.mkdirSync(process.env.SFU_ARTIFACT_DIR ?? "artifacts", { recursive: true });
    fs.writeFileSync(
      path.join(process.env.SFU_ARTIFACT_DIR ?? "artifacts", "sfu-media-redis-recovery-report.json"),
      JSON.stringify({
        generatedAt: new Date().toISOString(),
        roomId,
        initialOwner,
        primaryDuringOutage,
        secondaryDuringOutage,
        restoredPrimary,
        restoredSecondary,
        recoveredOwner,
        recoveryMs: Date.now() - started,
        peerIdentityStable: recovered.every((result, i) => result.peerId === [first, second][i].peerId),
        trackIdentityStable: recovered.every((result, i) => JSON.stringify(result.trackIds) === JSON.stringify([first, second][i].trackIds)),
        pass: true
      }, null, 2)
    );
  } finally {
    try { execFileSync("docker", ["compose", "-f", compose, "start", "redis", "sfu-primary", "sfu-secondary"], { stdio: "inherit" }); } catch {}
    await Promise.all(pages.map(page => page.close()));
  }
});
