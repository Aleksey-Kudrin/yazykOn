import { createHmac } from "node:crypto";
import { execFileSync } from "node:child_process";
import { test, expect } from "@playwright/test";

const primary = process.env.SFU_PRIMARY_URL ?? "http://127.0.0.1:4100";
const secondary = process.env.SFU_SECONDARY_URL ?? "http://127.0.0.1:4200";
const redisUrl = process.env.REDIS_URL ?? "redis://127.0.0.1:6379";
const roomId = process.env.SFU_CHAOS_ROOM ?? "CHAOS-FAILOVER";
const secret = process.env.ROOM_ACCESS_SECRET ?? "integration-secret";
const ttlMs = Number(process.env.SFU_ROOM_OWNER_TTL_MS ?? 3500);
const composeFile = process.env.SFU_FAILOVER_COMPOSE ?? "e2e/docker-compose.sfu-failover.yml";

function token() {
  const payload = Buffer.from(JSON.stringify({
    roomId,
    userId: "chaos-failover-user",
    role: "host",
    exp: Math.floor(Date.now() / 1000) + 600
  })).toString("base64url");
  const sig = createHmac("sha256", secret).update(payload).digest("base64url");
  return `${payload}.${sig}`;
}

async function redisTracks() {
  const { createClient } = await import("redis");
  const client = createClient({ url: redisUrl });
  await client.connect();
  const values = await client.hGetAll(`yazykon:sfu:tracks:${roomId}`);
  await client.quit();
  return Object.values(values).map(value => JSON.parse(value) as {
    peerId: string;
    trackId: string;
    sessionId: string;
    kind: string;
  });
}

function compose(...args: string[]) {
  execFileSync("docker", ["compose", "-f", composeFile, ...args], { stdio: "inherit" });
}

async function waitForHealth(url: string, timeoutMs = 30000) {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    try {
      const response = await fetch(`${url}/health`);
      if (response.ok) return;
    } catch {}
    await new Promise(resolve => setTimeout(resolve, 500));
  }
  throw new Error(`SFU did not become healthy: ${url}`);
}

test("chaos: repeated reconnect plus simultaneous SFU/Redis outage preserves media state", async ({ browser }) => {
  test.skip(!process.env.SFU_CHAOS_LIVE, "Set SFU_CHAOS_LIVE=1 for the live Docker chaos run");

  const page = await browser.newPage({ permissions: ["camera", "microphone"] });
  await page.goto(`${primary}/health`);
  await page.context().grantPermissions(["camera", "microphone"]);

  const authToken = token();
  const connected = await page.evaluate(async ({ primary, roomId, authToken }) => {
    const stream = await navigator.mediaDevices.getUserMedia({ video: true, audio: false });
    (window as any).__chaosStream = stream;
    const track = stream.getVideoTracks()[0];
    if (!track || track.readyState !== "live") throw new Error("camera track is not live");

    const pc = new RTCPeerConnection();
    pc.addTrack(track, stream);
    const ws = new WebSocket(primary.replace(/^http/, "ws") + "/ws");
    const queue: any[] = [];
    let waiter: ((value: any) => void) | undefined;
    ws.onmessage = event => {
      const message = JSON.parse(event.data);
      if (waiter) { const resolve = waiter; waiter = undefined; resolve(message); }
      else queue.push(message);
    };
    const next = (type: string) => new Promise<any>((resolve, reject) => {
      const timer = setTimeout(() => reject(new Error(`timeout: ${type}`)), 15000);
      const take = (message: any) => {
        if (message.type === type) { clearTimeout(timer); resolve(message); }
        else waiter = take;
      };
      const index = queue.findIndex(message => message.type === type);
      if (index >= 0) {
        const message = queue.splice(index, 1)[0];
        clearTimeout(timer);
        resolve(message);
      } else waiter = take;
    });

    await new Promise<void>((resolve, reject) => {
      ws.onopen = () => resolve();
      ws.onerror = () => reject(new Error("primary websocket failed"));
    });
    ws.send(JSON.stringify({
      type: "join",
      roomId,
      data: { accessToken: authToken }
    }));
    const joined = await next("joined");
    const peerId = joined.peerId as string;
    const offer = await pc.createOffer();
    await pc.setLocalDescription(offer);
    ws.send(JSON.stringify({ type: "offer", roomId, data: pc.localDescription }));
    await next("answer");

    return { peerId, trackId: track.id, connectionState: pc.connectionState };
  }, { primary, roomId, authToken });

  expect(connected.peerId).toBeTruthy();
  expect(connected.trackId).toBeTruthy();
  const initialTracks = await redisTracks();
  const initial = initialTracks.find(track => track.peerId === connected.peerId && track.kind === "video");
  expect(initial).toBeTruthy();
  const firstSession = initial!.sessionId;

  // Reconnect repeatedly without a server failure. This catches stale WebSocket,
  // peer/session and publication cleanup bugs before the harder infrastructure chaos.
  for (let cycle = 1; cycle <= 3; cycle++) {
    const recovery = await page.evaluate(async ({ primary, roomId, authToken, peerId }) => {
      const stream = (window as any).__chaosStream as MediaStream | undefined;
      if (!stream) throw new Error("local stream was lost");
      const track = stream.getVideoTracks()[0];
      if (!track || track.readyState !== "live") throw new Error("local track was lost");
      const pc = new RTCPeerConnection();
      pc.addTrack(track, stream);
      const ws = new WebSocket(primary.replace(/^http/, "ws") + "/ws");
      const queue: any[] = [];
      let waiter: ((value: any) => void) | undefined;
      ws.onmessage = event => {
        const message = JSON.parse(event.data);
        if (waiter) { const resolve = waiter; waiter = undefined; resolve(message); }
        else queue.push(message);
      };
      const next = (type: string) => new Promise<any>((resolve, reject) => {
        const timer = setTimeout(() => reject(new Error(`timeout: ${type}`)), 15000);
        const take = (message: any) => {
          if (message.type === type) { clearTimeout(timer); resolve(message); }
          else waiter = take;
        };
        const index = queue.findIndex(message => message.type === type);
        if (index >= 0) {
          const message = queue.splice(index, 1)[0];
          clearTimeout(timer);
          resolve(message);
        } else waiter = take;
      });
      await new Promise<void>((resolve, reject) => {
        ws.onopen = () => resolve();
        ws.onerror = () => reject(new Error("reconnect websocket failed"));
      });
      ws.send(JSON.stringify({ type: "join", roomId, peerId, data: { accessToken: authToken, reconnect: true } }));
      const joined = await next("joined");
      expect(joined.peerId).toBe(peerId);
      const offer = await pc.createOffer();
      await pc.setLocalDescription(offer);
      ws.send(JSON.stringify({ type: "offer", roomId, data: pc.localDescription }));
      await next("answer");
      ws.close();
      pc.close();
      return { peerId: joined.peerId, trackId: track.id, ready: track.readyState === "live" };
    }, { primary, roomId, authToken, peerId: connected.peerId });

    expect(recovery.peerId).toBe(connected.peerId);
    expect(recovery.trackId).toBe(connected.trackId);
    expect(recovery.ready).toBeTruthy();
    await page.waitForTimeout(300);

    const tracks = await redisTracks();
    const current = tracks.find(track => track.peerId === connected.peerId && track.kind === "video");
    expect(current).toBeTruthy();
    expect(current!.trackId).toBeTruthy();
    expect(current!.sessionId).not.toBe(firstSession);
  }

  // Hard outage: all SFU nodes and Redis go down together. Recovery starts
  // Redis first, then both SFUs; the browser keeps its local MediaStream alive
  // and reconnects with the same stable peer ID after the owner lease expires.
  compose("stop", "sfu-primary", "sfu-secondary", "redis");
  await new Promise(resolve => setTimeout(resolve, ttlMs + 1500));
  compose("start", "redis", "sfu-primary", "sfu-secondary");
  await waitForHealth(primary);
  await waitForHealth(secondary);

  const afterRestart = await page.evaluate(async ({ primary, roomId, authToken, peerId }) => {
    const stream = (window as any).__chaosStream as MediaStream | undefined;
    if (!stream) throw new Error("local stream disappeared during infrastructure outage");
    const track = stream.getVideoTracks()[0];
    if (!track || track.readyState !== "live") throw new Error("local camera track died during infrastructure outage");

    const pc = new RTCPeerConnection();
    pc.addTrack(track, stream);
    const ws = new WebSocket(primary.replace(/^http/, "ws") + "/ws");
    const queue: any[] = [];
    let waiter: ((value: any) => void) | undefined;
    ws.onmessage = event => {
      const message = JSON.parse(event.data);
      if (waiter) { const resolve = waiter; waiter = undefined; resolve(message); }
      else queue.push(message);
    };
    const next = (type: string) => new Promise<any>((resolve, reject) => {
      const timer = setTimeout(() => reject(new Error(`timeout: ${type}`)), 20000);
      const take = (message: any) => {
        if (message.type === type) { clearTimeout(timer); resolve(message); }
        else waiter = take;
      };
      const index = queue.findIndex(message => message.type === type);
      if (index >= 0) {
        const message = queue.splice(index, 1)[0];
        clearTimeout(timer);
        resolve(message);
      } else waiter = take;
    });

    await new Promise<void>((resolve, reject) => {
      ws.onopen = () => resolve();
      ws.onerror = () => reject(new Error("primary websocket failed after full outage"));
    });
    ws.send(JSON.stringify({ type: "join", roomId, peerId, data: { accessToken: authToken, reconnect: true } }));
    const joined = await next("joined");
    const offer = await pc.createOffer();
    await pc.setLocalDescription(offer);
    ws.send(JSON.stringify({ type: "offer", roomId, data: pc.localDescription }));
    await next("answer");
    return { peerId: joined.peerId, trackId: track.id, ready: track.readyState === "live" };
  }, { primary, roomId, authToken, peerId: connected.peerId });

  expect(afterRestart.peerId).toBe(connected.peerId);
  expect(afterRestart.trackId).toBe(connected.trackId);
  expect(afterRestart.ready).toBeTruthy();

  await page.waitForTimeout(1000);
  const finalTracks = await redisTracks();
  const final = finalTracks.find(track => track.peerId === connected.peerId && track.kind === "video");
  expect(final).toBeTruthy();
  expect(final!.trackId).toBeTruthy();
  expect(final!.sessionId).not.toBe(firstSession);

  await page.close();
});
