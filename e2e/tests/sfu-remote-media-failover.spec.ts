import { createHmac } from "node:crypto";
import { execFileSync } from "node:child_process";
import { test, expect } from "@playwright/test";

const primary = process.env.SFU_PRIMARY_URL ?? "http://127.0.0.1:4100";
const secondary = process.env.SFU_SECONDARY_URL ?? "http://127.0.0.1:4200";
const roomId = process.env.SFU_REMOTE_FAILOVER_ROOM ?? "REMOTE-MEDIA-FAILOVER";
const secret = process.env.ROOM_ACCESS_SECRET ?? "integration-secret";
const ttlMs = Number(process.env.SFU_ROOM_OWNER_TTL_MS ?? 3500);
const composeFile = process.env.SFU_FAILOVER_COMPOSE ?? "e2e/docker-compose.sfu-failover.yml";

function token(userId: string) {
  const payload = Buffer.from(JSON.stringify({
    roomId, userId, role: userId === "p1" ? "host" : "participant",
    exp: Math.floor(Date.now() / 1000) + 300
  })).toString("base64url");
  return payload + "." + createHmac("sha256", secret).update(payload).digest("base64url");
}

async function connect(page: import("@playwright/test").Page, endpoint: string, userId: string, peerId = "") {
  return page.evaluate(async ({ endpoint, roomId, accessToken, peerId }) => {
    const stream = (window as any).__remoteFailoverStream as MediaStream | undefined
      ?? await navigator.mediaDevices.getUserMedia({ video: true, audio: false });
    (window as any).__remoteFailoverStream = stream;

    const pc = new RTCPeerConnection();
    (window as any).__remoteFailoverConnections = ((window as any).__remoteFailoverConnections ?? 0) + 1;
    (window as any).__remoteFailoverConnectionStates ??= [];
    (window as any).__remoteFailoverConnectionStates.push(pc);
    let remoteFrames = 0;
    const frameWaiters: Array<() => void> = [];

    pc.ontrack = event => {
      const remote = event.streams[0] ?? new MediaStream([event.track]);
      const video = document.createElement("video");
      video.autoplay = true;
      video.muted = true;
      video.playsInline = true;
      video.srcObject = remote;
      document.body.appendChild(video);
      const markFrame = () => {
        remoteFrames++;
        for (const resolve of frameWaiters.splice(0)) resolve();
      };
      video.onloadeddata = markFrame;
      void video.play().then(markFrame).catch(() => undefined);
      if (video.requestVideoFrameCallback) video.requestVideoFrameCallback(markFrame);
    };

    const ws = new WebSocket(endpoint.replace(/^http/, "ws") + "/ws");
    const queue: any[] = [];
    const waiters = new Map<string, Array<(message: any) => void>>();

    const waitFor = (type: string) => new Promise<any>((resolve, reject) => {
      const queued = queue.findIndex(message => message.type === type);
      if (queued >= 0) return resolve(queue.splice(queued, 1)[0]);
      const timer = setTimeout(() => reject(new Error("timeout waiting for " + type)), 15000);
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

    await new Promise<void>((resolve, reject) => {
      ws.onopen = () => resolve();
      ws.onerror = () => reject(new Error("websocket failed: " + endpoint));
    });

    pc.onicecandidate = event => {
      if (event.candidate && ws.readyState === WebSocket.OPEN) {
        ws.send(JSON.stringify({ type: "ice", roomId, data: event.candidate.toJSON() }));
      }
    };

    ws.send(JSON.stringify({
      type: "join",
      roomId,
      peerId: peerId || undefined,
      data: { accessToken, reconnect: Boolean(peerId) }
    }));
    const joined = await waitFor("joined");

    const offer = await pc.createOffer();
    await pc.setLocalDescription(offer);
    ws.send(JSON.stringify({ type: "offer", roomId, data: pc.localDescription }));
    const answer = await waitFor("answer");
    await pc.setRemoteDescription(answer.data);

    return { peerId: joined.peerId, remoteFrames, remotePeers: joined.data?.peers ?? [] };
  }, { endpoint, roomId, accessToken: token(userId), peerId });
}

async function waitForFrame(page: import("@playwright/test").Page) {
  await page.evaluate(() => new Promise<void>((resolve, reject) => {
    const timer = setTimeout(() => reject(new Error("remote video frame timeout")), 15000);
    const check = () => {
      const videos = [...document.querySelectorAll("video")];
      if (videos.some(video => video.readyState >= 2)) {
        clearTimeout(timer);
        resolve();
        return;
      }
      requestAnimationFrame(check);
    };
    check();
  });
}

test("two participants recover remote media after SFU failover", async ({ browser }) => {
  test.skip(!process.env.SFU_FAILOVER_LIVE, "Set SFU_FAILOVER_LIVE=1 for the live Docker failover run");

  const pages = await Promise.all([
    browser.newPage({ permissions: ["camera", "microphone"] }),
    browser.newPage({ permissions: ["camera", "microphone"] })
  ]);
  await Promise.all(pages.map(page => page.goto(primary + "/health")));

  const first = await connect(pages[0], primary, "p1");
  const second = await connect(pages[1], primary, "p2");
  expect(second.remotePeers).toContain(first.peerId);

  await waitForFrame(pages[1]);

  execFileSync("docker", ["compose", "-f", composeFile, "stop", "sfu-primary"], { stdio: "inherit" });
  await new Promise(resolve => setTimeout(resolve, ttlMs + 2000));

  const recovered = await Promise.all([
    connect(pages[0], secondary, "p1", first.peerId),
    connect(pages[1], secondary, "p2", second.peerId)
  ]);

  expect(recovered[0].peerId).toBe(first.peerId);
  expect(recovered[1].peerId).toBe(second.peerId);
  expect(recovered[0].remotePeers).toContain(second.peerId);
  expect(recovered[1].remotePeers).toContain(first.peerId);

  await Promise.all(recovered.map((_, index) => waitForFrame(pages[index])));

  const connectionStates = await Promise.all(pages.map(page => page.evaluate(() =>
    ((window as any).__remoteFailoverConnectionStates ?? []).map((pc: RTCPeerConnection) => pc.connectionState)
  )));
  expect(connectionStates.every(states => states.some(state => state === "connected" || state === "completed"))).toBeTruthy();

  const mediaState = await Promise.all(pages.map(page => page.evaluate(() =>
    [...document.querySelectorAll("video")].map(video => ({
      readyState: video.readyState,
      width: video.videoWidth,
      height: video.videoHeight
    }))
  )));

  for (const videos of mediaState) {
    expect(videos.some(video => video.readyState >= 2 && video.width > 0 && video.height > 0)).toBeTruthy();
  }

  execFileSync("docker", ["compose", "-f", composeFile, "start", "sfu-primary"], { stdio: "inherit" });
  for (let attempt = 0; attempt < 30; attempt++) {
    try {
      const health = await fetch(primary + "/health");
      if (health.ok) break;
    } catch {}
    await new Promise(resolve => setTimeout(resolve, 500));
    if (attempt === 29) throw new Error("SFU primary did not recover");
  }

  const roundTrip = await Promise.all([
    connect(pages[0], primary, "p1", recovered[0].peerId),
    connect(pages[1], primary, "p2", recovered[1].peerId)
  ]);

  expect(roundTrip[0].peerId).toBe(first.peerId);
  expect(roundTrip[1].peerId).toBe(second.peerId);
  expect(roundTrip[0].remotePeers).toContain(second.peerId);
  expect(roundTrip[1].remotePeers).toContain(first.peerId);
  await Promise.all(roundTrip.map((_, index) => waitForFrame(pages[index])));

  const roundTripMedia = await Promise.all(pages.map(page => page.evaluate(() =>
    [...document.querySelectorAll("video")].map(video => ({
      readyState: video.readyState,
      width: video.videoWidth,
      height: video.videoHeight
    }))
  )));
  for (const videos of roundTripMedia) {
    expect(videos.some(video => video.readyState >= 2 && video.width > 0 && video.height > 0)).toBeTruthy();
  }

  await Promise.all(pages.map(page => page.close()));
});
