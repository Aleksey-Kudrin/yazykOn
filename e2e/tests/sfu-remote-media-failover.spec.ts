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
    roomId, userId, role: userId === "p1" ? "host" : "member",
    exp: Math.floor(Date.now() / 1000) + 300
  })).toString("base64url");
  return payload + "." + createHmac("sha256", secret).update(payload).digest("base64url");
}

async function connect(page: import("@playwright/test").Page, endpoint: string, userId: string, peerId = "") {
  if (page.url() === "about:blank") await page.goto(process.env.BASE_URL ?? "http://localhost:5173");
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

async function redisPeerKeys() {
  const { createClient } = await import("redis");
  const client = createClient({ url: process.env.REDIS_URL ?? "redis://127.0.0.1:6379" });
  await client.connect();
  const keys = await client.keys("yazykon:sfu:peer:*");
  await client.quit();
  return keys;
}

async function waitForFrame(page: import("@playwright/test").Page) {
  await page.evaluate(() => new Promise<void>((resolve, reject) => {
    const timer = setTimeout(() => reject(new Error("remote video frame timeout")), 15000);
    const check = () => {
      const videos = [...document.querySelectorAll("video")];
      if (videos.some(video => video.readyState >= 2 && video.videoWidth > 0 && video.videoHeight > 0)) {
        clearTimeout(timer);
        resolve();
        return;
      }
      requestAnimationFrame(check);
    };
    check();
  }));
}

test("two participants recover remote media through repeated SFU failover cycles", async ({ browser }) => {
  test.skip(!process.env.SFU_FAILOVER_LIVE, "Set SFU_FAILOVER_LIVE=1 for the live Docker failover run");

  const pages = await Promise.all([
    browser.newPage({ permissions: ["camera", "microphone"] }),
    browser.newPage({ permissions: ["camera", "microphone"] })
  ]);

  const initial = await Promise.all(pages.map(page => page.goto(primary + "/health")));
  expect(initial.every(response => response?.ok())).toBeTruthy();

  let peerIds = ["", ""];
  let endpoint = primary;
  let stoppedService = "sfu-primary";

  try {
    const first = await connect(pages[0], endpoint, "p1");
    const second = await connect(pages[1], endpoint, "p2");
    peerIds = [first.peerId, second.peerId];
    expect(second.remotePeers).toContain(first.peerId);
    await Promise.all(pages.map(page => waitForFrame(page)));
    const initialPeerSet = new Set(peerIds);
    expect(initialPeerSet.size).toBe(peerIds.length);
    const initialPeerKeys = await redisPeerKeys();

    for (let cycle = 0; cycle < 4; cycle++) {
      const nextEndpoint = endpoint === primary ? secondary : primary;
      stoppedService = endpoint === primary ? "sfu-primary" : "sfu-secondary";

      const startedAt = Date.now();
      execFileSync("docker", ["compose", "-f", composeFile, "stop", stoppedService], { stdio: "inherit" });
      await new Promise(resolve => setTimeout(resolve, ttlMs + 2000));

      const ownerEndpoint = nextEndpoint;
      const health = await fetch(ownerEndpoint + "/health");
      expect(health.ok).toBeTruthy();

      const recovered = await Promise.all([
        connect(pages[0], nextEndpoint, "p1", peerIds[0]),
        connect(pages[1], nextEndpoint, "p2", peerIds[1])
      ]);

      expect(recovered[0].peerId).toBe(peerIds[0]);
      expect(recovered[1].peerId).toBe(peerIds[1]);
      expect(recovered[0].remotePeers).toContain(peerIds[1]);
      expect(recovered[1].remotePeers).toContain(peerIds[0]);
      const recoveredIds = recovered.map(result => result.peerId);
      expect(new Set(recoveredIds).size).toBe(recoveredIds.length);
      expect(recoveredIds).toEqual(peerIds);
      const peerKeys = await redisPeerKeys();
      expect(peerKeys.length).toBeLessThanOrEqual(initialPeerKeys.length + 1);

      await Promise.all(pages.map(page => waitForFrame(page)));

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

      const connectionStates = await Promise.all(pages.map(page => page.evaluate(() =>
        ((window as any).__remoteFailoverConnectionStates ?? []).map((pc: RTCPeerConnection) => pc.connectionState)
      )));
      expect(connectionStates.every(states => states.some(state => state === "connected" || state === "completed"))).toBeTruthy();
      expect(connectionStates.every(states => states.filter(state => state === "connected" || state === "completed").length <= 1)).toBeTruthy();

      const recoveryMs = Date.now() - startedAt;
      expect(recoveryMs).toBeLessThan(Number(process.env.SFU_FAILOVER_SLA_MS ?? 15000));

      if (cycle < 3) {
        execFileSync("docker", ["compose", "-f", composeFile, "start", stoppedService], { stdio: "inherit" });
        const restartedEndpoint = endpoint;
        for (let attempt = 0; attempt < 30; attempt++) {
          try {
            const response = await fetch(restartedEndpoint + "/health");
            if (response.ok) break;
          } catch {}
          await new Promise(resolve => setTimeout(resolve, 500));
          if (attempt === 29) throw new Error(stoppedService + " did not recover");
        }
      }

      endpoint = nextEndpoint;
    }
  } finally {
    try {
      execFileSync("docker", ["compose", "-f", composeFile, "start", "sfu-primary", "sfu-secondary"], { stdio: "inherit" });
    } catch {}
    await Promise.all(pages.map(page => page.close()));
  }
});


test("four participants recover remote media through repeated SFU failover cycles", async ({ browser }) => {
  test.skip(!process.env.SFU_FAILOVER_LIVE, "Set SFU_FAILOVER_LIVE=1 for the live Docker failover run");

  const pages = await Promise.all(
    ["p1", "p2", "p3", "p4"].map(() => browser.newPage({ permissions: ["camera", "microphone"] }))
  );
  let endpoint = primary;
  const peerIds: string[] = [];
  let stoppedService = "sfu-primary";

  try {
    await Promise.all(pages.map(page => page.goto(primary + "/health")));

    for (let i = 0; i < pages.length; i++) {
      const joined = await connect(pages[i], endpoint, `p${i + 1}`);
      peerIds.push(joined.peerId);
    }

    await Promise.all(pages.map(page => waitForFrame(page)));
    expect(new Set(peerIds).size).toBe(peerIds.length);
    const initialPeerKeys = await redisPeerKeys();

    for (let cycle = 0; cycle < 4; cycle++) {
      const nextEndpoint = endpoint === primary ? secondary : primary;
      stoppedService = endpoint === primary ? "sfu-primary" : "sfu-secondary";
      const startedAt = Date.now();

      execFileSync("docker", ["compose", "-f", composeFile, "stop", stoppedService], { stdio: "inherit" });
      await new Promise(resolve => setTimeout(resolve, ttlMs + 2000));

      const health = await fetch(nextEndpoint + "/health");
      expect(health.ok).toBeTruthy();

      const recovered = await Promise.all(
        pages.map((page, i) => connect(page, nextEndpoint, `p${i + 1}`, peerIds[i]))
      );

      recovered.forEach((result, i) => {
        expect(result.peerId).toBe(peerIds[i]);
        for (const otherPeerId of peerIds) {
          if (otherPeerId !== peerIds[i]) expect(result.remotePeers).toContain(otherPeerId);
        }
      });
      expect(new Set(recovered.map(result => result.peerId)).size).toBe(peerIds.length);
      const peerKeys = await redisPeerKeys();
      expect(peerKeys.length).toBeLessThanOrEqual(initialPeerKeys.length + 1);

      await Promise.all(pages.map(page => waitForFrame(page)));

      const mediaState = await Promise.all(pages.map(page => page.evaluate(() =>
        [...document.querySelectorAll("video")].map(video => ({
          readyState: video.readyState,
          width: video.videoWidth,
          height: video.videoHeight
        }))
      )));
      for (const videos of mediaState) {
        expect(videos.filter(video => video.readyState >= 2 && video.width > 0 && video.height > 0).length)
          .toBeGreaterThanOrEqual(1);
      }

      expect(Date.now() - startedAt).toBeLessThan(Number(process.env.SFU_FAILOVER_SLA_MS ?? 15000));

      if (cycle < 3) {
        execFileSync("docker", ["compose", "-f", composeFile, "start", stoppedService], { stdio: "inherit" });
        for (let attempt = 0; attempt < 30; attempt++) {
          try {
            if ((await fetch(endpoint + "/health")).ok) break;
          } catch {}
          await new Promise(resolve => setTimeout(resolve, 500));
          if (attempt === 29) throw new Error(stoppedService + " did not recover");
        }
      }

      endpoint = nextEndpoint;
    }
  } finally {
    try {
      execFileSync("docker", ["compose", "-f", composeFile, "start", "sfu-primary", "sfu-secondary"], { stdio: "inherit" });
    } catch {}
    await Promise.all(pages.map(page => page.close()));
  }
});
