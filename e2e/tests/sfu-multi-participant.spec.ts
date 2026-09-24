import { createHmac } from "node:crypto";
import { test, expect } from "@playwright/test";

const primary = process.env.SFU_PRIMARY_URL ?? "http://127.0.0.1:4100";
const roomId = process.env.SFU_MULTI_ROOM ?? "MULTI-PARTICIPANT";
const secret = process.env.ROOM_ACCESS_SECRET ?? "integration-secret";

function token(userId: string) {
  const payload = Buffer.from(JSON.stringify({
    roomId, userId, role: userId === "p1" ? "host" : "participant",
    exp: Math.floor(Date.now() / 1000) + 300
  })).toString("base64url");
  return payload + "." + createHmac("sha256", secret).update(payload).digest("base64url");
}

async function connect(page: import("@playwright/test").Page, userId: string, peerId = "", expectRemoteMedia = false) {
  return page.evaluate(async ({ endpoint, roomId, accessToken, peerId }) => {
    const stream = await navigator.mediaDevices.getUserMedia({ video: true, audio: false });
    const pc = new RTCPeerConnection();
    let remoteTracks = 0;
    let remoteMediaReady: Promise<void> | null = null;
    let resolveRemoteMedia: (() => void) | null = null;
    remoteMediaReady = new Promise(resolve => { resolveRemoteMedia = resolve; });
    pc.ontrack = event => {
      remoteTracks++;
      const stream = event.streams[0];
      if (!stream) return;
      const video = document.createElement("video");
      video.autoplay = true;
      video.muted = true;
      video.playsInline = true;
      video.srcObject = stream;
      document.body.appendChild(video);
      const finish = () => {
        video.requestVideoFrameCallback?.(() => resolveRemoteMedia?.());
      };
      video.onloadeddata = finish;
      void video.play().then(finish).catch(() => undefined);
    };
    for (const track of stream.getTracks()) pc.addTrack(track, stream);

    const ws = new WebSocket(endpoint.replace(/^http/, "ws") + "/ws");
    const queue: any[] = [];
    let waiter: ((value: any) => void) | undefined;
    pc.onicecandidate = event => {
      if (event.candidate && ws.readyState === WebSocket.OPEN) {
        ws.send(JSON.stringify({ type: "ice", roomId, data: event.candidate.toJSON() }));
      }
    };
    ws.onmessage = event => {
      const message = JSON.parse(event.data);
      if (message.type === "ice" && message.data) {
        void pc.addIceCandidate(message.data);
      }
      if (waiter) { const resolve = waiter; waiter = undefined; resolve(message); }
      else queue.push(message);
    };
    const next = (type: string) => new Promise<any>((resolve, reject) => {
      const timer = setTimeout(() => reject(new Error("timeout: " + type)), 15000);
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
      ws.onerror = () => reject(new Error("websocket failed"));
    });
    ws.send(JSON.stringify({
      type: "join", roomId, peerId: peerId || undefined,
      data: { accessToken, reconnect: Boolean(peerId) }
    }));
    const joined = await next("joined");

    const offer = await pc.createOffer();
    await pc.setLocalDescription(offer);
    ws.send(JSON.stringify({ type: "offer", roomId, data: pc.localDescription }));
    const answer = await next("answer");
    await pc.setRemoteDescription(answer.data);
    if (expectRemoteMedia && remoteMediaReady) {
      await Promise.race([
        remoteMediaReady,
        new Promise((_, reject) => setTimeout(() => reject(new Error("remote media did not produce a video frame")), 10000))
      ]);
    }
    return {
      peerId: joined.peerId,
      peers: joined.data?.peers ?? [],
      remoteTracks,
      localTrackLive: stream.getVideoTracks().every(track => track.readyState === "live")
    };
  }, { endpoint: primary, roomId, accessToken: token(userId), peerId, expectRemoteMedia });
}

test("three participants receive real remote WebRTC media", async ({ browser }) => {
  test.skip(!process.env.SFU_FAILOVER_LIVE, "Set SFU_FAILOVER_LIVE=1 for the live Docker run");

  const pages = await Promise.all([
    browser.newPage({ permissions: ["camera", "microphone"] }),
    browser.newPage({ permissions: ["camera", "microphone"] }),
    browser.newPage({ permissions: ["camera", "microphone"] })
  ]);
  await Promise.all(pages.map(page => page.goto(primary + "/health")));

  const first = await connect(pages[0], "p1", "", false);
  expect(first.peerId).toBeTruthy();
  expect(first.localTrackLive).toBeTruthy();

  const second = await connect(pages[1], "p2", "", true);
  expect(second.peerId).toBeTruthy();
  expect(second.peerId).not.toBe(first.peerId);
  expect(second.peers).toContain(first.peerId);
  expect(second.remoteTracks).toBeGreaterThan(0);

  const third = await connect(pages[2], "p3", "", true);
  expect(third.peerId).toBeTruthy();
  expect(third.peerId).not.toBe(first.peerId);
  expect(third.peerId).not.toBe(second.peerId);
  expect(third.peers).toEqual(expect.arrayContaining([first.peerId, second.peerId]));
  expect(third.remoteTracks).toBeGreaterThanOrEqual(2);

  await Promise.all(pages.map(page => page.close()));
});
