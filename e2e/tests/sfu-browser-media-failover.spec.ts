import { createHmac } from "node:crypto";
import { execFileSync } from "node:child_process";
import { test, expect } from "@playwright/test";

const primary = process.env.SFU_PRIMARY_URL ?? "http://127.0.0.1:4100";
const secondary = process.env.SFU_SECONDARY_URL ?? "http://127.0.0.1:4200";
const redisUrl = process.env.REDIS_URL ?? "redis://127.0.0.1:6379";
const roomId = process.env.SFU_TEST_ROOM ?? "BROWSER-FAILOVER";
const secret = process.env.ROOM_ACCESS_SECRET ?? "integration-secret";
const ttlMs = Number(process.env.SFU_ROOM_OWNER_TTL_MS ?? 3500);
const composeFile = process.env.SFU_FAILOVER_COMPOSE ?? "e2e/docker-compose.sfu-failover.yml";

function token() {
  const payload = Buffer.from(JSON.stringify({
    roomId,
    userId: "browser-failover-user",
    role: "host",
    exp: Math.floor(Date.now() / 1000) + 300
  })).toString("base64url");
  const sig = createHmac("sha256", secret).update(payload).digest("base64url");
  return payload + "." + sig;
}

async function redisStates() {
  const { createClient } = await import("redis");
  const client = createClient({ url: redisUrl });
  await client.connect();
  const values = await client.hGetAll(`yazykon:sfu:tracks:${roomId}`);
  await client.quit();
  return Object.values(values).map(value => JSON.parse(value) as {
    peerId: string; trackId: string; sessionId: string; kind: string;
  });
}

test("browser WebRTC media survives SFU A loss and republishes on SFU B", async ({ browser }) => {
  test.skip(!process.env.SFU_FAILOVER_LIVE, "Set SFU_FAILOVER_LIVE=1 for the live Docker failover run");

  const page = await browser.newPage({
    permissions: ["camera", "microphone"],
    javaScriptEnabled: true
  });
  await page.goto("about:blank");

  const authToken = token();
  const result = await page.evaluate(async ({ primary, secondary, roomId, authToken, ttlMs }) => {
    const state = {
      peerId: "",
      primaryJoined: false,
      secondaryJoined: false,
      firstTrackId: "",
      secondTrackId: "",
      localTrackReady: false
    };

    const wait = (ms: number) => new Promise(resolve => setTimeout(resolve, ms));

    async function connect(endpoint: string, reconnectPeerId = "") {
      const stream = await navigator.mediaDevices.getUserMedia({ video: true, audio: false });
      state.localTrackReady = stream.getVideoTracks().some(track => track.readyState === "live");
      const pc = new RTCPeerConnection();
      for (const track of stream.getTracks()) pc.addTrack(track, stream);

      const ws = new WebSocket(endpoint.replace(/^http/, "ws") + "/ws");
      const messages: any[] = [];
      const messageWaiters: Array<(message: any) => void> = [];
      ws.onmessage = event => {
        const message = JSON.parse(event.data);
        messages.push(message);
        const waiter = messageWaiters.shift();
        if (waiter) waiter(message);
      };
      await new Promise<void>((resolve, reject) => {
        ws.onopen = () => resolve();
        ws.onerror = () => reject(new Error("WebSocket failed: " + endpoint));
      });

      const nextMessage = (type: string) => new Promise<any>((resolve, reject) => {
        const timer = setTimeout(() => reject(new Error("Timed out waiting for " + type)), 10000);
        const take = (message: any) => {
          if (message.type === type) {
            clearTimeout(timer);
            resolve(message);
          } else {
            messageWaiters.push(take);
          }
        };
        const queued = messages.find(message => message.type === type);
        if (queued) {
          clearTimeout(timer);
          resolve(queued);
        } else {
          messageWaiters.push(take);
        }
      });

      const candidates: RTCIceCandidateInit[] = [];
      pc.onicecandidate = event => {
        if (!event.candidate) return;
        const candidate = event.candidate.toJSON();
        if (ws.readyState === WebSocket.OPEN) {
          ws.send(JSON.stringify({ type: "ice", roomId, peerId: "", data: candidate }));
        } else {
          candidates.push(candidate);
        }
      };

      ws.send(JSON.stringify({
        type: "join",
        roomId,
        peerId: reconnectPeerId || undefined,
        data: { accessToken: authToken, reconnect: Boolean(reconnectPeerId) }
      }));

      const joined = await nextMessage("joined");
      state.peerId = joined.peerId;
      if (!reconnectPeerId) state.primaryJoined = true; else state.secondaryJoined = true;

      const offer = await pc.createOffer();
      await pc.setLocalDescription(offer);
      ws.send(JSON.stringify({ type: "offer", roomId, data: pc.localDescription }));

      let answer = await nextMessage("answer");
      await pc.setRemoteDescription(answer.data);

      ws.onmessage = event => {
        const message = JSON.parse(event.data);
        if (message.type === "ice" && message.data) void pc.addIceCandidate(message.data);
      };

      for (const candidate of candidates) {
        ws.send(JSON.stringify({ type: "ice", roomId, data: candidate }));
      }

      await wait(1000);
      return { ws, pc, stream, joined };
    }

    const first = await connect(primary);
    state.firstTrackId = first.stream.getVideoTracks()[0]?.id ?? "";
    await wait(Math.max(1000, ttlMs / 2));

    // The test runner stops SFU A outside the browser. Keep the media object
    // alive so the same browser track can be republished to SFU B.
    return {
      ...state,
      peerId: state.peerId,
      firstTrackId: state.firstTrackId,
      primaryConnection: first.pc.connectionState,
      primaryWs: first.ws.readyState
    };
  }, { primary, secondary, roomId, authToken, ttlMs });

  expect(result.primaryJoined).toBeTruthy();
  expect(result.localTrackReady).toBeTruthy();
  expect(result.firstTrackId).toBeTruthy();

  const before = await redisStates();
  expect(before.some(track => track.peerId === result.peerId && track.kind === "video")).toBeTruthy();
  const oldSession = before.find(track => track.peerId === result.peerId && track.kind === "video")!.sessionId;

  execFileSync("docker", ["compose", "-f", composeFile, "stop", "sfu-primary"], { stdio: "inherit" });
  await page.waitForTimeout(ttlMs + 1500);

  const recovery = await page.evaluate(async ({ secondary, roomId, authToken, peerId }) => {
    const stream = await navigator.mediaDevices.getUserMedia({ video: true, audio: false });
    const track = stream.getVideoTracks()[0];
    const pc = new RTCPeerConnection();
    pc.addTrack(track, stream);
    const ws = new WebSocket(secondary.replace(/^http/, "ws") + "/ws");

    const queue: any[] = [];
    let waiter: ((value: any) => void) | undefined;
    ws.onmessage = event => {
      const message = JSON.parse(event.data);
      if (waiter) { const resolve = waiter; waiter = undefined; resolve(message); }
      else queue.push(message);
    };
    const next = (type: string) => new Promise<any>((resolve, reject) => {
      const timer = setTimeout(() => reject(new Error("timeout: " + type)), 15000);
      const take = (message: any) => {
        if (message.type === type) { clearTimeout(timer); resolve(message); return; }
        waiter = take;
      };
      const queued = queue.findIndex(message => message.type === type);
      if (queued >= 0) {
        const message = queue.splice(queued, 1)[0];
        clearTimeout(timer);
        resolve(message);
      } else waiter = take;
    });

    await new Promise<void>((resolve, reject) => {
      ws.onopen = () => resolve();
      ws.onerror = () => reject(new Error("secondary websocket failed"));
    });
    ws.send(JSON.stringify({
      type: "join",
      roomId,
      peerId,
      data: { accessToken: authToken, reconnect: true }
    }));
    const joined = await next("joined");

    const offer = await pc.createOffer();
    await pc.setLocalDescription(offer);
    ws.send(JSON.stringify({ type: "offer", roomId, data: pc.localDescription }));
    await next("answer");

    return {
      peerId: joined.peerId,
      expectedTracks: joined.data?.expectedTracks ?? [],
      trackReady: track.readyState === "live"
    };
  }, { secondary, roomId, authToken, peerId: result.peerId });

  expect(recovery.peerId).toBe(result.peerId);
  expect(recovery.trackReady).toBeTruthy();

  await page.waitForTimeout(1000);
  const after = await redisStates();
  const republished = after.find(track => track.peerId === result.peerId && track.kind === "video");
  expect(republished).toBeTruthy();
  expect(republished!.sessionId).not.toBe(oldSession);
  expect(republished!.trackId).toBeTruthy();

  await page.close();
});
