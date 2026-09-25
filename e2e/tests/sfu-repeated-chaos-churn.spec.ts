import { createHmac } from "node:crypto";
import { execFileSync } from "node:child_process";
import fs from "node:fs";
import path from "node:path";
import { test, expect, type Page } from "@playwright/test";

const primary = process.env.SFU_PRIMARY_URL ?? "http://127.0.0.1:4100";
const secondary = process.env.SFU_SECONDARY_URL ?? "http://127.0.0.1:4200";
const compose = process.env.SFU_FAILOVER_COMPOSE ?? "e2e/docker-compose.sfu-failover.yml";
const redisUrl = process.env.REDIS_URL ?? "redis://127.0.0.1:6379";
const secret = process.env.ROOM_ACCESS_SECRET ?? "integration-secret";
const ttlMs = Number(process.env.SFU_ROOM_OWNER_TTL_MS ?? 3000);
const cycles = Math.min(5, Math.max(2, Number(process.env.SFU_CHAOS_CYCLES ?? 3)));
const members = 3;

function accessToken(roomId: string, userId: string) {
  const payload = Buffer.from(JSON.stringify({
    roomId, userId, role: userId.endsWith("-0") ? "host" : "participant",
    exp: Math.floor(Date.now() / 1000) + 900
  })).toString("base64url");
  return payload + "." + createHmac("sha256", secret).update(payload).digest("base64url");
}

async function redisState(roomId: string) {
  const { createClient } = await import("redis");
  const client = createClient({ url: redisUrl });
  await client.connect();
  const ownerRaw = await client.get("yazykon:sfu:owner:" + roomId);
  const state = await client.exists("yazykon:sfu:state:" + roomId);
  const tracks = await client.exists("yazykon:sfu:tracks:" + roomId);
  const peers = await client.keys("yazykon:sfu:peer:" + roomId + ":*");
  await client.quit();
  return { owner: ownerRaw ? JSON.parse(ownerRaw) : null, state, tracks, peerCount: peers.length, peerKeys: peers.sort() };
}

async function waitHealth(endpoint: string, expected = true) {
  for (let i = 0; i < 40; i++) {
    try {
      const response = await fetch(endpoint + "/health");
      if (response.ok) {
        const body = await response.json() as { cluster: boolean; nodeId: string };
        if (body.cluster === expected) return body;
      }
    } catch {}
    await new Promise(r => setTimeout(r, 250));
  }
  throw new Error("health convergence timeout");
}

async function connect(page: Page, endpoint: string, roomId: string, userId: string, peerId = "", replaceTrack = false) {
  return page.evaluate(async ({ endpoint, roomId, userId, accessToken, peerId, replaceTrack }) => {
    const state = (globalThis as any).__repeatedChaos ??= { stream: undefined, ws: undefined, pc: undefined };
    state.ws?.close();
    state.pc?.close();
    state.stream ??= await navigator.mediaDevices.getUserMedia({ video: true, audio: false });
    if (replaceTrack) {
      const old = state.stream.getVideoTracks()[0];
      const fresh = await navigator.mediaDevices.getUserMedia({ video: true, audio: false });
      const freshTrack = fresh.getVideoTracks()[0];
      state.stream.removeTrack(old);
      old.stop();
      state.stream.addTrack(freshTrack);
    }

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
    const waiters = new Map<string, ((message: any) => void)[]>();
    const wait = (type: string) => new Promise<any>((resolve, reject) => {
      const timer = setTimeout(() => reject(new Error("timeout " + type)), 30000);
      const index = queue.findIndex(message => message.type === type);
      if (index >= 0) {
        clearTimeout(timer);
        resolve(queue.splice(index, 1)[0]);
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
      if (list?.length) list.shift()!(message); else queue.push(message);
    };
    pc.onicecandidate = event => {
      if (event.candidate && ws.readyState === WebSocket.OPEN)
        ws.send(JSON.stringify({ type: "ice", roomId, data: event.candidate.toJSON() }));
    };
    ws.send(JSON.stringify({
      type: "join", roomId, ...(peerId ? { peerId } : {}),
      data: { accessToken, reconnect: Boolean(peerId) }
    }));
    const joined = await wait("joined");
    const offer = await pc.createOffer();
    await pc.setLocalDescription(offer);
    ws.send(JSON.stringify({ type: "offer", roomId, data: pc.localDescription }));
    const answer = await wait("answer");
    if (answer.data) await pc.setRemoteDescription(answer.data);
    state.ws = ws;
    state.pc = pc;
    return {
      peerId: joined.peerId,
      trackIds: state.stream.getTracks().map((track: MediaStreamTrack) => track.id),
      trackStates: state.stream.getTracks().map((track: MediaStreamTrack) => track.readyState),
      remoteTracks, frames
    };
  }, { endpoint, roomId, userId, accessToken: accessToken(roomId, userId), peerId, replaceTrack });
}

test("repeated SFU failover churn preserves peer identity and cleans replaced tracks", async ({ browser }) => {
  test.setTimeout(Math.max(120000, (cycles * 30000) + 60000));
  test.skip(!process.env.SFU_FAILOVER_LIVE, "Set SFU_FAILOVER_LIVE=1 for the live Docker run");
  await Promise.all([waitHealth(primary), waitHealth(secondary)]);

  const roomId = "REPEATED-CHAOS-" + Date.now();
  const pages: Page[] = [];
  const participants = [];

  try {
    for (let i = 0; i < members; i++) {
      const page = await browser.newPage({ permissions: ["camera", "microphone"] });
      await page.goto(primary + "/health");
      pages.push(page);
      participants.push({
        page, userId: "repeat-user-" + i, peerId: "", trackIds: [] as string[], trackHistory: [] as string[][]
      });
    }

    const initial = await Promise.all(participants.map(p => connect(p.page, primary, roomId, p.userId)));
    initial.forEach((r, i) => {
      participants[i].peerId = r.peerId;
      participants[i].trackIds = r.trackIds;
      participants[i].trackHistory.push(r.trackIds);
    });
    expect(new Set(participants.map(p => p.peerId)).size).toBe(members);
    expect((await redisState(roomId)).peerCount).toBe(members);

    const cyclesReport: any[] = [];

    for (let cycle = 0; cycle < cycles; cycle++) {
      const from = cycle % 2 === 0 ? primary : secondary;
      const to = cycle % 2 === 0 ? secondary : primary;

      execFileSync("docker", ["compose", "-f", compose, "stop", cycle % 2 === 0 ? "sfu-primary" : "sfu-secondary"], { stdio: "inherit" });
      await new Promise(r => setTimeout(r, ttlMs + 700));

      const takeover = await redisState(roomId);
      expect(takeover.owner?.nodeId).toBe(cycle % 2 === 0 ? "integration-secondary" : "integration-primary");

      await waitHealth(to, true);
      const recovered = await Promise.all(participants.map((p, index) =>
        connect(p.page, to, roomId, p.userId, p.peerId, cycle > 0 && index === cycle % members)
      ));

      recovered.forEach((r, i) => {
        expect(r.peerId).toBe(participants[i].peerId);
        expect(r.trackStates.every((s: string) => s === "live")).toBeTruthy();
        participants[i].trackHistory.push(r.trackIds);
        if (i === cycle % members && cycle > 0) {
          expect(r.trackIds.join()).not.toBe(participants[i].trackHistory[0].join());
        } else {
          expect(r.trackIds).toEqual(participants[i].trackIds);
        }
      });

      const state = await redisState(roomId);
      expect(state.peerCount).toBe(members);
      expect(new Set(state.peerKeys).size).toBe(members);
      cyclesReport.push({ cycle, from, to, takeover, state });

      execFileSync("docker", ["compose", "-f", compose, "start", cycle % 2 === 0 ? "sfu-primary" : "sfu-secondary"], { stdio: "inherit" });
      await waitHealth(from, true);
    }

    await Promise.all(pages.map(p => p.close()));
    pages.length = 0;
    await new Promise(r => setTimeout(r, ttlMs + 1000));

    const cleaned = await redisState(roomId);
    expect(cleaned.peerCount).toBe(0);

    const artifactDir = process.env.SFU_ARTIFACT_DIR ?? "artifacts";
    fs.mkdirSync(artifactDir, { recursive: true });
    fs.writeFileSync(
      path.join(artifactDir, "sfu-repeated-chaos-churn-report.json"),
      JSON.stringify({
        generatedAt: new Date().toISOString(),
        roomId,
        cycles,
        members,
        cyclesReport,
        cleaned,
        peerIdentityStable: participants.every(p => p.peerId.length > 0),
        trackReplacementCycles: participants.flatMap(p => p.trackHistory).length - participants.length,
        ghostPeerKeys: cleaned.peerCount,
        pass: true
      }, null, 2)
    );
  } finally {
    try { execFileSync("docker", ["compose", "-f", compose, "start", "redis", "sfu-primary", "sfu-secondary"], { stdio: "inherit" }); } catch {}
    await Promise.all(pages.map(p => p.close()));
  }
});
