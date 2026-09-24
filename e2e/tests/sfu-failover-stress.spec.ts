import { createHmac } from "node:crypto";
import { execFileSync } from "node:child_process";
import { test, expect } from "@playwright/test";

const primary = process.env.SFU_PRIMARY_URL ?? "http://127.0.0.1:4100";
const secondary = process.env.SFU_SECONDARY_URL ?? "http://127.0.0.1:4200";
const redisUrl = process.env.REDIS_URL ?? "redis://127.0.0.1:6379";
const roomId = process.env.SFU_STRESS_ROOM ?? "SFU-STRESS";
const secret = process.env.ROOM_ACCESS_SECRET ?? "integration-secret";
const cycles = Math.max(2, Number(process.env.SFU_FAILOVER_CYCLES ?? 3));
const participants = Math.min(8, Math.max(4, Number(process.env.SFU_PARTICIPANTS ?? 4)));
const ttlMs = Number(process.env.SFU_ROOM_OWNER_TTL_MS ?? 3000);
const composeFile = process.env.SFU_FAILOVER_COMPOSE ?? "e2e/docker-compose.sfu-failover.yml";

function token(userId: string) {
  const payload = Buffer.from(JSON.stringify({
    roomId, userId, role: userId === "p1" ? "host" : "participant",
    exp: Math.floor(Date.now() / 1000) + 600
  })).toString("base64url");
  return payload + "." + createHmac("sha256", secret).update(payload).digest("base64url");
}

async function trackState() {
  const { createClient } = await import("redis");
  const client = createClient({ url: redisUrl });
  await client.connect();
  const values = await client.hGetAll(`yazykon:sfu:tracks:${roomId}`);
  await client.quit();
  return Object.values(values).map(v => JSON.parse(v) as { peerId: string; trackId: string; sessionId: string; kind: string });
}

test("multi-cycle SFU failover preserves stable peers and republishes media", async ({ browser }) => {
  test.skip(!process.env.SFU_FAILOVER_LIVE, "Set SFU_FAILOVER_LIVE=1 for the live Docker run");

  const pages = await Promise.all(
    Array.from({ length: participants }, () => browser.newPage({ permissions: ["camera", "microphone"] }))
  );
  await Promise.all(pages.map(page => page.goto(primary + "/health")));

  const connections = new Map<string, { page: import("@playwright/test").Page; peerId: string; sessionId: string }>();

  async function connect(page: import("@playwright/test").Page, userId: string, endpoint: string, peerId = "") {
    return page.evaluate(async ({ endpoint, roomId, accessToken, peerId }) => {
      const stream = await navigator.mediaDevices.getUserMedia({ video: true, audio: false });
      const pc = new RTCPeerConnection();
      let remoteTracks = 0;
      pc.ontrack = () => { remoteTracks++; };
      for (const track of stream.getTracks()) pc.addTrack(track, stream);

      const ws = new WebSocket(endpoint.replace(/^http/, "ws") + "/ws");
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
      await next("answer");
      await new Promise(resolve => setTimeout(resolve, 1200));
      return {
        peerId: joined.peerId,
        remoteTracks,
        localTrackLive: stream.getVideoTracks().every(track => track.readyState === "live")
      };
    }, { endpoint, roomId, accessToken: token(userId), peerId });
  }

  for (let i = 0; i < participants; i++) {
    const userId = `p${i + 1}`;
    const result = await connect(pages[i], userId, primary);
    expect(result.peerId).toBeTruthy();
    expect(result.localTrackLive).toBeTruthy();
    connections.set(userId, { page: pages[i], peerId: result.peerId, sessionId: "" });
  }

  let states = await trackState();
  for (const [userId, item] of connections) {
    const current = states.find(t => t.peerId === item.peerId && t.kind === "video");
    expect(current, userId).toBeTruthy();
    item.sessionId = current!.sessionId;
  }

  for (let cycle = 0; cycle < cycles; cycle++) {
    const fromPrimary = cycle % 2 === 0;
    const failed = fromPrimary ? "sfu-primary" : "sfu-secondary";
    const target = fromPrimary ? secondary : primary;
    const source = fromPrimary ? primary : secondary;

    execFileSync("docker", ["compose", "-f", composeFile, "stop", failed], { stdio: "inherit" });
    await new Promise(resolve => setTimeout(resolve, ttlMs + 2000));

    states = await trackState();
    for (const [userId, item] of connections) {
      const recovered = await connect(item.page, userId, target, item.peerId);
      expect(recovered.peerId, userId).toBe(item.peerId);
      expect(recovered.localTrackLive, userId).toBeTruthy();
    }

    await new Promise(resolve => setTimeout(resolve, 1000));
    states = await trackState();
    for (const [userId, item] of connections) {
      const recovered = states.find(t => t.peerId === item.peerId && t.kind === "video");
      expect(recovered, userId).toBeTruthy();
      expect(recovered!.sessionId, userId).not.toBe(item.sessionId);
      item.sessionId = recovered!.sessionId;
    }

    if (cycle + 1 < cycles) {
      execFileSync("docker", ["compose", "-f", composeFile, "start", failed], { stdio: "inherit" });
      for (let attempt = 0; attempt < 30; attempt++) {
        try {
          const health = await fetch(source + "/health");
          if (health.ok) break;
        } catch {}
        await new Promise(resolve => setTimeout(resolve, 500));
        if (attempt === 29) throw new Error(`${failed} did not recover`);
      }
    }
  }

  states = await trackState();
  const livePeers = new Set(states.filter(t => t.kind === "video").map(t => t.peerId));
  expect(livePeers.size).toBe(participants);
  await Promise.all(pages.map(page => page.close()));
});
