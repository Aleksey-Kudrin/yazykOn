import { test, expect } from "@playwright/test";
import { createHmac } from "node:crypto";

const primary = process.env.SFU_PRIMARY_URL ?? "http://127.0.0.1:4100";
const secondary = process.env.SFU_SECONDARY_URL ?? "http://127.0.0.1:4200";
const secret = process.env.ROOM_ACCESS_SECRET ?? "integration-secret";
const rooms = Number(process.env.SFU_LOAD_ROOMS ?? 3);
const participants = Number(process.env.SFU_LOAD_PARTICIPANTS ?? 8);
const cycles = Number(process.env.SFU_LOAD_CYCLES ?? 3);
const sampleMs = Number(process.env.SFU_METRICS_SAMPLE_MS ?? 1000);

function accessToken(roomId: string, userId: string) {
  const payload = Buffer.from(JSON.stringify({
    roomId,
    userId,
    role: userId.endsWith("-1") ? "host" : "member",
    exp: Math.floor(Date.now() / 1000) + 900
  })).toString("base64url");
  return payload + "." + createHmac("sha256", secret).update(payload).digest("base64url");
}

async function join(endpoint: string, roomId: string, userId: string) {
  const ws = new WebSocket(endpoint.replace(/^http/, "ws") + "/ws");
  const joined = new Promise<any>((resolve, reject) => {
    const timer = setTimeout(() => reject(new Error(`join timeout ${roomId}/${userId}`)), 10000);
    ws.onopen = () => ws.send(JSON.stringify({
      type: "join", roomId, peerId: userId,
      data: { accessToken: accessToken(roomId, userId), reconnect: false }
    }));
    ws.onmessage = event => {
      const message = JSON.parse(event.data);
      if (message.type === "joined") {
        clearTimeout(timer);
        resolve(message);
      }
    };
    ws.onerror = () => {
      clearTimeout(timer);
      reject(new Error(`websocket failed ${roomId}/${userId}`));
    };
  });
  await joined;
  return ws;
}

async function metric(endpoint: string) {
  const response = await fetch(endpoint + "/metrics");
  return response.ok ? await response.text() : "";
}

test("multi-room load survives repeated SFU ownership failover", async () => {
  test.skip(!process.env.SFU_FAILOVER_LIVE, "Set SFU_FAILOVER_LIVE=1 for the live Docker failover run");

  const sockets: WebSocket[] = [];
  const samples: Array<{ t: number; primaryBytes: number; secondaryBytes: number }> = [];
  const start = Date.now();
  let endpoint = primary;

  const sampler = setInterval(async () => {
    const [p, s] = await Promise.all([metric(primary), metric(secondary)]);
    samples.push({ t: Date.now() - start, primaryBytes: p.length, secondaryBytes: s.length });
  }, sampleMs);

  try {
    for (let cycle = 0; cycle < cycles; cycle++) {
      const current = endpoint;
      for (let room = 1; room <= rooms; room++) {
        const roomId = `LOAD-${cycle}-${room}`;
        for (let participant = 1; participant <= participants; participant++) {
          sockets.push(await join(current, roomId, `p${participant}-${room}`));
        }
      }

      expect(sockets.length).toBe((cycle + 1) * rooms * participants);
      const health = await fetch(current + "/health");
      expect(health.ok).toBeTruthy();

      endpoint = endpoint === primary ? secondary : primary;
      const failoverHealth = await fetch(endpoint + "/health");
      expect(failoverHealth.ok).toBeTruthy();

      await new Promise(resolve => setTimeout(resolve, sampleMs * 2));
    }

    expect(samples.length).toBeGreaterThan(1);
    expect(samples.some(sample => sample.primaryBytes > 0 || sample.secondaryBytes > 0)).toBeTruthy();
  } finally {
    clearInterval(sampler);
    for (const ws of sockets) ws.close();
  }
});
