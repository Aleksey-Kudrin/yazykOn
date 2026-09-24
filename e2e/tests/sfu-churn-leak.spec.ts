import { createHmac } from "node:crypto";
import { test, expect } from "@playwright/test";

const endpoint = process.env.SFU_PRIMARY_URL ?? "http://127.0.0.1:4100";
const redisUrl = process.env.REDIS_URL ?? "redis://127.0.0.1:6379";
const secret = process.env.ROOM_ACCESS_SECRET ?? "integration-secret";
const rounds = Math.max(5, Number(process.env.SFU_CHURN_ROUNDS ?? 12));

function token(roomId: string, userId: string) {
  const payload = Buffer.from(JSON.stringify({
    roomId, userId, role: "host", exp: Math.floor(Date.now() / 1000) + 600
  })).toString("base64url");
  return `${payload}.${createHmac("sha256", secret).update(payload).digest("base64url")}`;
}

function joinAndClose(roomId: string, userId: string): Promise<string> {
  return new Promise((resolve, reject) => {
    const ws = new WebSocket(endpoint.replace(/^http/, "ws") + "/ws");
    const timer = setTimeout(() => reject(new Error(`join timeout: ${roomId}`)), 10000);
    ws.onopen = () => ws.send(JSON.stringify({
      type: "join", roomId,
      data: { accessToken: token(roomId, userId), reconnect: false }
    }));
    ws.onmessage = event => {
      const message = JSON.parse(event.data);
      if (message.type !== "joined") return;
      clearTimeout(timer);
      resolve(message.peerId);
      setTimeout(() => ws.close(), 50);
    };
    ws.onerror = () => {
      clearTimeout(timer);
      reject(new Error(`websocket failed: ${roomId}`));
    };
  });
}

async function roomKeys(client: any, roomId: string) {
  const [owner, state, tracks, peers] = await Promise.all([
    client.exists(`yazykon:sfu:owner:${roomId}`),
    client.exists(`yazykon:sfu:state:${roomId}`),
    client.exists(`yazykon:sfu:tracks:${roomId}`),
    client.keys(`yazykon:sfu:peer:${roomId}:*`)
  ]);
  return { owner, state, tracks, peers };
}

test("SFU repeated room churn leaves no Redis peer/session state", async () => {
  test.skip(!process.env.SFU_FAILOVER_LIVE, "Set SFU_FAILOVER_LIVE=1 for the live Docker run");

  const { createClient } = await import("redis");
  const client = createClient({ url: redisUrl });
  await client.connect();

  const roomIds: string[] = [];
  try {
    for (let i = 0; i < rounds; i++) {
      const roomId = `CHURN-LEAK-${Date.now()}-${i}`;
      roomIds.push(roomId);
      const peerId = await joinAndClose(roomId, `churn-${i}`);
      expect(peerId).toBeTruthy();
    }

    for (const roomId of roomIds) {
      let cleaned = false;
      for (let attempt = 0; attempt < 40; attempt++) {
        const keys = await roomKeys(client, roomId);
        if (keys.owner === 0 && keys.state === 0 && keys.tracks === 0 && keys.peers.length === 0) {
          cleaned = true;
          break;
        }
        await new Promise(resolve => setTimeout(resolve, 250));
      }
      expect(cleaned, `Redis state leaked for ${roomId}`).toBeTruthy();
    }
  } finally {
    for (const roomId of roomIds) {
      const keys = await roomKeys(client, roomId);
      const fixed = [
        `yazykon:sfu:owner:${roomId}`,
        `yazykon:sfu:state:${roomId}`,
        `yazykon:sfu:tracks:${roomId}`
      ];
      if (keys.peers.length || fixed.length) await client.del(...fixed, ...keys.peers);
    }
    await client.quit();
  }
});
