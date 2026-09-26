import { createHmac } from "node:crypto";
import { test, expect } from "@playwright/test";

const primary = process.env.SFU_PRIMARY_URL ?? "http://127.0.0.1:4100";
const secondary = process.env.SFU_SECONDARY_URL ?? "http://127.0.0.1:4200";
const redisUrl = process.env.REDIS_URL ?? "redis://127.0.0.1:6379";
const secret = process.env.ROOM_ACCESS_SECRET ?? "integration-secret";

function token(roomId: string, userId: string) {
  const payload = Buffer.from(JSON.stringify({ roomId, userId, role: "host", exp: Math.floor(Date.now() / 1000) + 600 })).toString("base64url");
  return payload + "." + createHmac("sha256", secret).update(payload).digest("base64url");
}

async function join(endpoint: string, roomId: string, peerId?: string) {
  const ws = new WebSocket(endpoint.replace(/^http/, "ws") + "/ws");
  const result = await new Promise<{ type: string; data?: any }>((resolve, reject) => {
    const timer = setTimeout(() => reject(new Error("join timeout")), 10000);
    ws.onopen = () => {
      ws.send(JSON.stringify({
        type: "join",
        roomId,
        peerId,
        data: { accessToken: token(roomId, peerId ?? "cross-node"), reconnect: Boolean(peerId) },
      }));
    };
    ws.onmessage = event => {
      const m = JSON.parse(event.data);
      if (m.type === "joined" || m.type === "error") {
        clearTimeout(timer);
        resolve(m);
      }
    };
    ws.onerror = () => {
      clearTimeout(timer);
      reject(new Error("websocket failed"));
    };
  });
  return { ws, result };
}

async function ownerNode(redis: any, key: string) {
  const raw = await redis.get(key);
  if (!raw) return null;
  try { return JSON.parse(raw)?.nodeId ?? raw; } catch { return raw; }
}

test("cross-node reconnect preserves peer identity and protects Redis ownership", async () => {
  test.skip(!process.env.SFU_FAILOVER_LIVE, "Set SFU_FAILOVER_LIVE=1 for the live Docker run");
  const { createClient } = await import("redis");
  const redis = createClient({ url: redisUrl });
  await redis.connect();
  const roomId = "CROSS-" + Date.now();
  const peerId = "peer-cross-node";
  let primaryWS: WebSocket | undefined;
  let secondaryWS: WebSocket | undefined;

  try {
    const primaryJoin = await join(primary, roomId, peerId);
    primaryWS = primaryJoin.ws;
    expect(primaryJoin.result.type).toBe("joined");

    const ownerKey = `yazykon:sfu:owner:${roomId}`;
    await expect.poll(() => ownerNode(redis, ownerKey), { timeout: 5000 }).toBe("integration-primary");

    const secondaryJoin = await join(secondary, roomId, peerId);
    secondaryWS = secondaryJoin.ws;

    // A live primary owns the room. Secondary must not overwrite ownership;
    // it should explicitly redirect the client to the current owner.
    expect(secondaryJoin.result.type).toBe("error");
    expect(secondaryJoin.result.data?.code).toBe("SFU_ROOM_OWNER");
    expect(secondaryJoin.result.data?.endpoint).toBeTruthy();

    await expect.poll(() => ownerNode(redis, ownerKey), { timeout: 3000 }).toBe("integration-primary");
  } finally {
    primaryWS?.close();
    secondaryWS?.close();
    await redis.del(ownerKey, `yazykon:sfu:state:${roomId}`, `yazykon:sfu:tracks:${roomId}`);
    await redis.quit();
  }
});
