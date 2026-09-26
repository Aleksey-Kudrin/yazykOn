import { test, expect } from "@playwright/test";
import WebSocket from "ws";

const redisUrl = process.env.REDIS_URL ?? "redis://127.0.0.1:6379";
const primary = process.env.SFU_PRIMARY_URL ?? "http://127.0.0.1:4100";
const secondary = process.env.SFU_SECONDARY_URL ?? "http://127.0.0.1:4200";

function token(roomId: string, peerId: string) {
  return require("node:crypto").createHmac("sha256", process.env.ROOM_ACCESS_SECRET ?? "integration-secret").update(`${roomId}:${peerId}:participant`).digest("hex");
}

async function join(base: string, roomId: string, peerId: string) {
  const ws = new WebSocket(base.replace(/^http/, "ws") + "/ws");
  const result = await new Promise<any>((resolve, reject) => {
    const timer = setTimeout(() => reject(new Error("join timeout")), 5000);
    ws.on("open", () => ws.send(JSON.stringify({ type: "join", peerId, data: { accessToken: token(roomId, peerId) } })));
    ws.on("message", data => { const msg = JSON.parse(data.toString()); if (msg.type === "joined" || msg.type === "error") { clearTimeout(timer); resolve(msg); } });
    ws.on("error", reject);
  });
  return { ws, result };
}

async function ownerNode(redis: any, ownerKey: string) {
  const raw = await redis.get(ownerKey);
  return raw ? JSON.parse(raw).nodeId : null;
}

test("cross-node reconnect preserves peer identity and protects Redis ownership", async () => {
  test.skip(!process.env.SFU_FAILOVER_LIVE, "Set SFU_FAILOVER_LIVE=1 for the live Docker run");
  const { createClient } = await import("redis");
  const redis = createClient({ url: redisUrl });
  await redis.connect();
  const roomId = "CROSS-" + Date.now();
  const peerId = "peer-cross-node";
  const ownerKey = `yazykon:sfu:owner:${roomId}`;
  let primaryWS: WebSocket | undefined;
  let secondaryWS: WebSocket | undefined;

  try {
    const primaryJoin = await join(primary, roomId, peerId);
    primaryWS = primaryJoin.ws;
    expect(primaryJoin.result.type).toBe("joined");
    await expect.poll(() => ownerNode(redis, ownerKey), { timeout: 5000 }).toBe("integration-primary");
    const secondaryJoin = await join(secondary, roomId, peerId);
    secondaryWS = secondaryJoin.ws;
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
