import { createHmac } from "node:crypto";
import { execFileSync } from "node:child_process";
import { test, expect } from "@playwright/test";

const primary = process.env.SFU_PRIMARY_URL ?? "http://127.0.0.1:4100";
const secondary = process.env.SFU_SECONDARY_URL ?? "http://127.0.0.1:4200";
const redisUrl = process.env.REDIS_URL ?? "redis://127.0.0.1:6379";
const secret = process.env.ROOM_ACCESS_SECRET ?? "integration-secret";
const compose = ["compose", "-p", "yazykon-sfu-it", "-f", "e2e/docker-compose.sfu-failover.yml"];

function token(roomId: string, userId: string) {
  const payload = Buffer.from(JSON.stringify({ roomId, userId, role: "host", exp: Math.floor(Date.now() / 1000) + 600 })).toString("base64url");
  return payload + "." + createHmac("sha256", secret).update(payload).digest("base64url");
}

async function join(endpoint: string, roomId: string, peerId?: string) {
  const ws = new WebSocket(endpoint.replace(/^http/, "ws") + "/ws");
  const joined = new Promise<Record<string, unknown>>((resolve, reject) => {
    const timer = setTimeout(() => reject(new Error("join timeout")), 10000);
    ws.onopen = () => ws.send(JSON.stringify({ type: "join", roomId, peerId, data: { accessToken: token(roomId, peerId ?? "takeover-host"), reconnect: Boolean(peerId) } }));
    ws.onmessage = event => { const message = JSON.parse(event.data); if (message.type === "joined") { clearTimeout(timer); resolve(message); } };
    ws.onerror = () => { clearTimeout(timer); reject(new Error("websocket failed")); };
  });
  return { ws, joined: await joined };
}

test("primary crash triggers secondary takeover and stable peer reconnect", async () => {
  test.skip(!process.env.SFU_FAILOVER_LIVE, "Set SFU_FAILOVER_LIVE=1 for the live Docker failover run");
  const { createClient } = await import("redis");
  const redis = createClient({ url: redisUrl });
  await redis.connect();
  const roomId = "CRASH-" + Date.now();
  const peerId = "peer-crash-stable";
  const ownerKey = `yazykon:sfu:owner:${roomId}`;
  let first: { ws: WebSocket; joined: Record<string, unknown> } | undefined;
  let second: { ws: WebSocket; joined: Record<string, unknown> } | undefined;
  try {
    first = await join(primary, roomId, peerId);
    expect(first.joined.peerId).toBe(peerId);
    await expect.poll(() => redis.get(ownerKey), { timeout: 5000 }).toBe("integration-primary");

    execFileSync("docker", [...compose, "stop", "sfu-primary"], { stdio: "inherit" });
    await expect.poll(async () => (await redis.get(ownerKey)), { timeout: 7000, intervals: [250] }).toBe("integration-secondary");

    second = await join(secondary, roomId, peerId);
    expect(second.joined.peerId).toBe(peerId);
    expect(second.ws.readyState).toBe(WebSocket.OPEN);
  } finally {
    first?.ws.close();
    second?.ws.close();
    try { execFileSync("docker", [...compose, "start", "sfu-primary"], { stdio: "inherit" }); } catch {}
    await redis.del(ownerKey, `yazykon:sfu:state:${roomId}`, `yazykon:sfu:tracks:${roomId}`);
    await redis.quit();
  }
});
