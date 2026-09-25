import { createHmac } from "node:crypto";
import { test, expect } from "@playwright/test";

const secondary = process.env.SFU_SECONDARY_URL ?? "http://127.0.0.1:4200";
const redisUrl = process.env.REDIS_URL ?? "redis://127.0.0.1:6379";
const secret = process.env.ROOM_ACCESS_SECRET ?? "integration-secret";

function token(roomId: string, userId: string) {
  const payload = Buffer.from(JSON.stringify({ roomId, userId, role: "host", exp: Math.floor(Date.now() / 1000) + 600 })).toString("base64url");
  return payload + "." + createHmac("sha256", secret).update(payload).digest("base64url");
}

test("secondary exposes recovered expected track metadata after takeover", async () => {
  test.skip(!process.env.SFU_FAILOVER_LIVE, "Set SFU_FAILOVER_LIVE=1 for the live Docker failover run");
  const { createClient } = await import("redis");
  const redis = createClient({ url: redisUrl });
  await redis.connect();
  const roomId = "TRACK-RECOVERY-" + Date.now();
  const tracksKey = `yazykon:sfu:tracks:${roomId}`;
  const stateKey = `yazykon:sfu:state:${roomId}`;
  const track = { peerId: "peer-publisher", trackId: "video-recovered", sessionId: "session-recovered", kind: "video", updatedAt: Math.floor(Date.now() / 1000) };
  const ws = new WebSocket(secondary.replace(/^http/, "ws") + "/ws");
  try {
    await redis.hSet(tracksKey, "peer-publisher:video-recovered", JSON.stringify(track));
    await redis.expire(tracksKey, 90);
    await redis.hSet(stateKey, "state", JSON.stringify({ roomId, hostId: "peer-publisher", locked: false, lobby: false, updatedAt: Math.floor(Date.now() / 1000) }));
    await redis.expire(stateKey, 90);

    const joined = new Promise<Record<string, unknown>>((resolve, reject) => {
      const timer = setTimeout(() => reject(new Error("join timeout")), 10000);
      ws.onopen = () => ws.send(JSON.stringify({ type: "join", roomId, peerId: "peer-recovered", data: { accessToken: token(roomId, "recovered"), reconnect: true } }));
      ws.onmessage = event => { const message = JSON.parse(event.data); if (message.type === "joined") { clearTimeout(timer); resolve(message); } };
      ws.onerror = () => { clearTimeout(timer); reject(new Error("websocket failed")); };
    });
    const message = await joined;
    const data = JSON.parse(String(message.data));
    expect(data.expectedTracks).toEqual([track]);
    expect(data.hostId).toBe("peer-publisher");
  } finally {
    ws.close();
    await redis.del(tracksKey, stateKey);
    await redis.quit();
  }
});
