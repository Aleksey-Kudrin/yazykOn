import { createHmac } from "node:crypto";
import fs from "node:fs";
import path from "node:path";
import { test, expect } from "@playwright/test";

const endpoint = process.env.SFU_PRIMARY_URL ?? "http://127.0.0.1:4100";
const redisUrl = process.env.REDIS_URL ?? "redis://127.0.0.1:6379";
const secret = process.env.ROOM_ACCESS_SECRET ?? "integration-secret";
const artifactDir = process.env.SFU_ARTIFACT_DIR ?? "artifacts";

function token(roomId: string, userId: string) {
  const payload = Buffer.from(JSON.stringify({
    roomId, userId, role: "host", exp: Math.floor(Date.now() / 1000) + 600
  })).toString("base64url");
  return payload + "." + createHmac("sha256", secret).update(payload).digest("base64url");
}

test("empty SFU room removes Redis ownership and state", async () => {
  test.skip(!process.env.SFU_FAILOVER_LIVE, "Set SFU_FAILOVER_LIVE=1 for the live Docker run");

  const { createClient } = await import("redis");
  const client = createClient({ url: redisUrl });
  await client.connect();

  const roomId = "CLEANUP-" + Date.now();
  const peerIdPromise = new Promise<string>((resolve, reject) => {
    const ws = new WebSocket(endpoint.replace(/^http/, "ws") + "/ws");
    const timer = setTimeout(() => reject(new Error("joined timeout")), 10000);
    ws.onopen = () => ws.send(JSON.stringify({
      type: "join", roomId,
      data: { accessToken: token(roomId, "cleanup-host"), reconnect: false }
    }));
    ws.onmessage = event => {
      const message = JSON.parse(event.data);
      if (message.type !== "joined") return;
      clearTimeout(timer);
      resolve(message.peerId);
      setTimeout(() => ws.close(), 100);
    };
    ws.onerror = () => {
      clearTimeout(timer);
      reject(new Error("websocket failed"));
    };
  });

  const peerId = await peerIdPromise;
  expect(peerId).toBeTruthy();
  const started = Date.now();

  const ownerKey = `yazykon:sfu:owner:${roomId}`;
  const stateKey = `yazykon:sfu:state:${roomId}`;
  const tracksKey = `yazykon:sfu:tracks:${roomId}`;
  const peerPattern = `yazykon:sfu:peer:${roomId}:*`;

  try {
    expect(await client.exists(ownerKey)).toBe(1);
    expect(await client.exists(stateKey)).toBe(1);

    for (let i = 0; i < 30; i++) {
      const [owner, state, tracks, peers] = await Promise.all([
        client.exists(ownerKey),
        client.exists(stateKey),
        client.exists(tracksKey),
        client.keys(peerPattern)
      ]);
      if (owner === 0 && state === 0 && tracks === 0 && peers.length === 0) {
        fs.mkdirSync(artifactDir, { recursive: true });
        fs.writeFileSync(path.join(artifactDir, "sfu-redis-cleanup-report.json"), JSON.stringify({ roomId, peerId, cleanupMs: Date.now() - started, pass: true }, null, 2));
        return;
      }
      await new Promise(resolve => setTimeout(resolve, 250));
    }

    const [owner, state, tracks, peers] = await Promise.all([
      client.exists(ownerKey),
      client.exists(stateKey),
      client.exists(tracksKey),
      client.keys(peerPattern)
    ]);
    expect(owner).toBe(0);
    expect(state).toBe(0);
    expect(tracks).toBe(0);
    expect(peers).toHaveLength(0);
    fs.mkdirSync(artifactDir, { recursive: true });
    fs.writeFileSync(path.join(artifactDir, "sfu-redis-cleanup-report.json"), JSON.stringify({ roomId, peerId, cleanupMs: Date.now() - started, pass: true }, null, 2));
  } finally {
    await client.del(ownerKey, stateKey, tracksKey);
    const peers = await client.keys(peerPattern);
    if (peers.length) await client.del(peers);
    await client.quit();
  }
});
