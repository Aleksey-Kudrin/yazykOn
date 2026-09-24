import { createHmac } from "node:crypto";
import { execFileSync } from "node:child_process";
import fs from "node:fs";
import path from "node:path";
import { test, expect } from "@playwright/test";

const primary = process.env.SFU_PRIMARY_URL ?? "http://127.0.0.1:4100";
const secondary = process.env.SFU_SECONDARY_URL ?? "http://127.0.0.1:4200";
const composeFile = process.env.SFU_FAILOVER_COMPOSE ?? "e2e/docker-compose.sfu-failover.yml";
const redisUrl = process.env.REDIS_URL ?? "redis://127.0.0.1:6379";
const rooms = Math.min(6, Math.max(2, Number(process.env.SFU_ROOM_FAILOVER_ROOMS ?? 3)));
const ttlMs = Number(process.env.SFU_ROOM_OWNER_TTL_MS ?? 3000);
const secret = process.env.ROOM_ACCESS_SECRET ?? "integration-secret";
const artifactDir = process.env.SFU_ARTIFACT_DIR ?? "artifacts";

function token(roomId: string, userId: string) {
  const payload = Buffer.from(JSON.stringify({
    roomId, userId, role: "host", exp: Math.floor(Date.now() / 1000) + 600
  })).toString("base64url");
  return payload + "." + createHmac("sha256", secret).update(payload).digest("base64url");
}

async function waitHealth(endpoint: string) {
  for (let i = 0; i < 40; i++) {
    try {
      if ((await fetch(endpoint + "/health")).ok) return;
    } catch {}
    await new Promise(resolve => setTimeout(resolve, 250));
  }
  throw new Error("SFU health timeout: " + endpoint);
}

async function redisOwners(roomIds: string[]) {
  const { createClient } = await import("redis");
  const client = createClient({ url: redisUrl });
  await client.connect();
  const owners = await Promise.all(roomIds.map(async roomId => {
    const value = await client.get("yazykon:sfu:owner:" + roomId);
    return { roomId, value: value ? JSON.parse(value) : null };
  }));
  await client.quit();
  return owners;
}

async function createRoom(page: import("@playwright/test").Page, roomId: string) {
  return page.evaluate(async ({ endpoint, roomId, accessToken }) => {
    const ws = new WebSocket(endpoint.replace(/^http/, "ws") + "/ws");
    const messages: any[] = [];
    let waiter: ((message: any) => void) | undefined;
    ws.onmessage = event => {
      const message = JSON.parse(event.data);
      if (waiter) { const resolve = waiter; waiter = undefined; resolve(message); }
      else messages.push(message);
    };
    const next = (type: string) => new Promise<any>((resolve, reject) => {
      const queued = messages.findIndex(message => message.type === type);
      if (queued >= 0) return resolve(messages.splice(queued, 1)[0]);
      const timer = setTimeout(() => reject(new Error("timeout: " + type)), 10000);
      waiter = message => {
        if (message.type === type) { clearTimeout(timer); resolve(message); }
        else waiter = message;
      };
    });
    await new Promise<void>((resolve, reject) => {
      ws.onopen = () => resolve();
      ws.onerror = () => reject(new Error("websocket failed"));
    });
    ws.send(JSON.stringify({
      type: "join", roomId,
      data: { accessToken, reconnect: false }
    }));
    const joined = await next("joined");
    return { peerId: joined.peerId, roomId, trackIds: [] as string[] };
  }, {
    endpoint: primary, roomId, accessToken: token(roomId, "owner-" + roomId)
  });
}

test("multiple active rooms transfer Redis ownership after SFU loss", async ({ browser }) => {
  test.skip(!process.env.SFU_FAILOVER_LIVE, "Set SFU_FAILOVER_LIVE=1 for the live Docker run");

  await waitHealth(primary);
  await waitHealth(secondary);
  const reconnectResults = await Promise.all(joined.map((item, i) => pages[i].evaluate(async ({ endpoint, roomId, peerId, accessToken }) => {
    const ws = new WebSocket(endpoint.replace(/^http/, "ws") + "/ws");
    const joinedPromise = new Promise<any>((resolve, reject) => {
      const timer = setTimeout(() => reject(new Error("reconnect timeout")), 10000);
      ws.onmessage = event => { const message = JSON.parse(event.data); if (message.type === "joined") { clearTimeout(timer); resolve(message); } };
      ws.onerror = () => { clearTimeout(timer); reject(new Error("reconnect websocket failed")); };
    });
    await new Promise<void>((resolve, reject) => { ws.onopen = () => resolve(); ws.onerror = () => reject(new Error("reconnect open failed")); });
    ws.send(JSON.stringify({ type: "join", roomId, data: { accessToken, reconnect: true, peerId } }));
    const message = await joinedPromise;
    return { peerId: message.peerId, samePeerId: message.peerId === peerId };
  }, { endpoint: secondary, roomId: item.roomId, peerId: item.peerId, accessToken: token(item.roomId, "owner-" + item.roomId) })));

  const pages = await Promise.all(
    Array.from({ length: rooms }, () => browser.newPage({ permissions: ["camera", "microphone"] }))
  );
  await Promise.all(pages.map(page => page.goto(primary + "/health")));

  const roomIds = Array.from({ length: rooms }, (_, i) => "FAILOVER-ROOM-" + (i + 1));
  const joined = await Promise.all(roomIds.map((roomId, i) => createRoom(pages[i], roomId)));
  expect(new Set(joined.map(item => item.peerId)).size).toBe(rooms);

  const before = await redisOwners(roomIds);
  const beforeOwners = before.map(item => item.value);
  for (const owner of before) {
    expect(owner.value?.nodeId).toBe("integration-primary");
  }

  const failoverStarted = Date.now();
  execFileSync("docker", ["compose", "-f", composeFile, "stop", "sfu-primary"], { stdio: "inherit" });
  await new Promise(resolve => setTimeout(resolve, ttlMs + 1500));

  const after = await redisOwners(roomIds);
  for (const owner of after) {
    expect(owner.value?.nodeId).toBe("integration-secondary");
    expect(owner.value?.endpoint).toContain("4200");
  }

  await waitHealth(secondary);
  execFileSync("docker", ["compose", "-f", composeFile, "start", "sfu-primary"], { stdio: "inherit" });
  await waitHealth(primary);

  expect(reconnectResults.every(result => result.samePeerId)).toBeTruthy();
  const report = {
    rooms, roomIds, failoverMs: Date.now() - failoverStarted,
    ownersBefore: beforeOwners, owners: after,
    reconnectResults,
    pass: after.every(owner => owner.value?.nodeId === "integration-secondary" && String(owner.value?.endpoint ?? "").includes("4200")) &&
      reconnectResults.every(result => result.samePeerId)
  };
  fs.mkdirSync(artifactDir, { recursive: true });
  fs.writeFileSync(path.join(artifactDir, "sfu-multi-room-failover-report.json"), JSON.stringify(report, null, 2));
  expect(report.pass).toBeTruthy();
  await Promise.all(pages.map(page => page.close()));
});
