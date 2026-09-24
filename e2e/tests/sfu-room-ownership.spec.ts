import { test, expect } from "@playwright/test";

const redis = process.env.REDIS_URL ?? "redis://127.0.0.1:6379";
const room = process.env.SFU_TEST_ROOM ?? "failover-room";
const ttlMs = Number(process.env.SFU_ROOM_OWNER_TTL_MS ?? 3500);

test("SFU room ownership transfers after primary lease expiry", async () => {
  test.skip(!process.env.SFU_FAILOVER_LIVE, "Set SFU_FAILOVER_LIVE=1 for the live integration run");

  const { createClient } = await import("redis");
  const client = createClient({ url: redis });
  await client.connect();

  const ownerKey = `yazykon:sfu:owner:${room}`;
  const primary = JSON.stringify({ nodeId: "integration-primary", endpoint: "ws://127.0.0.1:4100" });
  const secondary = JSON.stringify({ nodeId: "integration-secondary", endpoint: "ws://127.0.0.1:4200" });

  await client.del(ownerKey);
  expect(await client.set(ownerKey, primary, { NX: true, PX: ttlMs })).toBe("OK");
  expect(JSON.parse((await client.get(ownerKey))!).nodeId).toBe("integration-primary");

  await new Promise(resolve => setTimeout(resolve, ttlMs + 500));

  expect(await client.set(ownerKey, secondary, { NX: true, PX: ttlMs })).toBe("OK");
  expect(JSON.parse((await client.get(ownerKey))!).nodeId).toBe("integration-secondary");

  await client.quit();
});
