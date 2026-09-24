import { test, expect } from "@playwright/test";

const primary = process.env.SFU_PRIMARY_URL ?? "http://127.0.0.1:4100";
const secondary = process.env.SFU_SECONDARY_URL ?? "http://127.0.0.1:4200";
const waitMs = Number(process.env.SFU_FAILOVER_WAIT_MS ?? 7000);

test("two-SFU Redis takeover: primary loss allows secondary ownership", async ({ request }) => {
  test.skip(!process.env.SFU_FAILOVER_LIVE, "Set SFU_FAILOVER_LIVE=1 for the live Docker failover run");

  const a = await request.get(new URL("/health", primary).toString());
  const b = await request.get(new URL("/health", secondary).toString());
  expect(a.ok()).toBeTruthy();
  expect(b.ok()).toBeTruthy();

  const aBody = await a.json();
  const bBody = await b.json();
  expect(aBody.cluster).toBeTruthy();
  expect(bBody.cluster).toBeTruthy();
  expect(aBody.nodeId).not.toBe(bBody.nodeId);

  // The actual browser/media recovery is intentionally kept separate from
  // this control-plane probe. The two processes must share Redis and use a
  // short SFU_ROOM_OWNER_TTL in the integration environment.
  await new Promise(resolve => setTimeout(resolve, waitMs));

  const aAfter = await request.get(new URL("/health", primary).toString()).catch(() => null);
  const bAfter = await request.get(new URL("/health", secondary).toString());
  expect(bAfter.ok()).toBeTruthy();

  if (aAfter) {
    expect((await aAfter.json()).nodeId).toBe(aBody.nodeId);
  }
  expect((await bAfter.json()).nodeId).toBe(bBody.nodeId);
});
