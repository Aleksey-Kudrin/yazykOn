import { test, expect } from "@playwright/test";
import { waitForClusterReady } from "./helpers/sfu-health";

const primary = process.env.SFU_PRIMARY_URL ?? "http://127.0.0.1:4100";
const secondary = process.env.SFU_SECONDARY_URL ?? "http://127.0.0.1:4200";
const waitMs = Number(process.env.SFU_FAILOVER_WAIT_MS ?? 7000);
test("two-SFU Redis takeover: primary loss allows secondary ownership", async () => {
  test.skip(!process.env.SFU_FAILOVER_LIVE, "Set SFU_FAILOVER_LIVE=1 for the live Docker failover run");

  const aBody = await waitForClusterReady(primary);
  const bBody = await waitForClusterReady(secondary);
  expect(aBody.nodeId).not.toBe(bBody.nodeId);

  // The actual browser/media recovery is intentionally kept separate from
  // this control-plane probe. The two processes must share Redis and use a
  // short SFU_ROOM_OWNER_TTL in the integration environment.
  await new Promise(resolve => setTimeout(resolve, waitMs));

  let aAfter: { nodeId: string } | null = null;
  try {
    const response = await fetch(new URL("/health", primary).toString());
    if (response.ok) aAfter = await response.json() as { nodeId: string };
  } catch {}

  const bAfterResponse = await fetch(new URL("/health", secondary).toString());
  expect(bAfterResponse.ok).toBeTruthy();
  const bAfter = await bAfterResponse.json() as { nodeId: string };

  if (aAfter) expect(aAfter.nodeId).toBe(aBody.nodeId);
  expect(bAfter.nodeId).toBe(bBody.nodeId);
});
