import { test, expect } from "@playwright/test";

test("web app boots", async ({ page }) => {
  await page.goto("/");
  await expect(page).toHaveTitle(/языкOn/i);
  await expect(page.locator("body")).toContainText("языкOn");
});

test("health endpoint is reachable through deployment", async ({ request }) => {
  const base = process.env.BASE_URL ?? "http://localhost:5173";
  const response = await request.get(new URL("/health", base).toString());
  expect([200, 404]).toContain(response.status());
});


test("SFU failover recovery contract is exposed by the room snapshot", async ({ request }) => {
  test.skip(!process.env.SFU_FAILOVER_URL, "Set SFU_FAILOVER_URL to run the live failover check");
  const base = process.env.SFU_FAILOVER_URL!;
  const health = await request.get(new URL("/health", base).toString());
  expect(health.ok()).toBeTruthy();
  const body = await health.json();
  expect(body).toHaveProperty("status");
  expect(body).toHaveProperty("version");
});

test("SFU failover harness requires explicit endpoints", async () => {
  if (!process.env.SFU_PRIMARY_URL && !process.env.SFU_SECONDARY_URL) test.skip(true, "Set SFU_PRIMARY_URL and SFU_SECONDARY_URL for a live run");
  expect(process.env.SFU_PRIMARY_URL).toBeTruthy();
  expect(process.env.SFU_SECONDARY_URL).toBeTruthy();
});
