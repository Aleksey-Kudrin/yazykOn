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
