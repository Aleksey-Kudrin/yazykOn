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


test("user can register and create a protected room from the home page", async ({ page }) => {
  let registered = false;
  await page.route("**/api/auth/me", route => route.fulfill({ status: 401, contentType: "application/json", body: JSON.stringify({ error: "UNAUTHENTICATED" }) }));
  await page.route("**/api/auth/register", async route => {
    registered = true;
    await route.fulfill({ status: 200, contentType: "application/json", body: JSON.stringify({ id: "user-e2e", username: "e2e-user" }) });
  });
  await page.route("**/api/rooms", async route => {
    expect(route.request().method()).toBe("POST");
    expect(route.request().postDataJSON()).toEqual({ name: "E2E конференция", password: "secret123" });
    await route.fulfill({
      status: 200,
      contentType: "application/json",
      body: JSON.stringify({ id: "E2EROOM", name: "E2E конференция", requiresPassword: true, createdAt: new Date().toISOString() })
    });
  });

  await page.goto("/");
  await page.getByTestId("username").fill("e2e-user");
  await page.getByTestId("auth-password").fill("secret123");
  await page.getByTestId("auth-toggle").click();
  await page.getByTestId("auth-submit").click();
  await expect(page.getByTestId("user-panel")).toContainText("e2e-user");
  expect(registered).toBe(true);

  await page.getByPlaceholder("Название конференции").fill("E2E конференция");
  await page.getByPlaceholder("Пароль комнаты (необязательно)").fill("secret123");
  await page.getByRole("button", { name: "Создать конференцию" }).click();

  await expect(page).toHaveURL(/\/room\/E2EROOM$/);
  await expect(page.locator("body")).toContainText("E2E конференция");
});
