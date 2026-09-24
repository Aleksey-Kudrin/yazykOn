import { execFileSync } from "node:child_process";
import { test, expect } from "@playwright/test";

const primary = process.env.SFU_PRIMARY_URL ?? "http://127.0.0.1:4100";
const secondary = process.env.SFU_SECONDARY_URL ?? "http://127.0.0.1:4200";
const composeFile = process.env.SFU_FAILOVER_COMPOSE ?? "e2e/docker-compose.sfu-failover.yml";
const rooms = Math.min(6, Math.max(2, Number(process.env.SFU_ROOM_FAILOVER_ROOMS ?? 3)));
const ttlMs = Number(process.env.SFU_ROOM_OWNER_TTL_MS ?? 3000);

async function waitHealth(endpoint: string) {
  for (let i = 0; i < 40; i++) {
    try {
      const response = await fetch(endpoint + "/health");
      if (response.ok) return;
    } catch {}
    await new Promise(resolve => setTimeout(resolve, 250));
  }
  throw new Error("SFU health timeout: " + endpoint);
}

test("multiple rooms survive coordinated SFU owner failover", async () => {
  test.skip(!process.env.SFU_FAILOVER_LIVE, "Set SFU_FAILOVER_LIVE=1 for the live Docker run");

  await waitHealth(primary);
  await waitHealth(secondary);

  execFileSync("docker", ["compose", "-f", composeFile, "stop", "sfu-primary"], { stdio: "inherit" });
  await new Promise(resolve => setTimeout(resolve, ttlMs + 1500));
  await waitHealth(secondary);

  const ownerChecks = await Promise.all(
    Array.from({ length: rooms }, async (_, i) => {
      const response = await fetch(secondary + "/health");
      return { room: "ROOM-" + (i + 1), healthy: response.ok };
    })
  );
  expect(ownerChecks.every(item => item.healthy)).toBeTruthy();

  execFileSync("docker", ["compose", "-f", composeFile, "start", "sfu-primary"], { stdio: "inherit" });
  await waitHealth(primary);
  expect((await fetch(primary + "/health")).ok).toBeTruthy();
});
