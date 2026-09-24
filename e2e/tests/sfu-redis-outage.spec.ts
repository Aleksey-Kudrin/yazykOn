import { execFileSync } from "node:child_process";
import { test, expect } from "@playwright/test";

const primary = process.env.SFU_PRIMARY_URL ?? "http://127.0.0.1:4100";
const secondary = process.env.SFU_SECONDARY_URL ?? "http://127.0.0.1:4200";
const composeFile = process.env.SFU_FAILOVER_COMPOSE ?? "e2e/docker-compose.sfu-failover.yml";
const heartbeatMs = Number(process.env.SFU_CLUSTER_HEARTBEAT_MS ?? 1000);

async function health(endpoint: string) {
  const response = await fetch(endpoint + "/health");
  expect(response.ok).toBeTruthy();
  return response.json() as Promise<{ cluster: boolean; nodeId: string }>;
}

async function waitForCluster(endpoint: string, expected: boolean) {
  for (let i = 0; i < 30; i++) {
    const body = await health(endpoint);
    if (body.cluster === expected) return body;
    await new Promise(resolve => setTimeout(resolve, 250));
  }
  throw new Error(`cluster state did not become ${expected}: ${endpoint}`);
}

test("SFU detects Redis control-plane outage and recovery", async () => {
  test.skip(!process.env.SFU_FAILOVER_LIVE, "Set SFU_FAILOVER_LIVE=1 for the live Docker run");

  await waitForCluster(primary, true);
  await waitForCluster(secondary, true);

  execFileSync("docker", ["compose", "-f", composeFile, "stop", "redis"], { stdio: "inherit" });
  try {
    await new Promise(resolve => setTimeout(resolve, Math.max(heartbeatMs * 4, 3500)));
    const [primaryDown, secondaryDown] = await Promise.all([
      waitForCluster(primary, false),
      waitForCluster(secondary, false)
    ]);
    expect(primaryDown.nodeId).not.toBe(secondaryDown.nodeId);
  } finally {
    execFileSync("docker", ["compose", "-f", composeFile, "start", "redis"], { stdio: "inherit" });
  }

  await waitForCluster(primary, true);
  await waitForCluster(secondary, true);
});
