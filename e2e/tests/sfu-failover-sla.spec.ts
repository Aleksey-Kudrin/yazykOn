import { execFileSync } from "node:child_process";
import { test, expect } from "@playwright/test";

const primary = process.env.SFU_PRIMARY_URL ?? "http://127.0.0.1:4100";
const secondary = process.env.SFU_SECONDARY_URL ?? "http://127.0.0.1:4200";
const composeFile = process.env.SFU_FAILOVER_COMPOSE ?? "e2e/docker-compose.sfu-failover.yml";
const cycles = Math.max(2, Number(process.env.SFU_SLA_CYCLES ?? 4));
const slaMs = Number(process.env.SFU_FAILOVER_SLA_MS ?? 30000);

function percentile(values: number[], p: number) {
  const sorted = [...values].sort((a, b) => a - b);
  if (!sorted.length) return 0;
  const index = Math.min(sorted.length - 1, Math.ceil((p / 100) * sorted.length) - 1);
  return sorted[index];
}

async function waitHealth(endpoint: string) {
  for (let i = 0; i < 40; i++) {
    try {
      const response = await fetch(endpoint + "/health");
      if (response.ok) return;
    } catch {}
    await new Promise(resolve => setTimeout(resolve, 250));
  }
  throw new Error(`SFU health timeout: ${endpoint}`);
}

test("SFU failover SLA: collect min/avg/p95/max across repeated cycles", async () => {
  test.skip(!process.env.SFU_FAILOVER_LIVE, "Set SFU_FAILOVER_LIVE=1 for the live Docker run");

  const samples: number[] = [];

  for (let cycle = 0; cycle < cycles; cycle++) {
    const failed = cycle % 2 === 0 ? "sfu-primary" : "sfu-secondary";
    const target = cycle % 2 === 0 ? secondary : primary;
    const source = cycle % 2 === 0 ? primary : secondary;

    await waitHealth(source);
    const started = Date.now();
    execFileSync("docker", ["compose", "-f", composeFile, "stop", failed], { stdio: "inherit" });
    await waitHealth(target);

    const recovery = Date.now() - started;
    samples.push(recovery);

    console.log(JSON.stringify({
      cycle: cycle + 1,
      from: source,
      to: target,
      recoveryMs: recovery
    }));

    expect(recovery).toBeLessThanOrEqual(slaMs);

    execFileSync("docker", ["compose", "-f", composeFile, "start", failed], { stdio: "inherit" });
    await waitHealth(source);
  }

  const summary = {
    cycles: samples.length,
    minMs: Math.min(...samples),
    avgMs: Math.round(samples.reduce((sum, value) => sum + value, 0) / samples.length),
    p95Ms: percentile(samples, 95),
    maxMs: Math.max(...samples),
    slaMs
  };

  console.log("SFU_FAILOVER_SLA_SUMMARY=" + JSON.stringify(summary));
  expect(summary.maxMs).toBeLessThanOrEqual(slaMs);
  expect(summary.p95Ms).toBeLessThanOrEqual(slaMs);
});
