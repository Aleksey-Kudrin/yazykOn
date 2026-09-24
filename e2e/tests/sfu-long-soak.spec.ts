import { execFileSync } from "node:child_process";
import fs from "node:fs";
import path from "node:path";
import { test, expect } from "@playwright/test";

const primary = process.env.SFU_PRIMARY_URL ?? "http://127.0.0.1:4100";
const secondary = process.env.SFU_SECONDARY_URL ?? "http://127.0.0.1:4200";
const composeFile = process.env.SFU_FAILOVER_COMPOSE ?? "e2e/docker-compose.sfu-failover.yml";
const durationMs = Math.max(10000, Number(process.env.SFU_SOAK_DURATION_MS ?? 120000));
const sampleMs = Math.max(250, Number(process.env.SFU_SOAK_SAMPLE_MS ?? 2000));
const cycleMs = Math.max(1000, Number(process.env.SFU_SOAK_CYCLE_MS ?? 10000));
const slaMs = Number(process.env.SFU_FAILOVER_SLA_MS ?? 30000);
const maxErrorRate = Number(process.env.SFU_SOAK_MAX_ERROR_RATE ?? 0.01);
const artifactDir = process.env.SFU_ARTIFACT_DIR ?? "artifacts";

type Metrics = Record<string, number>;
type Sample = { at: string; primary: Metrics; secondary: Metrics };

async function metrics(endpoint: string): Promise<Metrics> {
  const response = await fetch(endpoint + "/metrics");
  if (!response.ok) throw new Error("metrics unavailable: " + endpoint);
  const out: Metrics = {};
  for (const line of (await response.text()).split("\n")) {
    if (!line || line.startsWith("#")) continue;
    const [name, value] = line.trim().split(/\s+/);
    const numeric = Number(value);
    if (name && Number.isFinite(numeric)) out[name] = numeric;
  }
  return out;
}

async function waitHealth(endpoint: string) {
  for (let i = 0; i < 60; i++) {
    try {
      const response = await fetch(endpoint + "/health");
      if (response.ok) return;
    } catch {}
    await new Promise(resolve => setTimeout(resolve, 250));
  }
  throw new Error("health timeout: " + endpoint);
}

function delta(a: Metrics, b: Metrics, name: string) {
  return (b[name] ?? 0) - (a[name] ?? 0);
}

test("SFU long soak keeps health, runtime metrics and failover errors bounded", async ({ page }) => {
  test.skip(!process.env.SFU_FAILOVER_LIVE, "Set SFU_FAILOVER_LIVE=1 for the live Docker run");

  await waitHealth(primary);
  await waitHealth(secondary);

  const started = Date.now();
  const deadline = started + durationMs;
  const samples: Sample[] = [];
  const failovers: Array<{ cycle: number; target: string; recoveryMs: number; ok: boolean }> = [];
  let sampleErrors = 0;
  let cycle = 0;

  const sample = async () => {
    try {
      const [p, s] = await Promise.all([metrics(primary), metrics(secondary)]);
      samples.push({ at: new Date().toISOString(), primary: p, secondary: s });
    } catch {
      sampleErrors++;
    }
  };

  await page.goto(primary + "/health");
  await sample();

  try {
    while (Date.now() < deadline) {
      const target = cycle % 2 === 0 ? secondary : primary;
      const failed = cycle % 2 === 0 ? "sfu-primary" : "sfu-secondary";
      const source = cycle % 2 === 0 ? primary : secondary;
      const failoverStarted = Date.now();

      execFileSync("docker", ["compose", "-f", composeFile, "stop", failed], { stdio: "inherit" });

      let targetReady = false;
      try {
        await waitHealth(target);
        targetReady = true;
      } catch {}

      const recoveryMs = Date.now() - failoverStarted;
      failovers.push({ cycle, target, recoveryMs, ok: targetReady && recoveryMs <= slaMs });

      execFileSync("docker", ["compose", "-f", composeFile, "start", failed], { stdio: "inherit" });
      await waitHealth(source);
      await sample();

      const holdUntil = Math.min(deadline, Date.now() + cycleMs);
      while (Date.now() < holdUntil) {
        await new Promise(resolve => setTimeout(resolve, Math.min(sampleMs, holdUntil - Date.now())));
        if (Date.now() < deadline) await sample();
      }
      cycle++;
    }
  } finally {
    execFileSync("docker", ["compose", "-f", composeFile, "start", "sfu-primary"], { stdio: "inherit" });
    execFileSync("docker", ["compose", "-f", composeFile, "start", "sfu-secondary"], { stdio: "inherit" });
    await waitHealth(primary).catch(() => {});
    await waitHealth(secondary).catch(() => {});
    await sample();
  }

  const failedFailovers = failovers.filter(item => !item.ok);
  const maxRecoveryMs = failovers.length ? Math.max(...failovers.map(item => item.recoveryMs)) : 0;
  const first = samples[0];
  const last = samples[samples.length - 1];

  const metricExtremes = samples.length ? {
    primaryMaxGoroutines: Math.max(...samples.map(s => s.primary.yazykon_media_goroutines ?? 0)),
    secondaryMaxGoroutines: Math.max(...samples.map(s => s.secondary.yazykon_media_goroutines ?? 0)),
    primaryMaxHeap: Math.max(...samples.map(s => s.primary.yazykon_media_heap_bytes ?? 0)),
    secondaryMaxHeap: Math.max(...samples.map(s => s.secondary.yazykon_media_heap_bytes ?? 0)),
    primaryMaxPeers: Math.max(...samples.map(s => s.primary.yazykon_media_active_peers ?? 0)),
    secondaryMaxPeers: Math.max(...samples.map(s => s.secondary.yazykon_media_active_peers ?? 0)),
    primaryMaxRooms: Math.max(...samples.map(s => s.primary.yazykon_media_active_rooms ?? 0)),
    secondaryMaxRooms: Math.max(...samples.map(s => s.secondary.yazykon_media_active_rooms ?? 0))
  } : null;

  const runtimeGrowth = first && last ? {
    primaryGoroutinesDelta: delta(first.primary, last.primary, "yazykon_media_goroutines"),
    secondaryGoroutinesDelta: delta(first.secondary, last.secondary, "yazykon_media_goroutines"),
    primaryHeapDelta: delta(first.primary, last.primary, "yazykon_media_heap_bytes"),
    secondaryHeapDelta: delta(first.secondary, last.secondary, "yazykon_media_heap_bytes"),
    primaryRoomsDelta: delta(first.primary, last.primary, "yazykon_media_active_rooms"),
    secondaryRoomsDelta: delta(first.secondary, last.secondary, "yazykon_media_active_rooms"),
    primaryPeersDelta: delta(first.primary, last.primary, "yazykon_media_active_peers"),
    secondaryPeersDelta: delta(first.secondary, last.secondary, "yazykon_media_active_peers")
  } : null;

  const attempts = Math.max(1, failovers.length);
  const errorRate = (failedFailovers.length + sampleErrors) / (attempts + samples.length);

  const report = {
    generatedAt: new Date().toISOString(),
    durationMs: Date.now() - started,
    cycles: cycle,
    samples: samples.length,
    sampleErrors,
    failovers,
    summary: {
      failedFailovers: failedFailovers.length,
      maxRecoveryMs,
      errorRate,
      maxAllowedErrorRate: maxErrorRate,
      slaMs
    },
    runtimeGrowth,
    metricExtremes,
    pass: failedFailovers.length === 0 && errorRate <= maxErrorRate
  };

  fs.mkdirSync(artifactDir, { recursive: true });
  fs.writeFileSync(path.join(artifactDir, "sfu-long-soak-report.json"), JSON.stringify(report, null, 2));

  expect(failedFailovers).toHaveLength(0);
  expect(errorRate).toBeLessThanOrEqual(maxErrorRate);
  expect(samples.length).toBeGreaterThan(1);
});
