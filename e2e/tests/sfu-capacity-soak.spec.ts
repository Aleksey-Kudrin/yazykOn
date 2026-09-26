import fs from "node:fs";
import path from "node:path";
import { test, expect } from "@playwright/test";

const primary = process.env.SFU_PRIMARY_URL ?? "http://127.0.0.1:4100";
const secondary = process.env.SFU_SECONDARY_URL ?? "http://127.0.0.1:4200";
const durationMs = Math.max(10000, Number(process.env.SFU_SOAK_DURATION_MS ?? 120_000));
const sampleMs = Math.max(250, Number(process.env.SFU_SOAK_SAMPLE_MS ?? 2_000));
const maxErrorRate = Number(process.env.SFU_SOAK_MAX_ERROR_RATE ?? 0.01);
const maxHeapBytes = Number(process.env.SFU_SOAK_MAX_HEAP_BYTES ?? 0);
const maxResourceGrowth = Number(process.env.SFU_SOAK_MAX_RESOURCE_GROWTH ?? 0.20);
const artifactDir = process.env.SFU_ARTIFACT_DIR ?? "artifacts";

type Probe = { ok: boolean; latencyMs: number };
type Metrics = Record<string, number>;

async function probe(url: string): Promise<Probe> {
  const started = performance.now();
  try {
    const response = await fetch(url + "/health");
    return { ok: response.ok, latencyMs: performance.now() - started };
  } catch {
    return { ok: false, latencyMs: performance.now() - started };
  }
}

async function metrics(url: string): Promise<Metrics> {
  const response = await fetch(url + "/metrics");
  if (!response.ok) throw new Error("metrics unavailable: " + url);
  const out: Metrics = {};
  for (const line of (await response.text()).split("\n")) {
    if (!line || line.startsWith("#")) continue;
    const [name, value] = line.trim().split(/\s+/);
    const n = Number(value);
    if (name && Number.isFinite(n)) out[name] = n;
  }
  return out;
}

function percentile(values: number[], p: number) {
  if (!values.length) return null;
  const sorted = [...values].sort((a, b) => a - b);
  return sorted[Math.min(sorted.length - 1, Math.ceil(sorted.length * p / 100) - 1)];
}

function growth(before: number, after: number) {
  return before > 0 ? (after - before) / before : null;
}

test("SFU capacity soak stays healthy with runtime leak budgets", async () => {
  test.skip(!process.env.SFU_FAILOVER_LIVE, "Set SFU_FAILOVER_LIVE=1 for the live Docker run");

  const started = Date.now();
  const samples: Array<{ at: string; primary: Probe; secondary: Probe; primaryMetrics: Metrics; secondaryMetrics: Metrics }> = [];
  let sampleErrors = 0;

  while (Date.now() - started < durationMs) {
    try {
      const [p, s, pm, sm] = await Promise.all([
        probe(primary), probe(secondary), metrics(primary), metrics(secondary)
      ]);
      samples.push({
        at: new Date().toISOString(),
        primary: p, secondary: s,
        primaryMetrics: pm, secondaryMetrics: sm
      });
    } catch {
      sampleErrors++;
    }
    await new Promise(resolve => setTimeout(resolve, sampleMs));
  }

  const probes = samples.flatMap(sample => [sample.primary, sample.secondary]);
  const failures = probes.filter(item => !item.ok).length;
  const errorRate = probes.length ? failures / probes.length : 1;
  const p95 = percentile(probes.map(item => item.latencyMs), 95);

  const first = samples[0];
  const last = samples[samples.length - 1];
  const required = [
    "yazykon_media_goroutines",
    "yazykon_media_heap_inuse_bytes",
    "yazykon_media_active_peers",
    "yazykon_media_active_rooms"
  ];
  const missingMetrics = first
    ? required.filter(name => first.primaryMetrics[name] === undefined || first.secondaryMetrics[name] === undefined)
    : required;

  const growths = first && last ? [
    growth(first.primaryMetrics.yazykon_media_goroutines ?? 0, last.primaryMetrics.yazykon_media_goroutines ?? 0),
    growth(first.secondaryMetrics.yazykon_media_goroutines ?? 0, last.secondaryMetrics.yazykon_media_goroutines ?? 0),
    growth(first.primaryMetrics.yazykon_media_heap_inuse_bytes ?? 0, last.primaryMetrics.yazykon_media_heap_inuse_bytes ?? 0),
    growth(first.secondaryMetrics.yazykon_media_heap_inuse_bytes ?? 0, last.secondaryMetrics.yazykon_media_heap_inuse_bytes ?? 0)
  ].filter((value): value is number => value !== null && Number.isFinite(value)) : [];
  const maxObservedResourceGrowth = growths.length ? Math.max(...growths) : Infinity;

  const heapValues = samples.flatMap(sample => [
    sample.primaryMetrics.yazykon_media_heap_inuse_bytes,
    sample.secondaryMetrics.yazykon_media_heap_inuse_bytes
  ]).filter((value): value is number => Number.isFinite(value));
  const maxHeap = heapValues.length ? Math.max(...heapValues) : null;

  const sampleErrorRate = (samples.length + sampleErrors) ? sampleErrors / (samples.length + sampleErrors) : 1;
  const pass = probes.length > 0 &&
    samples.length > 1 &&
    failures === 0 &&
    errorRate <= maxErrorRate &&
    sampleErrorRate <= maxErrorRate &&
    missingMetrics.length === 0 &&
    maxObservedResourceGrowth <= maxResourceGrowth &&
    (!maxHeapBytes || (maxHeap !== null && maxHeap <= maxHeapBytes));

  const report = {
    generatedAt: new Date().toISOString(),
    durationMs: Date.now() - started,
    configuredDurationMs: durationMs,
    sampleMs,
    samples: samples.length,
    sampleErrors,
    probes: probes.length,
    failures,
    errorRate,
    sampleErrorRate,
    healthLatencyMs: {
      p50: percentile(probes.map(item => item.latencyMs), 50),
      p95,
      p99: percentile(probes.map(item => item.latencyMs), 99),
      max: probes.length ? Math.max(...probes.map(item => item.latencyMs)) : null
    },
    maxHeapBytes: maxHeap,
    maxObservedResourceGrowth,
    limits: { maxErrorRate, maxHeapBytes: maxHeapBytes || null, maxResourceGrowth },
    missingMetrics,
    pass
  };

  fs.mkdirSync(artifactDir, { recursive: true });
  fs.writeFileSync(path.join(artifactDir, "sfu-capacity-soak-report.json"), JSON.stringify(report, null, 2));

  expect(missingMetrics).toEqual([]);
  expect(report.probes).toBeGreaterThan(0);
  expect(report.pass).toBeTruthy();
});
