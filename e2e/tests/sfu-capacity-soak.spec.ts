import { test, expect } from "@playwright/test";

const primary = process.env.SFU_PRIMARY_URL ?? "http://127.0.0.1:4100";
const secondary = process.env.SFU_SECONDARY_URL ?? "http://127.0.0.1:4200";
const durationMs = Number(process.env.SFU_SOAK_DURATION_MS ?? 120_000);
const sampleMs = Number(process.env.SFU_SOAK_SAMPLE_MS ?? 2_000);
const maxErrorRate = Number(process.env.SFU_SOAK_MAX_ERROR_RATE ?? 0.01);
const maxHeapBytes = Number(process.env.SFU_SOAK_MAX_HEAP_BYTES ?? 0);

async function probe(url: string) {
  const started = performance.now();
  try {
    const response = await fetch(`${url}/health`);
    return { ok: response.ok, latencyMs: performance.now() - started };
  } catch {
    return { ok: false, latencyMs: performance.now() - started };
  }
}

async function metrics(url: string) {
  try {
    const response = await fetch(`${url}/metrics`);
    return response.ok ? await response.text() : "";
  } catch {
    return "";
  }
}

function gauge(text: string, names: string[]) {
  for (const name of names) {
    const match = text.match(new RegExp(`^${name}(?:\\{[^}]*\\})?\\s+([0-9.]+)$`, "m"));
    if (match) return Number(match[1]);
  }
  return null;
}

test("SFU capacity soak stays healthy under sustained probing", async () => {
  test.skip(!process.env.SFU_FAILOVER_LIVE, "Set SFU_FAILOVER_LIVE=1 for the live Docker run");

  const samples: Array<{ at: string; primary: Awaited<ReturnType<typeof probe>>; secondary: Awaited<ReturnType<typeof probe>> }> = [];
  const started = Date.now();

  while (Date.now() - started < durationMs) {
    const [p, s] = await Promise.all([probe(primary), probe(secondary)]);
    samples.push({ at: new Date().toISOString(), primary: p, secondary: s });
    await new Promise(resolve => setTimeout(resolve, sampleMs));
  }

  const probes = samples.flatMap(sample => [sample.primary, sample.secondary]);
  const failures = probes.filter(probe => !probe.ok).length;
  const errorRate = probes.length ? failures / probes.length : 1;
  const p95 = [...probes].sort((a, b) => a.latencyMs - b.latencyMs)[Math.min(probes.length - 1, Math.ceil(probes.length * 0.95) - 1)]?.latencyMs ?? Infinity;

  const [primaryMetrics, secondaryMetrics] = await Promise.all([metrics(primary), metrics(secondary)]);
  const heapValues = [
    gauge(primaryMetrics, ["process_resident_memory_bytes", "nodejs_heap_size_used_bytes"]),
    gauge(secondaryMetrics, ["process_resident_memory_bytes", "nodejs_heap_size_used_bytes"])
  ].filter((value): value is number => value !== null && Number.isFinite(value));
  const maxHeap = heapValues.length ? Math.max(...heapValues) : null;

  const report = {
    generatedAt: new Date().toISOString(),
    durationMs,
    sampleMs,
    probes: probes.length,
    failures,
    errorRate,
    healthLatencyMs: { p95 },
    maxHeapBytes: maxHeap,
    limits: { maxErrorRate, maxHeapBytes: maxHeapBytes || null },
    pass: errorRate <= maxErrorRate && (!maxHeapBytes || (maxHeap !== null && maxHeap <= maxHeapBytes))
  };

  expect(report.probes).toBeGreaterThan(0);
  expect(report.pass).toBeTruthy();
});
