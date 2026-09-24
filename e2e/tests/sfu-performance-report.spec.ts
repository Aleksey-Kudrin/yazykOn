import { test, expect } from "@playwright/test";
import fs from "node:fs";
import path from "node:path";

const primary = process.env.SFU_PRIMARY_URL ?? "http://127.0.0.1:4100";
const secondary = process.env.SFU_SECONDARY_URL ?? "http://127.0.0.1:4200";
const slaMs = Number(process.env.SFU_FAILOVER_SLA_MS ?? 30000);
const artifactDir = process.env.SFU_ARTIFACT_DIR ?? "artifacts";
const sampleCount = Math.max(3, Number(process.env.SFU_PERF_SAMPLES ?? 8));
const sampleIntervalMs = Math.max(100, Number(process.env.SFU_PERF_SAMPLE_INTERVAL_MS ?? 500));

function percentile(values: number[], p: number) {
  if (!values.length) return null;
  const sorted = [...values].sort((a, b) => a - b);
  const index = Math.min(sorted.length - 1, Math.ceil((p / 100) * sorted.length) - 1);
  return sorted[index];
}

async function sampleMetrics(url: string) {
  try {
    const response = await fetch(`${url}/metrics`);
    return response.ok ? await response.text() : "";
  } catch {
    return "";
  }
}

function metricNumber(text: string, name: string) {
  const match = text.match(new RegExp(`^${name}(?:\\{[^}]*\\})?\\s+([0-9.]+)$`, "m"));
  return match ? Number(match[1]) : null;
}

test("generate machine-readable SFU performance report", async () => {
  test.skip(!process.env.SFU_FAILOVER_LIVE, "Set SFU_FAILOVER_LIVE=1 for the live Docker failover run");

  const samples: number[] = [];
  const primarySamples: number[] = [];
  const secondarySamples: number[] = [];
  let primaryMetrics = "";
  let secondaryMetrics = "";

  for (let i = 0; i < sampleCount; i++) {
    primaryMetrics = await sampleMetrics(primary);
    secondaryMetrics = await sampleMetrics(secondary);
    for (const [text, target] of [[primaryMetrics, primarySamples], [secondaryMetrics, secondarySamples]] as const) {
      const value = metricNumber(text, "sfu_failover_recovery_ms");
      if (value !== null && Number.isFinite(value) && value >= 0) {
        samples.push(value);
        target.push(value);
      }
    }
    if (i + 1 < sampleCount) await new Promise(resolve => setTimeout(resolve, sampleIntervalMs));
  }

  const report = {
    generatedAt: new Date().toISOString(),
    target: { primary, secondary },
    sampleCount: samples.length,
    configuredSamples: sampleCount,
    sampleIntervalMs,
    samples: { all: samples, primary: primarySamples, secondary: secondarySamples },
    recoveryMs: {
      p50: percentile(samples, 50),
      p95: percentile(samples, 95),
      p99: percentile(samples, 99),
      max: samples.length ? Math.max(...samples) : null
    },
    slaMs,
    pass: samples.length > 0 && (percentile(samples, 95) ?? Infinity) <= slaMs,
    metrics: {
      primary: primaryMetrics,
      secondary: secondaryMetrics
    }
  };

  fs.mkdirSync(artifactDir, { recursive: true });
  fs.writeFileSync(path.join(artifactDir, "sfu-performance-report.json"), JSON.stringify(report, null, 2));
  fs.writeFileSync(path.join(artifactDir, "sfu-performance-report.md"), `# SFU Performance Report\n\n- Samples: ${report.sampleCount}\n- p50 recovery: ${report.recoveryMs.p50 ?? "n/a"} ms\n- p95 recovery: ${report.recoveryMs.p95 ?? "n/a"} ms\n- p99 recovery: ${report.recoveryMs.p99 ?? "n/a"} ms\n- Max recovery: ${report.recoveryMs.max ?? "n/a"} ms\n- SLA: ${slaMs} ms\n- Result: ${report.pass ? "PASS" : "FAIL"}\n`);

  expect(report.pass).toBeTruthy();
});
