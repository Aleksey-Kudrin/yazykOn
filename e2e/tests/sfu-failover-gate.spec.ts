import fs from "node:fs";
import path from "node:path";
import { test, expect } from "@playwright/test";

const artifactDir = process.env.SFU_ARTIFACT_DIR ?? "artifacts";

type Report = Record<string, any>;

function readReport(name: string): Report | null {
  const file = path.join(artifactDir, name);
  if (!fs.existsSync(file)) return null;
  try {
    return JSON.parse(fs.readFileSync(file, "utf8"));
  } catch {
    return null;
  }
}

test("SFU failover reports consolidate into a machine-readable gate", async () => {
  test.skip(!process.env.SFU_FAILOVER_LIVE, "Set SFU_FAILOVER_LIVE=1 for the live Docker run");

  const files = [
    "sfu-failover-latency-report.json",
    "sfu-chaos-regression-report.json",
    "sfu-long-soak-report.json",
    "sfu-performance-report.json"
  ];
  const reports = Object.fromEntries(files.map(file => [file, readReport(file)]));

  const missing = files.filter(file => reports[file] === null);
  const failed = files.filter(file => reports[file] !== null && reports[file].pass === false);

  const summary = {
    generatedAt: new Date().toISOString(),
    artifactDir,
    reports: Object.fromEntries(files.map(file => [file, {
      present: reports[file] !== null,
      pass: reports[file]?.pass ?? null
    }])),
    missing,
    failed,
    pass: missing.length === 0 && failed.length === 0
  };

  fs.mkdirSync(artifactDir, { recursive: true });
  fs.writeFileSync(
    path.join(artifactDir, "sfu-failover-gate-report.json"),
    JSON.stringify({ summary, reports }, null, 2)
  );
  fs.writeFileSync(
    path.join(artifactDir, "sfu-failover-gate-report.md"),
    [
      "# SFU Failover Gate",
      "",
      ...files.map(file => {
        const report = reports[file];
        return `- ${file}: ${report ? (report.pass ? "PASS" : "FAIL") : "MISSING"}`;
      }),
      "",
      `- Missing reports: ${missing.length}`,
      `- Failed reports: ${failed.length}`,
      `- Gate: ${summary.pass ? "PASS" : "FAIL"}`,
      ""
    ].join("\n")
  );

  expect(missing, "all failover reports must exist").toEqual([]);
  expect(failed, "all failover reports must pass").toEqual([]);
});
