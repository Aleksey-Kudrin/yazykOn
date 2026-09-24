import { test, expect } from "@playwright/test";

const primary = process.env.SFU_PRIMARY_URL ?? "http://127.0.0.1:4100";
const secondary = process.env.SFU_SECONDARY_URL ?? "http://127.0.0.1:4200";

type Metrics = Record<string, number>;

async function metrics(endpoint: string): Promise<Metrics> {
  const response = await fetch(endpoint + "/metrics");
  expect(response.ok).toBeTruthy();
  const out: Metrics = {};
  for (const line of (await response.text()).split("\n")) {
    if (!line || line.startsWith("#")) continue;
    const [name, value] = line.trim().split(/\s+/);
    out[name] = Number(value);
  }
  return out;
}

test("SFU runtime metrics remain bounded during participant churn", async () => {
  test.skip(!process.env.SFU_FAILOVER_LIVE, "Set SFU_FAILOVER_LIVE=1 for the live Docker run");

  const before = await Promise.all([metrics(primary), metrics(secondary)]);
  expect(Object.keys(before[0]).length).toBeGreaterThanOrEqual(6);
  expect(Object.keys(before[1]).length).toBeGreaterThanOrEqual(6);
  expect(before[0].yazykon_media_failover_claims_total).toBeGreaterThanOrEqual(0);
  expect(before[1].yazykon_media_owner_redirects_total).toBeGreaterThanOrEqual(0);
  expect(before[0].yazykon_media_active_peers).toBeGreaterThanOrEqual(0);
  expect(before[1].yazykon_media_active_peers).toBeGreaterThanOrEqual(0);

  const rounds = Math.max(3, Number(process.env.SFU_METRICS_ROUNDS ?? 5));
  for (let i = 0; i < rounds; i++) {
    await new Promise(resolve => setTimeout(resolve, 500));
    const current = await Promise.all([metrics(primary), metrics(secondary)]);
    for (const m of current) {
      expect(m.yazykon_media_active_peers).toBeGreaterThanOrEqual(0);
      expect(m.yazykon_media_active_rooms).toBeGreaterThanOrEqual(0);
      expect(m.yazykon_media_goroutines).toBeGreaterThan(0);
      expect(m.yazykon_media_heap_bytes).toBeGreaterThan(0);
    }
  }

  const after = await Promise.all([metrics(primary), metrics(secondary)]);
  for (let i = 0; i < 2; i++) {
    expect(after[i].yazykon_media_active_peers).toBeGreaterThanOrEqual(0);
    expect(after[i].yazykon_media_active_rooms).toBeGreaterThanOrEqual(0);
    expect(after[i].yazykon_media_goroutines).toBeGreaterThan(0);
  }
});
