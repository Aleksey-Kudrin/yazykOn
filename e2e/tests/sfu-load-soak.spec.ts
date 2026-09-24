import { execFileSync } from "node:child_process";
import { test, expect } from "@playwright/test";

const primary = process.env.SFU_PRIMARY_URL ?? "http://127.0.0.1:4100";
const secondary = process.env.SFU_SECONDARY_URL ?? "http://127.0.0.1:4200";
const composeFile = process.env.SFU_FAILOVER_COMPOSE ?? "e2e/docker-compose.sfu-failover.yml";
const participants = Math.min(16, Math.max(4, Number(process.env.SFU_LOAD_PARTICIPANTS ?? 8)));
const rounds = Math.min(20, Math.max(3, Number(process.env.SFU_LOAD_ROUNDS ?? 5)));
const holdMs = Math.min(10000, Math.max(250, Number(process.env.SFU_LOAD_HOLD_MS ?? 1000)));

test("SFU load soak: concurrent participants reconnect repeatedly", async ({ browser }) => {
  test.skip(!process.env.SFU_FAILOVER_LIVE, "Set SFU_FAILOVER_LIVE=1 for the live Docker run");

  const pages = await Promise.all(
    Array.from({ length: participants }, () => browser.newPage({ permissions: ["camera", "microphone"] }))
  );
  await Promise.all(pages.map(page => page.goto(primary + "/health")));

  const results = await Promise.all(pages.map(async (page, index) => {
    return page.evaluate(async ({ endpoint, index }) => {
      const stream = await navigator.mediaDevices.getUserMedia({ video: true, audio: false });
      const pc = new RTCPeerConnection();
      for (const track of stream.getTracks()) pc.addTrack(track, stream);
      const offer = await pc.createOffer();
      await pc.setLocalDescription(offer);
      return {
        index,
        live: stream.getVideoTracks().every(track => track.readyState === "live"),
        connectionState: pc.connectionState,
        offerReady: Boolean(pc.localDescription?.sdp)
      };
    }, { endpoint: primary, index }));
  }));

  expect(results.filter(result => result.live && result.offerReady)).toHaveLength(participants);

  for (let round = 0; round < rounds; round++) {
    const target = round % 2 === 0 ? secondary : primary;
    const failed = round % 2 === 0 ? "sfu-primary" : "sfu-secondary";
    const source = round % 2 === 0 ? primary : secondary;

    execFileSync("docker", ["compose", "-f", composeFile, "stop", failed], { stdio: "inherit" });

    for (let attempt = 0; attempt < 30; attempt++) {
      try {
        const health = await fetch(target + "/health");
        if (health.ok) break;
      } catch {}
      await new Promise(resolve => setTimeout(resolve, 250));
      if (attempt === 29) throw new Error("target SFU is unavailable");
    }

    await Promise.all(pages.map(async page => {
      await page.waitForTimeout(holdMs);
      return page.evaluate(async endpoint => {
        const registry = (globalThis as any).__sfuLoadSoakStream as MediaStream | undefined;
        const stream = registry ?? await navigator.mediaDevices.getUserMedia({ video: true, audio: false });
        (globalThis as any).__sfuLoadSoakStream = stream;
        return stream.getVideoTracks().every(track => track.readyState === "live");
      }, target);
    }));

    execFileSync("docker", ["compose", "-f", composeFile, "start", failed], { stdio: "inherit" });
    for (let attempt = 0; attempt < 30; attempt++) {
      try {
        const health = await fetch(source + "/health");
        if (health.ok) break;
      } catch {}
      await new Promise(resolve => setTimeout(resolve, 250));
      if (attempt === 29) throw new Error(failed + " did not recover");
    }
  }

  const health = await Promise.all([primary, secondary].map(async endpoint => {
    const response = await fetch(endpoint + "/health");
    expect(response.ok).toBeTruthy();
    return response.json();
  }));
  expect(health[0].cluster).toBeTruthy();
  expect(health[1].cluster).toBeTruthy();
  expect(health[0].nodeId).not.toBe(health[1].nodeId);

  await Promise.all(pages.map(page => page.evaluate(() => {
    const stream = (globalThis as any).__sfuLoadSoakStream as MediaStream | undefined;
    if (stream) for (const track of stream.getTracks()) track.stop();
    (globalThis as any).__sfuLoadSoakStream = undefined;
  }).catch(() => {})));
  await Promise.all(pages.map(page => page.close()));
});
