import { createHmac } from "node:crypto";
import { execFileSync } from "node:child_process";
import { test, expect } from "@playwright/test";

const primary = process.env.SFU_PRIMARY_URL ?? "http://127.0.0.1:4100";
const secondary = process.env.SFU_SECONDARY_URL ?? "http://127.0.0.1:4200";
const composeFile = process.env.SFU_FAILOVER_COMPOSE ?? "e2e/docker-compose.sfu-failover.yml";
const cycles = Math.max(2, Number(process.env.SFU_SLA_CYCLES ?? 4));
const slaMs = Number(process.env.SFU_FAILOVER_SLA_MS ?? 30000);
const ttlMs = Number(process.env.SFU_ROOM_OWNER_TTL_MS ?? 3000);
const secret = process.env.ROOM_ACCESS_SECRET ?? "integration-secret";
const roomId = process.env.SFU_SLA_ROOM ?? "MEDIA-SLA";

function percentile(values: number[], p: number) {
  const sorted = [...values].sort((a, b) => a - b);
  if (!sorted.length) return 0;
  return sorted[Math.min(sorted.length - 1, Math.ceil(p / 100 * sorted.length) - 1)];
}

function token(userId: string) {
  const payload = Buffer.from(JSON.stringify({
    roomId, userId, role: userId === "p1" ? "host" : "participant",
    exp: Math.floor(Date.now() / 1000) + 600
  })).toString("base64url");
  return payload + "." + createHmac("sha256", secret).update(payload).digest("base64url");
}

async function waitHealth(endpoint: string) {
  for (let i = 0; i < 60; i++) {
    try {
      if ((await fetch(endpoint + "/health")).ok) return;
    } catch {}
    await new Promise(resolve => setTimeout(resolve, 250));
  }
  throw new Error("SFU health timeout: " + endpoint);
}

async function connect(page: import("@playwright/test").Page, endpoint: string, userId: string, peerId = "") {
  return page.evaluate(async ({ endpoint, roomId, accessToken, peerId }) => {
    const stream = (window as any).__slaStream as MediaStream | undefined
      ?? await navigator.mediaDevices.getUserMedia({ video: true, audio: false });
    (window as any).__slaStream = stream;
    const pc = new RTCPeerConnection();
    pc.ontrack = event => {
      const video = document.createElement("video");
      video.autoplay = true; video.muted = true; video.playsInline = true;
      video.srcObject = event.streams[0] ?? new MediaStream([event.track]);
      document.body.appendChild(video);
      void video.play().catch(() => undefined);
    };

    const ws = new WebSocket(endpoint.replace(/^http/, "ws") + "/ws");
    const queue: any[] = [];
    const waiters = new Map<string, Array<(m: any) => void>>();
    const waitFor = (type: string) => new Promise<any>((resolve, reject) => {
      const queued = queue.findIndex(m => m.type === type);
      if (queued >= 0) return resolve(queue.splice(queued, 1)[0]);
      const timer = setTimeout(() => reject(new Error("timeout: " + type)), 15000);
      const list = waiters.get(type) ?? [];
      list.push(m => { clearTimeout(timer); resolve(m); });
      waiters.set(type, list);
    });

    ws.onmessage = event => {
      const message = JSON.parse(event.data);
      if (message.type === "ice" && message.data) void pc.addIceCandidate(message.data);
      const list = waiters.get(message.type);
      if (list?.length) {
        list.shift()!(message);
        if (!list.length) waiters.delete(message.type);
      } else queue.push(message);
    };
    await new Promise<void>((resolve, reject) => {
      ws.onopen = () => resolve();
      ws.onerror = () => reject(new Error("websocket failed"));
    });
    pc.onicecandidate = event => {
      if (event.candidate && ws.readyState === WebSocket.OPEN)
        ws.send(JSON.stringify({ type: "ice", roomId, data: event.candidate.toJSON() }));
    };
    for (const track of stream.getTracks()) pc.addTrack(track, stream);
    ws.send(JSON.stringify({ type: "join", roomId, peerId: peerId || undefined,
      data: { accessToken, reconnect: Boolean(peerId) } }));
    const joined = await waitFor("joined");
    const offer = await pc.createOffer();
    await pc.setLocalDescription(offer);
    ws.send(JSON.stringify({ type: "offer", roomId, data: pc.localDescription }));
    const answer = await waitFor("answer");
    await pc.setRemoteDescription(answer.data);
    return joined.peerId;
  }, { endpoint, roomId, accessToken: token(userId), peerId });
}

async function waitRemoteFrame(page: import("@playwright/test").Page) {
  await page.evaluate(() => new Promise<void>((resolve, reject) => {
    const deadline = setTimeout(() => reject(new Error("remote media recovery timeout")), 15000);
    const check = () => {
      if ([...document.querySelectorAll("video")].some(v => v.readyState >= 2 && v.videoWidth > 0 && v.videoHeight > 0)) {
        clearTimeout(deadline); resolve(); return;
      }
      requestAnimationFrame(check);
    };
    check();
  });
}

test("SFU failover SLA measures actual remote media recovery", async ({ browser }) => {
  test.skip(!process.env.SFU_FAILOVER_LIVE, "Set SFU_FAILOVER_LIVE=1 for the live Docker run");

  const samples: number[] = [];
  let source = primary;
  let target = secondary;
  let failed = "sfu-primary";

  const pages = await Promise.all([
    browser.newPage({ permissions: ["camera", "microphone"] }),
    browser.newPage({ permissions: ["camera", "microphone"] })
  ]);
  await Promise.all(pages.map(page => page.goto(primary + "/health")));

  let peerIds = await Promise.all([
    connect(pages[0], source, "p1"),
    connect(pages[1], source, "p2")
  ]);
  await waitRemoteFrame(pages[1]);

  try {
    for (let cycle = 0; cycle < cycles; cycle++) {
      await waitHealth(source);
      const started = Date.now();
      execFileSync("docker", ["compose", "-f", composeFile, "stop", failed], { stdio: "inherit" });
      await new Promise(resolve => setTimeout(resolve, ttlMs + 500));

      peerIds = await Promise.all([
        connect(pages[0], target, "p1", peerIds[0]),
        connect(pages[1], target, "p2", peerIds[1])
      ]);
      await Promise.all(pages.map(waitRemoteFrame));

      const recovery = Date.now() - started;
      samples.push(recovery);
      console.log(JSON.stringify({ cycle: cycle + 1, from: source, to: target, remoteMediaRecoveryMs: recovery }));
      expect(recovery).toBeLessThanOrEqual(slaMs);

      execFileSync("docker", ["compose", "-f", composeFile, "start", failed], { stdio: "inherit" });
      await waitHealth(source);

      [source, target] = [target, source];
      failed = failed === "sfu-primary" ? "sfu-secondary" : "sfu-primary";
    }
  } finally {
    if (failed === "sfu-primary") {
      try { execFileSync("docker", ["compose", "-f", composeFile, "start", "sfu-primary"], { stdio: "inherit" }); } catch {}
    } else {
      try { execFileSync("docker", ["compose", "-f", composeFile, "start", "sfu-secondary"], { stdio: "inherit" }); } catch {}
    }
    await Promise.all(pages.map(page => page.close()));
  }

  const summary = {
    cycles: samples.length,
    minMs: Math.min(...samples),
    avgMs: Math.round(samples.reduce((sum, value) => sum + value, 0) / samples.length),
    p95Ms: percentile(samples, 95),
    maxMs: Math.max(...samples),
    slaMs
  };
  console.log("SFU_REMOTE_MEDIA_SLA_SUMMARY=" + JSON.stringify(summary));
  expect(summary.maxMs).toBeLessThanOrEqual(slaMs);
  expect(summary.p95Ms).toBeLessThanOrEqual(slaMs);
});
