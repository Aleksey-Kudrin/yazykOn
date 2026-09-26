import { createHmac } from "node:crypto";
import { execFileSync } from "node:child_process";
import { test, expect, type Page } from "@playwright/test";

const composeFile = "docker-compose.sfu-media.yml";
const primary = "http://127.0.0.1:4100";
const secondary = "http://127.0.0.1:4200";
const roomId = "MEDIAE2E";
const secret = "media-e2e-room-secret";

function accessToken(userId: string, role: "host" | "member") {
  const payload = Buffer.from(JSON.stringify({
    roomId,
    userId,
    role,
    exp: Math.floor(Date.now() / 1000) + 600
  })).toString("base64url");
  const sig = createHmac("sha256", secret).update(payload).digest("base64url");
  return payload + "." + sig;
}

async function waitHealth(endpoint: string) {
  for (let i = 0; i < 80; i++) {
    try {
      const response = await fetch(endpoint + "/health");
      if (response.ok) return;
    } catch {}
    await new Promise(resolve => setTimeout(resolve, 250));
  }
  throw new Error("SFU health timeout: " + endpoint);
}

async function connectClient(page: Page, endpoint: string, userId: string, role: "host" | "member") {
  await page.goto(endpoint + "/health");
  return page.evaluate(async ({ endpoint, token, roomId, peerId }) => {
    const wsUrl = endpoint.replace(/^http/, "ws") + "/ws";
    const state = {
      connection: "new",
      remoteTracks: 0,
      inboundBytes: 0,
      wsClosed: false,
      errors: [] as string[]
    };
    (window as any).__mediaE2E = state;

    const pc = new RTCPeerConnection();
    const ws = new WebSocket(wsUrl);
    const stream = await navigator.mediaDevices.getUserMedia({ video: true, audio: true });
    stream.getTracks().forEach(track => pc.addTrack(track, stream));

    let localOffer = false;
    const pendingIce: RTCIceCandidateInit[] = [];
    const send = (message: unknown) => {
      if (ws.readyState === WebSocket.OPEN) ws.send(JSON.stringify(message));
    };

    pc.onconnectionstatechange = () => {
      state.connection = pc.connectionState;
    };
    pc.ontrack = () => {
      state.remoteTracks += 1;
    };
    pc.onicecandidate = event => {
      if (!event.candidate) return;
      const candidate = event.candidate.toJSON();
      if (ws.readyState === WebSocket.OPEN) send({ type: "ice", roomId, peerId, data: candidate });
      else pendingIce.push(candidate);
    };
    ws.onclose = () => { state.wsClosed = true; };

    const waitOpen = new Promise<void>((resolve, reject) => {
      const timer = setTimeout(() => reject(new Error("WebSocket open timeout")), 10000);
      ws.onopen = () => {
        clearTimeout(timer);
        send({ type: "join", roomId, peerId, data: { accessToken: token } });
        for (const candidate of pendingIce.splice(0)) send({ type: "ice", roomId, peerId, data: candidate });
        resolve();
      };
      ws.onerror = () => state.errors.push("websocket-error");
    });
    await waitOpen;

    ws.onmessage = async event => {
      const message = JSON.parse(event.data);
      try {
        if (message.type === "answer") {
          if (pc.signalingState === "have-local-offer") {
            await pc.setRemoteDescription(message.data);
            localOffer = false;
          }
          return;
        }
        if (message.type === "offer") {
          if (pc.signalingState !== "stable") {
            if (pc.signalingState === "have-local-offer") {
              await pc.setLocalDescription({ type: "rollback" });
              localOffer = false;
            } else return;
          }
          await pc.setRemoteDescription(message.data);
          await pc.setLocalDescription(await pc.createAnswer());
          send({ type: "answer", roomId, peerId, data: pc.localDescription });
          return;
        }
        if (message.type === "ice") {
          if (pc.remoteDescription) await pc.addIceCandidate(message.data);
          else pendingIce.push(message.data);
          return;
        }
        if (message.type === "error") state.errors.push(message.data?.code ?? message.error ?? "SFU_ERROR");
      } catch (error) {
        state.errors.push(error instanceof Error ? error.message : String(error));
      }
    };

    localOffer = true;
    await pc.setLocalDescription(await pc.createOffer());
    send({ type: "offer", roomId, peerId, data: pc.localDescription });

    const deadline = Date.now() + 20000;
    while (Date.now() < deadline) {
      const stats = await pc.getStats();
      let bytes = 0;
      stats.forEach(report => {
        if (report.type === "inbound-rtp" && report.kind === "video") bytes += Number(report.bytesReceived ?? 0);
      });
      state.inboundBytes = bytes;
      if (state.connection === "connected" && state.remoteTracks > 0 && bytes > 0) return state;
      await new Promise(resolve => setTimeout(resolve, 250));
    }
    throw new Error("media did not establish: " + JSON.stringify(state));
  }, {
    endpoint,
    token: accessToken(userId, role),
    roomId,
    peerId: userId
  });
}

async function waitDisconnected(page: Page) {
  await expect.poll(async () => page.evaluate(() => (window as any).__mediaE2E?.connection), {
    timeout: 15000,
    intervals: [250, 500, 1000]
  }).toMatch(/disconnected|failed|closed/);
}

async function closeClient(page: Page) {
  await page.evaluate(() => {
    const state = (window as any).__mediaE2E;
    if (state) state.closedByTest = true;
    document.querySelectorAll("video,audio").forEach(element => (element as HTMLMediaElement).srcObject = null);
  }).catch(() => {});
}

test.use({
  permissions: ["camera", "microphone"],
  launchOptions: {
    args: [
      "--use-fake-ui-for-media-stream",
      "--use-fake-device-for-media-stream"
    ]
  }
});

test("two browser clients publish/receive media and recover on SFU failover", async ({ browser }) => {
  test.setTimeout(120000);

  const contextA = await browser.newContext();
  const contextB = await browser.newContext();
  const pageA = await contextA.newPage();
  const pageB = await contextB.newPage();

  try {
    await waitHealth(primary);

    const [firstA, firstB] = await Promise.all([
      connectClient(pageA, primary, "media-user-a", "host"),
      connectClient(pageB, primary, "media-user-b", "member")
    ]);

    expect(firstA.connection).toBe("connected");
    expect(firstB.connection).toBe("connected");
    expect(firstA.remoteTracks).toBeGreaterThan(0);
    expect(firstB.remoteTracks).toBeGreaterThan(0);
    expect(firstA.inboundBytes).toBeGreaterThan(0);
    expect(firstB.inboundBytes).toBeGreaterThan(0);
    expect(firstA.errors).toEqual([]);
    expect(firstB.errors).toEqual([]);

    execFileSync("docker", ["compose", "-f", composeFile, "stop", "sfu-primary"], { stdio: "inherit" });
    await Promise.all([waitDisconnected(pageA), waitDisconnected(pageB)]);

    execFileSync("docker", ["compose", "-f", composeFile, "start", "sfu-secondary"], { stdio: "inherit" });
    await waitHealth(secondary);

    const [secondA, secondB] = await Promise.all([
      connectClient(pageA, secondary, "media-user-a", "host"),
      connectClient(pageB, secondary, "media-user-b", "member")
    ]);

    expect(secondA.connection).toBe("connected");
    expect(secondB.connection).toBe("connected");
    expect(secondA.remoteTracks).toBeGreaterThan(0);
    expect(secondB.remoteTracks).toBeGreaterThan(0);
    expect(secondA.inboundBytes).toBeGreaterThan(0);
    expect(secondB.inboundBytes).toBeGreaterThan(0);
    expect(secondA.errors).toEqual([]);
    expect(secondB.errors).toEqual([]);
  } finally {
    await closeClient(pageA);
    await closeClient(pageB);
    await contextA.close();
    await contextB.close();
  }
});
