import { createHmac } from "node:crypto";
import { test, expect } from "@playwright/test";

const primary = process.env.SFU_PRIMARY_URL ?? "http://127.0.0.1:4100";
const rooms = Math.min(8, Math.max(2, Number(process.env.SFU_CONCURRENT_ROOMS ?? 4)));
const usersPerRoom = Math.min(6, Math.max(2, Number(process.env.SFU_USERS_PER_ROOM ?? 3)));
const secret = process.env.ROOM_ACCESS_SECRET ?? "integration-secret";

function token(roomId: string, userId: string) {
  const payload = Buffer.from(JSON.stringify({
    roomId, userId, role: userId.endsWith("-1") ? "host" : "participant",
    exp: Math.floor(Date.now() / 1000) + 600
  })).toString("base64url");
  return payload + "." + createHmac("sha256", secret).update(payload).digest("base64url");
}

async function join(page: import("@playwright/test").Page, roomId: string, userId: string) {
  return page.evaluate(async ({ endpoint, roomId, accessToken }) => {
    const stream = await navigator.mediaDevices.getUserMedia({ video: true, audio: false });
    const pc = new RTCPeerConnection();
    let remoteTracks = 0;
    pc.ontrack = () => remoteTracks++;
    for (const track of stream.getTracks()) pc.addTrack(track, stream);
    const ws = new WebSocket(endpoint.replace(/^http/, "ws") + "/ws");
    const messages: any[] = [];
    let resolveMessage: ((m: any) => void) | undefined;
    ws.onmessage = event => {
      const message = JSON.parse(event.data);
      if (resolveMessage) { const resolve = resolveMessage; resolveMessage = undefined; resolve(message); }
      else messages.push(message);
    };
    const next = (type: string) => new Promise<any>((resolve, reject) => {
      const take = (m: any) => {
        if (m.type === type) resolve(m);
        else resolveMessage = take;
      };
      const queued = messages.findIndex(m => m.type === type);
      if (queued >= 0) resolve(messages.splice(queued, 1)[0]);
      else {
        resolveMessage = take;
        setTimeout(() => reject(new Error("timeout: " + type)), 15000);
      }
    });
    await new Promise<void>((resolve, reject) => {
      ws.onopen = () => resolve();
      ws.onerror = () => reject(new Error("websocket failed"));
    });
    ws.send(JSON.stringify({ type: "join", roomId, data: { accessToken, reconnect: false } }));
    const joined = await next("joined");
    const offer = await pc.createOffer();
    await pc.setLocalDescription(offer);
    ws.send(JSON.stringify({ type: "offer", roomId, data: pc.localDescription }));
    await next("answer");
    expect(["connected", "connecting", "completed"].includes(pc.connectionState) || ["connected", "completed"].includes(pc.iceConnectionState)).toBeTruthy();
    await new Promise(resolve => setTimeout(resolve, 800));
    return { peerId: joined.peerId, remoteTracks, peers: joined.data?.peers ?? [] };
  }, { endpoint: primary, roomId, accessToken: token(roomId, userId) });
}

test("concurrent rooms isolate peer state and remote media", async ({ browser }) => {
  test.skip(!process.env.SFU_FAILOVER_LIVE, "Set SFU_FAILOVER_LIVE=1 for the live Docker run");

  const pages = await Promise.all(
    Array.from({ length: rooms * usersPerRoom }, () =>
      browser.newPage({ permissions: ["camera", "microphone"] })
    )
  );
  await Promise.all(pages.map(page => page.goto(primary + "/health")));

  const all = await Promise.all(pages.map(async (page, index) => {
    const roomIndex = Math.floor(index / usersPerRoom);
    const userIndex = (index % usersPerRoom) + 1;
    return join(page, `CONCURRENT-${roomIndex + 1}`, `r${roomIndex + 1}-${userIndex}`);
  }));

  for (let roomIndex = 0; roomIndex < rooms; roomIndex++) {
    const room = all.slice(roomIndex * usersPerRoom, (roomIndex + 1) * usersPerRoom);
    expect(new Set(room.map(item => item.peerId)).size).toBe(usersPerRoom);
    for (let i = 1; i < room.length; i++) {
      expect(room[i].peers).toContain(room[0].peerId);
      expect(room[i].remoteTracks).toBeGreaterThan(0);
    }
  }

  const crossRoomPeerIds = new Set<string>();
  for (let roomIndex = 0; roomIndex < rooms; roomIndex++) {
    for (const item of all.slice(roomIndex * usersPerRoom, (roomIndex + 1) * usersPerRoom)) {
      expect(crossRoomPeerIds.has(item.peerId)).toBeFalsy();
      crossRoomPeerIds.add(item.peerId);
    }
  }
  expect(crossRoomPeerIds.size).toBe(rooms * usersPerRoom);
  await Promise.all(pages.map(page => page.close()));
});
