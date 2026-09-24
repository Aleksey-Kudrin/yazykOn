import assert from "node:assert/strict";
import { createServer } from "node:http";
import test from "node:test";
import WebSocket from "ws";
import { attachSignaling } from "./signaling.js";

function connect(port: number, roomId: string, peerId: string) {
  const socket = new WebSocket(`ws://127.0.0.1:${port}/ws`);
  const messages: Array<Record<string, unknown>> = [];
  const waiters: Array<(message: Record<string, unknown>) => void> = [];

  socket.on("message", raw => {
    const message = JSON.parse(raw.toString()) as Record<string, unknown>;
    messages.push(message);
    while (waiters.length) waiters.shift()!(message);
  });

  const nextMessage = (type: string, timeoutMs = 2000) => new Promise<Record<string, unknown>>((resolve, reject) => {
    const existing = messages.find(message => message.type === type);
    if (existing) {
      resolve(existing);
      return;
    }
    const timer = setTimeout(() => reject(new Error(`Timed out waiting for ${type}`)), timeoutMs);
    waiters.push(message => {
      if (message.type === type) {
        clearTimeout(timer);
        resolve(message);
      }
    });
  });

  const opened = new Promise<void>((resolve, reject) => {
    socket.once("open", () => resolve());
    socket.once("error", reject);
  });

  return { socket, opened, nextMessage };
}

async function closeServer(server: ReturnType<typeof createServer>) {
  await new Promise<void>(resolve => server.close(() => resolve()));
}

test("duplicate peer id replaces the old local session without emitting a stale peer-left", async () => {
  const server = createServer();
  attachSignaling(server);
  await new Promise<void>(resolve => server.listen(0, "127.0.0.1", () => resolve()));
  const address = server.address();
  assert.ok(address && typeof address !== "string");

  const first = connect(address.port, "reconnect-room", "peer-a");
  await first.opened;
  first.socket.send(JSON.stringify({ type: "join", roomId: "reconnect-room", peerId: "peer-a" }));
  const firstJoined = await first.nextMessage("joined");
  assert.equal(firstJoined.peerId, "peer-a");

  const second = connect(address.port, "reconnect-room", "peer-a");
  await second.opened;
  second.socket.send(JSON.stringify({ type: "join", roomId: "reconnect-room", peerId: "peer-a" }));
  const secondJoined = await second.nextMessage("joined");
  assert.equal(secondJoined.peerId, "peer-a");

  const replaced = await first.nextMessage("session-replaced");
  assert.equal(replaced.peerId, "peer-a");

  await new Promise<void>(resolve => first.socket.once("close", () => resolve()));
  assert.equal(second.socket.readyState, WebSocket.OPEN);

  const observer = connect(address.port, "reconnect-room", "peer-b");
  await observer.opened;
  observer.socket.send(JSON.stringify({ type: "join", roomId: "reconnect-room", peerId: "peer-b" }));
  const observerJoined = await observer.nextMessage("joined");
  assert.deepEqual(observerJoined.peers, ["peer-a"]);

  observer.socket.close();
  second.socket.close();
  await new Promise(resolve => setTimeout(resolve, 25));
  await closeServer(server);
});
