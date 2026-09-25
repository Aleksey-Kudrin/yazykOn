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

test("join acknowledges from local state before asynchronous cluster reconciliation", async () => {
  const server = createServer();
  attachSignaling(server);
  await new Promise<void>(resolve => server.listen(0, "127.0.0.1", () => resolve()));
  const address = server.address();
  assert.ok(address && typeof address !== "string");

  const first = connect(address.port, "fast-join-room", "peer-a");
  await first.opened;
  first.socket.send(JSON.stringify({ type: "join", roomId: "fast-join-room", peerId: "peer-a" }));
  const firstJoined = await first.nextMessage("joined");
  assert.equal(firstJoined.peerId, "peer-a");
  assert.deepEqual(firstJoined.peers, []);

  const second = connect(address.port, "fast-join-room", "peer-b");
  await second.opened;
  second.socket.send(JSON.stringify({ type: "join", roomId: "fast-join-room", peerId: "peer-b" }));
  const secondJoined = await second.nextMessage("joined");
  assert.equal(secondJoined.peerId, "peer-b");
  assert.deepEqual(secondJoined.peers, ["peer-a"]);

  second.socket.close();
  first.socket.close();
  await new Promise(resolve => setTimeout(resolve, 25));
  await closeServer(server);
});

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


test("duplicate reconnect keeps the newest local session after stale old close", async () => {
  const server = createServer();
  attachSignaling(server);
  await new Promise<void>(resolve => server.listen(0, "127.0.0.1", () => resolve()));
  const address = server.address();
  assert.ok(address && typeof address !== "string");

  const first = connect(address.port, "stale-close-room", "peer-a");
  await first.opened;
  first.socket.send(JSON.stringify({ type: "join", roomId: "stale-close-room", peerId: "peer-a" }));
  await first.nextMessage("joined");

  const second = connect(address.port, "stale-close-room", "peer-a");
  await second.opened;
  second.socket.send(JSON.stringify({ type: "join", roomId: "stale-close-room", peerId: "peer-a" }));
  await second.nextMessage("joined");
  await first.nextMessage("session-replaced");

  await new Promise(resolve => setTimeout(resolve, 25));
  assert.equal(second.socket.readyState, WebSocket.OPEN);

  const observer = connect(address.port, "stale-close-room", "peer-b");
  await observer.opened;
  observer.socket.send(JSON.stringify({ type: "join", roomId: "stale-close-room", peerId: "peer-b" }));
  const joined = await observer.nextMessage("joined");
  assert.deepEqual(joined.peers, ["peer-a"]);

  observer.socket.close();
  second.socket.close();
  await new Promise(resolve => setTimeout(resolve, 25));
  await closeServer(server);
});


test("rapid A-B-A reconnect keeps exactly one live peer", async () => {
  const server = createServer();
  attachSignaling(server);
  await new Promise<void>(resolve => server.listen(0, "127.0.0.1", () => resolve()));
  const address = server.address();
  assert.ok(address && typeof address !== "string");

  const first = connect(address.port, "rapid-reconnect-room", "peer-a");
  await first.opened;
  first.socket.send(JSON.stringify({ type: "join", roomId: "rapid-reconnect-room", peerId: "peer-a" }));
  await first.nextMessage("joined");

  const second = connect(address.port, "rapid-reconnect-room", "peer-a");
  await second.opened;
  second.socket.send(JSON.stringify({ type: "join", roomId: "rapid-reconnect-room", peerId: "peer-a" }));
  await second.nextMessage("joined");
  await first.nextMessage("session-replaced");

  const third = connect(address.port, "rapid-reconnect-room", "peer-a");
  await third.opened;
  third.socket.send(JSON.stringify({ type: "join", roomId: "rapid-reconnect-room", peerId: "peer-a" }));
  await third.nextMessage("joined");
  await second.nextMessage("session-replaced");

  await new Promise(resolve => setTimeout(resolve, 25));
  assert.equal(first.socket.readyState, WebSocket.CLOSED);
  assert.equal(second.socket.readyState, WebSocket.CLOSED);
  assert.equal(third.socket.readyState, WebSocket.OPEN);

  const observer = connect(address.port, "rapid-reconnect-room", "peer-b");
  await observer.opened;
  observer.socket.send(JSON.stringify({ type: "join", roomId: "rapid-reconnect-room", peerId: "peer-b" }));
  const joined = await observer.nextMessage("joined");
  assert.deepEqual(joined.peers, ["peer-a"]);

  observer.socket.close();
  third.socket.close();
  await new Promise(resolve => setTimeout(resolve, 25));
  await closeServer(server);
});
