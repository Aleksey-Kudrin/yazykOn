import type { Server as HttpServer } from "node:http";
import { randomUUID } from "node:crypto";
import { WebSocketServer, WebSocket } from "ws";
import {
  redisEnabled,
  redisListPeers,
  redisRegisterPeer,
  redisRefreshPeer,
  redisRemovePeer,
  redisRoomChannel,
  redisSubscribe,
  redisUnsubscribe,
  redisPublish
} from "./redis.js";

function allowedOrigin(origin: string | undefined) {
  if (!origin) return true;
  const origins = (process.env.WEB_ORIGINS ?? "").split(",").map(value => value.trim()).filter(Boolean);
  return origins.length > 0 && origins.includes(origin);
}

interface Client {
  socket: WebSocket;
  roomId: string;
  peerId: string;
}

interface SignalMessage {
  type: "join" | "offer" | "answer" | "ice" | "leave";
  roomId: string;
  peerId?: string;
  data?: unknown;
}

interface ClusterSignal {
  sourceNode: string;
  type: "peer-joined" | "peer-left" | "signal";
  roomId: string;
  peerId: string;
  targetPeerId?: string;
  data?: unknown;
}

const rooms = new Map<string, Map<string, Client>>();
const NODE_ID = process.env.INSTANCE_ID || `${process.env.HOSTNAME || "node"}-${randomUUID().slice(0, 8)}`;
const PEER_TTL_SECONDS = Math.max(15, Number(process.env.SIGNALING_PEER_TTL ?? 60) || 60);
const heartbeatTimers = new Map<string, ReturnType<typeof setInterval>>();

function send(socket: WebSocket, message: unknown) {
  if (socket.readyState === WebSocket.OPEN) socket.send(JSON.stringify(message));
}

function localRoom(roomId: string) {
  let room = rooms.get(roomId);
  if (!room) {
    room = new Map();
    rooms.set(roomId, room);
  }
  return room;
}

async function ensureRoomSubscription(roomId: string) {
  if (!redisEnabled()) return;
  await redisSubscribe(redisRoomChannel(roomId), payload => {
    const event = payload as ClusterSignal;
    if (!event || event.sourceNode === NODE_ID || event.roomId !== roomId) return;
    const room = rooms.get(roomId);
    if (!room) return;

    if (event.type === "peer-joined") {
      for (const client of room.values()) {
        if (client.peerId !== event.peerId) send(client.socket, { type: "peer-joined", peerId: event.peerId });
      }
      return;
    }

    if (event.type === "peer-left") {
      for (const client of room.values()) {
        if (client.peerId !== event.peerId) send(client.socket, { type: "peer-left", peerId: event.peerId });
      }
      return;
    }

    if (event.type === "signal" && event.targetPeerId) {
      const target = room.get(event.targetPeerId);
      if (target) send(target.socket, { type: event.data && (event.data as { signalType?: string }).signalType || "error", peerId: event.peerId, data: event.data && (event.data as { payload?: unknown }).payload });
    }
  });
}

async function publishRoomEvent(roomId: string, event: Omit<ClusterSignal, "sourceNode" | "roomId">) {
  if (!redisEnabled()) return;
  await redisPublish(redisRoomChannel(roomId), { ...event, roomId, sourceNode: NODE_ID });
}

async function registerClient(client: Client) {
  if (!redisEnabled()) return;
  await ensureRoomSubscription(client.roomId);
  await redisRegisterPeer(client.roomId, client.peerId, NODE_ID, PEER_TTL_SECONDS);
  const timer = setInterval(() => {
    void redisRefreshPeer(client.roomId, client.peerId, NODE_ID, PEER_TTL_SECONDS);
  }, Math.max(5000, Math.floor(PEER_TTL_SECONDS * 1000 / 2)));
  heartbeatTimers.set(client.peerId, timer);
}

async function unregisterClient(client: Client) {
  const timer = heartbeatTimers.get(client.peerId);
  if (timer) clearInterval(timer);
  heartbeatTimers.delete(client.peerId);
  if (!redisEnabled()) return;
  await redisRemovePeer(client.roomId, client.peerId, NODE_ID);
  const room = rooms.get(client.roomId);
  if (!room || room.size === 0) await redisUnsubscribe(redisRoomChannel(client.roomId));
}

export function attachSignaling(server: HttpServer) {
  const wss = new WebSocketServer({
    server,
    path: "/ws",
    verifyClient: (info: { origin: string; secure: boolean; req: import("node:http").IncomingMessage }) => allowedOrigin(info.origin)
  });

  wss.on("connection", socket => {
    let client: Client | undefined;

    socket.on("message", raw => {
      let message: SignalMessage;
      try {
        message = JSON.parse(raw.toString()) as SignalMessage;
      } catch {
        send(socket, { type: "error", error: "INVALID_JSON" });
        return;
      }

      if (!message.roomId || !message.type) {
        send(socket, { type: "error", error: "INVALID_MESSAGE" });
        return;
      }

      if (message.type === "join") {
        if (client) return;
        const roomId = message.roomId.toUpperCase();
        const peerId = message.peerId || randomUUID();
        const room = localRoom(roomId);
        if (room.has(peerId)) {
          send(socket, { type: "error", error: "PEER_ID_IN_USE" });
          return;
        }

        client = { socket, roomId, peerId };
        room.set(peerId, client);

        void (async () => {
          await registerClient(client!);
          const clusterPeers = redisEnabled() ? await redisListPeers(roomId) : [];
          const existingPeers = new Set([...room.keys()]);
          for (const peer of clusterPeers) existingPeers.add(peer.peerId);
          existingPeers.delete(peerId);

          send(socket, { type: "joined", roomId, peerId, peers: [...existingPeers] });
          for (const other of room.values()) {
            if (other.peerId !== peerId) send(other.socket, { type: "peer-joined", peerId });
          }
          await publishRoomEvent(roomId, { type: "peer-joined", peerId });
        })().catch(error => {
          console.error("signaling join failed", error);
          send(socket, { type: "error", error: "SIGNALING_JOIN_FAILED" });
        });
        return;
      }

      if (!client) {
        send(socket, { type: "error", error: "NOT_JOINED" });
        return;
      }

      const room = rooms.get(client.roomId);
      if (!room) return;

      if (message.type === "leave") {
        socket.close();
        return;
      }

      if (message.type === "offer" || message.type === "answer" || message.type === "ice") {
        if (!message.peerId) return;
        const target = room.get(message.peerId);
        if (target) {
          send(target.socket, { type: message.type, peerId: client.peerId, data: message.data });
        } else if (redisEnabled()) {
          void publishRoomEvent(client.roomId, {
            type: "signal",
            peerId: client.peerId,
            targetPeerId: message.peerId,
            data: { signalType: message.type, payload: message.data }
          });
        }
      }
    });

    socket.on("close", () => {
      if (!client) return;
      const current = rooms.get(client.roomId);
      if (current) {
        current.delete(client.peerId);
        for (const other of current.values()) send(other.socket, { type: "peer-left", peerId: client.peerId });
        if (current.size === 0) rooms.delete(client.roomId);
      }
      void publishRoomEvent(client.roomId, { type: "peer-left", peerId: client.peerId })
        .finally(() => unregisterClient(client!));
    });
  });

  return wss;
}