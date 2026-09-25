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
  sessionId: string;
  replaced: boolean;
}

interface SignalMessage {
  type: "join" | "offer" | "answer" | "ice" | "leave";
  roomId: string;
  peerId?: string;
  data?: unknown;
}

interface ClusterSignal {
  sourceNode: string;
  type: "peer-joined" | "peer-left" | "peer-replaced" | "signal";
  roomId: string;
  peerId: string;
  targetPeerId?: string;
  targetNodeId?: string;
  targetSessionId?: string;
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

    if (event.type === "peer-replaced" && event.targetNodeId === NODE_ID && event.targetSessionId) {
      const target = [...room.values()].find(client => client.sessionId === event.targetSessionId && client.peerId === event.peerId);
      if (target) {
        target.replaced = true;
        send(target.socket, { type: "session-replaced", peerId: target.peerId });
        target.socket.close(4001, "session-replaced");
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
  if (!redisEnabled()) return null;
  await ensureRoomSubscription(client.roomId);
  const previous = await redisRegisterPeer(client.roomId, client.peerId, NODE_ID, client.sessionId, PEER_TTL_SECONDS);
  const timer = setInterval(() => {
    void redisRefreshPeer(client.roomId, client.peerId, NODE_ID, client.sessionId, PEER_TTL_SECONDS);
  }, Math.max(5000, Math.floor(PEER_TTL_SECONDS * 1000 / 2)));
  heartbeatTimers.set(client.sessionId, timer);
  return previous;
}

async function unregisterClient(client: Client): Promise<"removed" | "stale" | "unavailable"> {
  const timer = heartbeatTimers.get(client.sessionId);
  if (timer) clearInterval(timer);
  heartbeatTimers.delete(client.sessionId);
  if (!redisEnabled()) return "removed";
  const removal = await redisRemovePeer(client.roomId, client.peerId, NODE_ID, client.sessionId);
  const room = rooms.get(client.roomId);
  if (!room || room.size === 0) await redisUnsubscribe(redisRoomChannel(client.roomId));
  return removal;
}

export function attachSignaling(server: HttpServer) {
  const maxPayload = Math.max(1024, Math.min(256 * 1024, Number(process.env.WS_MAX_PAYLOAD ?? 64 * 1024) || 64 * 1024));
  const wss = new WebSocketServer({
    server,
    path: "/ws",
    maxPayload,
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
        const existing = room.get(peerId);
        if (existing) {
          existing.replaced = true;
          send(existing.socket, { type: "session-replaced", peerId });
          existing.socket.close(4001, "session-replaced");
        }

        client = { socket, roomId, peerId, sessionId: randomUUID(), replaced: false };
        room.set(peerId, client);

        void (async () => {
          const previous = await registerClient(client!);
          if (previous && previous.sessionId !== client!.sessionId) {
            await publishRoomEvent(roomId, {
              type: "peer-replaced",
              peerId,
              targetNodeId: previous.nodeId,
              targetSessionId: previous.sessionId
            });
          }
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
      if (current?.get(client.peerId) !== client) {
        void unregisterClient(client);
        return;
      }

      current.delete(client.peerId);
      if (current.size === 0) rooms.delete(client.roomId);
      void unregisterClient(client).then(status => {
        if (client.replaced || status === "stale") return;
        for (const other of current.values()) send(other.socket, { type: "peer-left", peerId: client.peerId });
        void publishRoomEvent(client.roomId, { type: "peer-left", peerId: client.peerId });
      });
    });
  });

  return wss;
}