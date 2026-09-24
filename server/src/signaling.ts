import type { Server as HttpServer } from "node:http";
import { randomUUID } from "node:crypto";
import { WebSocketServer, WebSocket } from "ws";

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

const rooms = new Map<string, Map<string, Client>>();

function send(socket: WebSocket, message: unknown) {
  if (socket.readyState === WebSocket.OPEN) {
    socket.send(JSON.stringify(message));
  }
}

export function attachSignaling(server: HttpServer) {
  const wss = new WebSocketServer({
    server,
    path: "/ws",
    verifyClient: (info: { origin: string; secure: boolean; req: import("node:http").IncomingMessage }) => allowedOrigin(info.origin)
  });

  wss.on("connection", (socket) => {
    let client: Client | undefined;

    socket.on("message", (raw) => {
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

        let room = rooms.get(roomId);
        if (!room) {
          room = new Map();
          rooms.set(roomId, room);
        }

        const existingPeers = [...room.keys()];
        client = { socket, roomId, peerId };
        room.set(peerId, client);

        send(socket, { type: "joined", roomId, peerId, peers: existingPeers });

        for (const other of room.values()) {
          if (other.peerId !== peerId) {
            send(other.socket, { type: "peer-joined", peerId });
          }
        }
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
        if (!target) return;

        send(target.socket, {
          type: message.type,
          peerId: client.peerId,
          data: message.data
        });
      }
    });

    socket.on("close", () => {
      if (!client) return;

      const room = rooms.get(client.roomId);
      if (!room) return;

      room.delete(client.peerId);

      for (const other of room.values()) {
        send(other.socket, { type: "peer-left", peerId: client.peerId });
      }

      if (room.size === 0) rooms.delete(client.roomId);
    });
  });

  return wss;
}
