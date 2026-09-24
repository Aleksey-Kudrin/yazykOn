import { createHmac, randomBytes } from "node:crypto";
import cors from "cors";
import express from "express";
import helmet from "helmet";
import { createServer } from "node:http";
import { attachSignaling } from "./signaling.js";
import { createRoom, getRoom, verifyRoomPassword } from "./rooms.js";

const app = express();
const port = Number(process.env.PORT ?? 3000);
const roomAccessSecret = process.env.ROOM_ACCESS_SECRET ?? "";
const roomAccessTtlValue = Number(process.env.ROOM_ACCESS_TTL ?? 86400);
const roomAccessTtl = Number.isFinite(roomAccessTtlValue) ? Math.max(300, roomAccessTtlValue) : 86400;

function issueRoomAccessToken(roomId: string): string | null {
  if (!roomAccessSecret) return null;
  const expires = Math.floor(Date.now() / 1000) + roomAccessTtl;
  const payload = Buffer.from(JSON.stringify({ roomId, exp: expires })).toString("base64url");
  const signature = createHmac("sha256", roomAccessSecret).update(payload).digest("base64url");
  return payload + "." + signature;
}

app.use(helmet());
app.use(cors());
app.use(express.json());

app.get("/api/health", (_req, res) => {
  res.json({
    ok: true,
    service: "yazykOn-server",
    version: "0.2.0",
    signaling: "/ws",
    time: new Date().toISOString()
  });
});

app.get("/api/turn-credentials", (_req, res) => {
  const secret = process.env.TURN_SECRET;
  const turnUrl = process.env.TURN_URL;
  const realm = process.env.TURN_REALM ?? "yazykon.local";
  if (!secret || !turnUrl) {
    res.json({ enabled: false });
    return;
  }

  const parsedTtl = Number(process.env.TURN_CREDENTIAL_TTL ?? 3600);
  const ttl = Number.isFinite(parsedTtl) ? Math.max(60, parsedTtl) : 3600;
  const expires = Math.floor(Date.now() / 1000) + ttl;
  const username = `${expires}:${randomBytes(12).toString("hex")}`;
  const credential = createHmac("sha1", secret).update(username).digest("base64");
  res.json({ enabled: true, urls: turnUrl, username, credential, realm, ttl });
});

app.get("/api", (_req, res) => {
  res.json({ name: "языкOn", message: "Backend API is running" });
});

app.post("/api/rooms", (req, res) => {
  const name =
    typeof req.body?.name === "string" && req.body.name.trim()
      ? req.body.name.trim().slice(0, 100)
      : "Новая конференция";
  const password = typeof req.body?.password === "string" ? req.body.password.trim() : "";
  if (password && !roomAccessSecret) {
    res.status(503).json({ error: "ROOM_ACCESS_NOT_CONFIGURED" });
    return;
  }
  if (password && (password.length < 4 || password.length > 128)) {
    res.status(400).json({ error: "INVALID_ROOM_PASSWORD" });
    return;
  }
  const room = createRoom(name, password);
  const accessToken = issueRoomAccessToken(room.id);
  res.status(201).json({ ...room, accessToken });
});

app.post("/api/rooms/:id/access", (req, res) => {
  const room = getRoom(req.params.id);
  if (!room) { res.status(404).json({ error: "ROOM_NOT_FOUND" }); return; }
  if (!room.requiresPassword) {
    res.json({ accessToken: issueRoomAccessToken(room.id) });
    return;
  }
  const password = typeof req.body?.password === "string" ? req.body.password : "";
  if (!verifyRoomPassword(room.id, password)) {
    res.status(401).json({ error: "INVALID_ROOM_PASSWORD" });
    return;
  }
  const accessToken = issueRoomAccessToken(room.id);
  if (!accessToken) { res.status(503).json({ error: "ROOM_ACCESS_NOT_CONFIGURED" }); return; }
  res.json({ accessToken });
});

app.get("/api/rooms/:id", (req, res) => {
  const room = getRoom(req.params.id);

  if (!room) {
    res.status(404).json({ error: "ROOM_NOT_FOUND" });
    return;
  }

  res.json(room);
});

const server = createServer(app);
attachSignaling(server);

server.listen(port, "0.0.0.0", () => {
  console.log(`языкOn server listening on http://0.0.0.0:${port}`);
  console.log(`языкOn signaling listening on ws://0.0.0.0:${port}/ws`);
});
