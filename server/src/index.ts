import { createHmac, randomBytes } from "node:crypto";
import cors from "cors";
import express from "express";
import helmet from "helmet";
import { createServer } from "node:http";
import { attachSignaling } from "./signaling.js";
import { createRoom, getRoom } from "./rooms.js";

const app = express();
const port = Number(process.env.PORT ?? 3000);

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

  const ttl = Math.max(60, Number(process.env.TURN_CREDENTIAL_TTL ?? 3600));
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

  res.status(201).json(createRoom(name));
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
