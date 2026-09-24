import cors from "cors";
import express from "express";
import helmet from "helmet";
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
    version: "0.1.0",
    time: new Date().toISOString()
  });
});

app.get("/api", (_req, res) => {
  res.json({
    name: "языкOn",
    message: "Backend API is running"
  });
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

app.listen(port, "0.0.0.0", () => {
  console.log(`языкOn server listening on http://0.0.0.0:${port}`);
});
