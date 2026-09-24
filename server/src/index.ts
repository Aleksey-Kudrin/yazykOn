import { createHmac, randomBytes, scryptSync, timingSafeEqual } from "node:crypto";
import cors from "cors";
import express from "express";
import helmet from "helmet";
import { createServer } from "node:http";
import { randomUUID } from "node:crypto";
import { Pool } from "pg";
import { attachSignaling } from "./signaling.js";
import { createRoom, getRoom, verifyRoomPassword } from "./rooms.js";

const app = express();
const port = Number(process.env.PORT ?? 3000);
const roomAccessSecret = process.env.ROOM_ACCESS_SECRET ?? "";
const roomAccessTtlValue = Number(process.env.ROOM_ACCESS_TTL ?? 86400);
const roomAccessTtl = Number.isFinite(roomAccessTtlValue) ? Math.max(300, roomAccessTtlValue) : 86400;
const databaseUrl = process.env.DATABASE_URL ?? "";
const db = databaseUrl ? new Pool({ connectionString: databaseUrl }) : null;

async function initDatabase() {
  if (!db) return;
  await db.query(`CREATE TABLE IF NOT EXISTS users (
    id UUID PRIMARY KEY,
    username TEXT NOT NULL UNIQUE,
    password_hash TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
  );
  CREATE TABLE IF NOT EXISTS sessions (
    id UUID PRIMARY KEY,
    user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    expires_at TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
  );
  CREATE TABLE IF NOT EXISTS rooms (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    owner_id UUID REFERENCES users(id) ON DELETE SET NULL,
    password_hash TEXT,
    password_salt TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
  );
  CREATE INDEX IF NOT EXISTS sessions_user_id_idx ON sessions(user_id);
  CREATE INDEX IF NOT EXISTS sessions_expires_at_idx ON sessions(expires_at);`);
}

function authCookie(sessionId: string) {
  return `yazykon_session=${sessionId}; Path=/; HttpOnly; SameSite=Lax; Max-Age=604800`;
}

function readSessionId(req: express.Request) {
  const raw = req.headers.cookie ?? "";
  return raw.split(";").map(v => v.trim()).find(v => v.startsWith("yazykon_session="))?.slice("yazykon_session=".length) ?? "";
}

async function currentUser(req: express.Request) {
  if (!db) return null;
  const sessionId = readSessionId(req);
  if (!sessionId) return null;
  const result = await db.query(
    "SELECT u.id, u.username FROM sessions s JOIN users u ON u.id=s.user_id WHERE s.id=$1 AND s.expires_at > now()",
    [sessionId]
  );
  return result.rows[0] ?? null;
}

function requireScrypt(password: string, salt: string) { return scryptSync(password, salt, 32).toString("hex"); }
function verifyScrypt(password: string, stored: string) { const [salt, expectedHex] = stored.split(":"); if (!salt || !expectedHex) return false; const actual=Buffer.from(requireScrypt(password,salt),"hex"); const expected=Buffer.from(expectedHex,"hex"); return actual.length===expected.length && timingSafeEqual(actual,expected); }

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

app.post("/api/auth/register", async (req, res) => {
  if (!db) { res.status(503).json({ error: "DATABASE_NOT_CONFIGURED" }); return; }
  const username = typeof req.body?.username === "string" ? req.body.username.trim().toLowerCase() : "";
  const password = typeof req.body?.password === "string" ? req.body.password : "";
  if (!/^[a-z0-9_.-]{3,32}$/.test(username) || password.length < 8 || password.length > 128) {
    res.status(400).json({ error: "INVALID_CREDENTIALS" }); return;
  }
  try {
    const salt = randomBytes(16).toString("hex");
    const hash = requireScrypt(password, salt);
    const userId = randomUUID();
    await db.query("INSERT INTO users(id, username, password_hash) VALUES($1,$2,$3)", [userId, username, `${salt}:${hash}`]);
    const sessionId = randomUUID();
    await db.query("INSERT INTO sessions(id,user_id,expires_at) VALUES($1,$2,now()+interval '7 days')", [sessionId,userId]);
    res.setHeader("Set-Cookie", authCookie(sessionId));
    res.status(201).json({ id:userId, username });
  } catch {
    res.status(409).json({ error: "USERNAME_TAKEN" });
  }
});

app.post("/api/auth/login", async (req, res) => {
  if (!db) { res.status(503).json({ error: "DATABASE_NOT_CONFIGURED" }); return; }
  const username = typeof req.body?.username === "string" ? req.body.username.trim().toLowerCase() : "";
  const password = typeof req.body?.password === "string" ? req.body.password : "";
  const result = await db.query("SELECT id, username, password_hash FROM users WHERE username=$1", [username]);
  const row = result.rows[0];
  if (!row || !verifyScrypt(password, row.password_hash)) { res.status(401).json({ error:"INVALID_CREDENTIALS" }); return; }
  const sessionId = randomUUID();
  await db.query("INSERT INTO sessions(id,user_id,expires_at) VALUES($1,$2,now()+interval '7 days')", [sessionId,row.id]);
  res.setHeader("Set-Cookie", authCookie(sessionId));
  res.json({ id:row.id, username:row.username });
});

app.post("/api/auth/logout", async (req, res) => {
  if (db) { const id=readSessionId(req); if(id) await db.query("DELETE FROM sessions WHERE id=$1",[id]); }
  res.setHeader("Set-Cookie","yazykon_session=; Path=/; HttpOnly; SameSite=Lax; Max-Age=0");
  res.status(204).end();
});

app.get("/api/auth/me", async (req, res) => {
  const user = await currentUser(req);
  res.json({ user });
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

initDatabase().then(() => server.listen(port, "0.0.0.0", () => {
  console.log(`языкOn server listening on http://0.0.0.0:${port}`);
  console.log(`языкOn signaling listening on ws://0.0.0.0:${port}/ws`);
});
