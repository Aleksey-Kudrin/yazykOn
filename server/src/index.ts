import { createHmac, randomBytes, scryptSync, timingSafeEqual, randomUUID } from "node:crypto";
import cors from "cors";
import express from "express";
import helmet from "helmet";
import { createServer } from "node:http";
import { Pool } from "pg";
import { attachSignaling } from "./signaling.js";
import { closeRedis, getRedis, redisEnabled } from "./redis.js";
import { BreakoutManager } from "./breakout/manager.js";

type RoomRole = "host" | "cohost" | "member";

const app = express();
const port = Number(process.env.PORT ?? 3000);
const roomAccessSecret = process.env.ROOM_ACCESS_SECRET ?? "";
const roomAccessTtlValue = Number(process.env.ROOM_ACCESS_TTL ?? 86400);
const roomAccessTtl = Number.isFinite(roomAccessTtlValue) ? Math.max(300, roomAccessTtlValue) : 86400;
const databaseUrl = process.env.DATABASE_URL ?? "";
const mediaControlUrl = process.env.MEDIA_CONTROL_URL ?? "http://media:4000";
const mediaControlSecret = process.env.MEDIA_CONTROL_SECRET ?? "";
const allowedOrigins = new Set(
  (process.env.WEB_ORIGINS ?? "")
    .split(",")
    .map(value => value.trim())
    .filter(Boolean)
);
const authRate = new Map<string, { count: number; resetAt: number }>();
const roomRate = new Map<string, { count: number; resetAt: number }>();
const breakoutManager = new BreakoutManager();

function requestIp(req: express.Request) {
  return req.socket.remoteAddress ?? "unknown";
}

function rateLimit(store: Map<string, { count: number; resetAt: number }>, limit: number, windowMs: number) {
  return (req: express.Request, res: express.Response, next: express.NextFunction) => {
    const now = Date.now();
    const key = requestIp(req);
    const current = store.get(key);
    if (!current || current.resetAt <= now) {
      store.set(key, { count: 1, resetAt: now + windowMs });
      next();
      return;
    }
    if (current.count >= limit) {
      res.status(429).json({ error: "RATE_LIMITED" });
      return;
    }
    current.count++;
    next();
  };
}

function isAllowedOrigin(origin: string | undefined) {
  if (!origin) return true;
  if (allowedOrigins.size === 0 && process.env.NODE_ENV !== "production") return true;
  return allowedOrigins.has(origin);
}
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
  CREATE INDEX IF NOT EXISTS sessions_expires_at_idx ON sessions(expires_at);
  CREATE TABLE IF NOT EXISTS room_members (
    room_id TEXT NOT NULL REFERENCES rooms(id) ON DELETE CASCADE,
    user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    role TEXT NOT NULL CHECK (role IN ('host','cohost','member')),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (room_id, user_id)
  );
  CREATE INDEX IF NOT EXISTS room_members_user_id_idx ON room_members(user_id);
  CREATE TABLE IF NOT EXISTS room_messages (
    id UUID PRIMARY KEY,
    room_id TEXT NOT NULL REFERENCES rooms(id) ON DELETE CASCADE,
    user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    text TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
  );
  CREATE INDEX IF NOT EXISTS room_messages_room_created_idx ON room_messages(room_id, created_at);
  CREATE TABLE IF NOT EXISTS audit_events (
    id UUID PRIMARY KEY,
    user_id UUID REFERENCES users(id) ON DELETE SET NULL,
    room_id TEXT,
    action TEXT NOT NULL,
    target_user_id UUID,
    ip TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
  );
  CREATE INDEX IF NOT EXISTS audit_events_room_created_idx ON audit_events(room_id, created_at);`);
}

function authCookie(sessionId: string) {
  const secure = process.env.NODE_ENV === "production" ? "; Secure" : "";
  return `yazykon_session=${sessionId}; Path=/; HttpOnly; SameSite=Lax; Max-Age=604800${secure}`;
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

function issueRoomAccessToken(roomId: string, userId: string, role: RoomRole): string | null {
  if (!roomAccessSecret) return null;
  const expires = Math.floor(Date.now() / 1000) + roomAccessTtl;
  const payload = Buffer.from(JSON.stringify({ roomId, userId, role, exp: expires })).toString("base64url");
  const signature = createHmac("sha256", roomAccessSecret).update(payload).digest("base64url");
  return payload + "." + signature;
}

app.use(helmet());
app.use(cors({
  origin: (origin, callback) => {
    if (isAllowedOrigin(origin)) callback(null, true);
    else callback(null, false);
  },
  credentials: true
}));
app.use(express.json({ limit: "32kb" }));
app.use("/api/auth", rateLimit(authRate, 30, 60_000));
app.use("/api/rooms", rateLimit(roomRate, 60, 60_000));

app.get("/health", (_req, res) => res.json({ ok: true, service: "yazykOn-server" }));
app.get("/ready", async (_req, res) => {
  if (!db) { res.status(503).json({ ok: false, database: false }); return; }
  try {
    await db.query("SELECT 1");
    const redis = redisEnabled() ? await getRedis() : null;
    if (redisEnabled() && !redis) { res.status(503).json({ ok: false, database: true, redis: false }); return; }
    res.json({ ok: true, database: true, redis: redisEnabled() ? true : null });
  } catch {
    res.status(503).json({ ok: false, database: false });
  }
});

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
  const secure = process.env.NODE_ENV === "production" ? "; Secure" : "";
  res.setHeader("Set-Cookie",`yazykon_session=; Path=/; HttpOnly; SameSite=Lax; Max-Age=0${secure}`);
  res.status(204).end();
});

app.get("/api/auth/me", async (req, res) => {
  const user = await currentUser(req);
  res.json({ user });
});

app.get("/api", (_req, res) => {
  res.json({ name: "языкOn", message: "Backend API is running" });
});

app.post("/api/rooms", async (req, res) => {
  if (!db) { res.status(503).json({ error: "DATABASE_NOT_CONFIGURED" }); return; }
  if (!roomAccessSecret) { res.status(503).json({ error: "ROOM_ACCESS_NOT_CONFIGURED" }); return; }
  const user = await currentUser(req);
  if (!user) { res.status(401).json({ error: "AUTH_REQUIRED" }); return; }
  const name = typeof req.body?.name === "string" && req.body.name.trim() ? req.body.name.trim().slice(0, 100) : "Новая конференция";
  const password = typeof req.body?.password === "string" ? req.body.password.trim() : "";
  if (password && (password.length < 4 || password.length > 128)) { res.status(400).json({ error: "INVALID_ROOM_PASSWORD" }); return; }

  for (let attempt = 0; attempt < 5; attempt++) {
    const id = randomBytes(4).toString("base64url").slice(0, 6).toUpperCase();
    const salt = password ? randomBytes(16).toString("hex") : null;
    const hash = password && salt ? requireScrypt(password, salt) : null;
    const client = await db.connect();
    try {
      await client.query("BEGIN");
      const result = await client.query(
        "INSERT INTO rooms(id,name,owner_id,password_hash,password_salt) VALUES($1,$2,$3,$4,$5) RETURNING id,name,password_hash,password_salt,created_at",
        [id, name, user.id, hash, salt]
      );
      await client.query("INSERT INTO room_members(room_id,user_id,role) VALUES($1,$2,'host')", [id, user.id]);
      await client.query("COMMIT");
      const row = result.rows[0];
      const room = { id: row.id, name: row.name, createdAt: new Date(row.created_at).toISOString(), requiresPassword: Boolean(row.password_hash && row.password_salt) };
      res.status(201).json({ ...room, accessToken: issueRoomAccessToken(id, user.id, "host"), role: "host" });
      return;
    } catch (error: unknown) {
      try { await client.query("ROLLBACK"); } catch {}
      const code = typeof error === "object" && error !== null && "code" in error ? String((error as { code?: unknown }).code) : "";
      if (code === "23505") continue;
      res.status(500).json({ error: "ROOM_CREATE_FAILED" }); return;
    } finally {
      client.release();
    }
  }
  res.status(500).json({ error: "ROOM_ID_GENERATION_FAILED" });
});
app.post("/api/rooms/:id/access", async (req, res) => {
  if (!db) { res.status(503).json({ error: "DATABASE_NOT_CONFIGURED" }); return; }
  if (!roomAccessSecret) { res.status(503).json({ error: "ROOM_ACCESS_NOT_CONFIGURED" }); return; }
  const user = await currentUser(req);
  if (!user) { res.status(401).json({ error: "AUTH_REQUIRED" }); return; }

  const result = await db.query(
    "SELECT id,name,owner_id,password_hash,password_salt,created_at FROM rooms WHERE id=$1",
    [req.params.id.toUpperCase()]
  );
  const room = result.rows[0];
  if (!room) { res.status(404).json({ error: "ROOM_NOT_FOUND" }); return; }

  if (room.password_hash && room.password_salt) {
    const password = typeof req.body?.password === "string" ? req.body.password : "";
    const actual = Buffer.from(requireScrypt(password, room.password_salt), "hex");
    const expected = Buffer.from(room.password_hash, "hex");
    if (actual.length !== expected.length || !timingSafeEqual(actual, expected)) {
      res.status(401).json({ error: "INVALID_ROOM_PASSWORD" }); return;
    }
  }

  const member = await db.query("SELECT role FROM room_members WHERE room_id=$1 AND user_id=$2", [room.id, user.id]);
  let role: RoomRole = member.rows[0]?.role ?? (room.owner_id === user.id ? "host" : "member");
  if (!member.rows[0]) {
    await db.query("INSERT INTO room_members(room_id,user_id,role) VALUES($1,$2,$3)", [room.id, user.id, role]);
  }
  res.json({ accessToken: issueRoomAccessToken(room.id, user.id, role), role });
});

app.post("/api/rooms/:id/token", async (req, res) => {
  if (!db) { res.status(503).json({ error: "DATABASE_NOT_CONFIGURED" }); return; }
  if (!roomAccessSecret) { res.status(503).json({ error: "ROOM_ACCESS_NOT_CONFIGURED" }); return; }
  const user = await currentUser(req);
  if (!user) { res.status(401).json({ error: "AUTH_REQUIRED" }); return; }
  const roomId = req.params.id.toUpperCase();
  const member = await db.query("SELECT role FROM room_members WHERE room_id=$1 AND user_id=$2", [roomId, user.id]);
  if (!member.rows[0]) { res.status(403).json({ error: "ROOM_MEMBERSHIP_REQUIRED" }); return; }
  const role = member.rows[0].role as RoomRole;
  res.json({ accessToken: issueRoomAccessToken(roomId, user.id, role), role });
});

app.get("/api/rooms/:id", async (req, res) => {
  if (!db) { res.status(503).json({ error: "DATABASE_NOT_CONFIGURED" }); return; }
  const result = await db.query(
    "SELECT id,name,password_hash,password_salt,created_at FROM rooms WHERE id=$1",
    [req.params.id.toUpperCase()]
  );
  const room = result.rows[0];
  if (!room) { res.status(404).json({ error: "ROOM_NOT_FOUND" }); return; }
  res.json({
    id: room.id,
    name: room.name,
    createdAt: new Date(room.created_at).toISOString(),
    requiresPassword: Boolean(room.password_hash && room.password_salt)
  });
});

function encodeChatCursor(createdAt: Date | string, id: string): string {
  return Buffer.from(JSON.stringify({ createdAt: new Date(createdAt).toISOString(), id }), "utf8").toString("base64url");
}

function decodeChatCursor(value: string | undefined): { createdAt: string; id: string } | null {
  if (!value) return null;
  try {
    const parsed = JSON.parse(Buffer.from(value, "base64url").toString("utf8")) as { createdAt?: unknown; id?: unknown };
    if (typeof parsed.createdAt !== "string" || typeof parsed.id !== "string") return null;
    if (!Number.isFinite(Date.parse(parsed.createdAt))) return null;
    return { createdAt: new Date(parsed.createdAt).toISOString(), id: parsed.id };
  } catch {
    return null;
  }
}

app.get("/api/rooms/:id/messages", async (req, res) => {
  if (!db) { res.status(503).json({ error: "DATABASE_NOT_CONFIGURED" }); return; }
  const user = await currentUser(req);
  if (!user) { res.status(401).json({ error: "AUTH_REQUIRED" }); return; }
  const roomId = req.params.id.toUpperCase();
  const member = await db.query("SELECT 1 FROM room_members WHERE room_id=$1 AND user_id=$2", [roomId, user.id]);
  if (!member.rows[0]) { res.status(403).json({ error: "ROOM_MEMBERSHIP_REQUIRED" }); return; }

  const rawLimit = Number(req.query.limit ?? 50);
  const limit = Number.isFinite(rawLimit) ? Math.min(100, Math.max(1, Math.floor(rawLimit))) : 50;
  const cursor = decodeChatCursor(typeof req.query.before === "string" ? req.query.before : undefined);
  if (req.query.before && !cursor) { res.status(400).json({ error: "INVALID_CHAT_CURSOR" }); return; }

  const result = cursor
    ? await db.query(
      `SELECT m.id,m.user_id,m.text,m.created_at,u.username
       FROM room_messages m JOIN users u ON u.id=m.user_id
       WHERE m.room_id=$1 AND (m.created_at,m.id) < ($2::timestamptz,$3::uuid)
       ORDER BY m.created_at DESC,m.id DESC LIMIT $4`,
      [roomId, cursor.createdAt, cursor.id, limit]
    )
    : await db.query(
      `SELECT m.id,m.user_id,m.text,m.created_at,u.username
       FROM room_messages m JOIN users u ON u.id=m.user_id
       WHERE m.room_id=$1
       ORDER BY m.created_at DESC,m.id DESC LIMIT $2`,
      [roomId, limit]
    );

  const rows = [...result.rows].reverse();
  const messages = rows.map(row => ({
    id: row.id,
    userId: row.user_id,
    username: row.username,
    text: row.text,
    timestamp: new Date(row.created_at).getTime()
  }));
  const oldest = rows[0];
  res.json({
    messages,
    hasMore: result.rows.length === limit,
    nextBefore: oldest ? encodeChatCursor(oldest.created_at, oldest.id) : null
  });
});

app.post("/api/rooms/:id/messages", async (req, res) => {
  if (!db) { res.status(503).json({ error: "DATABASE_NOT_CONFIGURED" }); return; }
  const user = await currentUser(req);
  if (!user) { res.status(401).json({ error: "AUTH_REQUIRED" }); return; }
  const roomId = req.params.id.toUpperCase();
  const member = await db.query("SELECT 1 FROM room_members WHERE room_id=$1 AND user_id=$2", [roomId, user.id]);
  if (!member.rows[0]) { res.status(403).json({ error: "ROOM_MEMBERSHIP_REQUIRED" }); return; }
  const message = typeof req.body?.text === "string" ? req.body.text.trim() : "";
  if (!message || Array.from(message).length > 2000) { res.status(400).json({ error: "INVALID_MESSAGE" }); return; }
  const id = randomUUID();
  const result = await db.query(
    "INSERT INTO room_messages(id,room_id,user_id,text) VALUES($1,$2,$3,$4) RETURNING id,user_id,text,created_at",
    [id, roomId, user.id, message]
  );
  const row = result.rows[0];
  if (mediaControlSecret) {
    try {
      await fetch(mediaControlUrl.replace(/\/$/, "") + "/control/chat", {
        method: "POST",
        headers: { "Content-Type": "application/json", "Authorization": "Bearer " + mediaControlSecret },
        body: JSON.stringify({ roomId, userId: user.id, username: user.username, messageId: row.id, text: row.text, timestamp: new Date(row.created_at).getTime() })
      });
    } catch (error) { console.error("media chat sync failed", error); }
  }
  res.status(201).json({ id: row.id, userId: row.user_id, username: user.username, text: row.text, timestamp: new Date(row.created_at).getTime() });
});

app.post("/api/rooms/:id/breakouts", async (req, res) => {
  if (!db) { res.status(503).json({ error: "DATABASE_NOT_CONFIGURED" }); return; }
  const user = await currentUser(req);
  if (!user) { res.status(401).json({ error: "AUTH_REQUIRED" }); return; }
  const roomId = req.params.id.toUpperCase();
  const owner = await db.query("SELECT owner_id FROM rooms WHERE id=$1", [roomId]);
  if (!owner.rows[0]) { res.status(404).json({ error: "ROOM_NOT_FOUND" }); return; }
  if (owner.rows[0].owner_id !== user.id) { res.status(403).json({ error: "OWNER_REQUIRED" }); return; }
  const name = typeof req.body?.name === "string" ? req.body.name.trim().slice(0, 100) : "";
  if (!name) { res.status(400).json({ error: "INVALID_BREAKOUT_NAME" }); return; }
  const id = randomBytes(5).toString("base64url").slice(0, 8).toUpperCase();
  const breakout = breakoutManager.create(roomId, id, name);
  res.status(201).json({ id: breakout.id, name: breakout.name, parentRoomId: roomId, participants: [] });
});

app.delete("/api/rooms/:id/breakouts/:breakoutId", async (req, res) => {
  if (!db) { res.status(503).json({ error: "DATABASE_NOT_CONFIGURED" }); return; }
  const user = await currentUser(req);
  if (!user) { res.status(401).json({ error: "AUTH_REQUIRED" }); return; }
  const roomId = req.params.id.toUpperCase();
  const owner = await db.query("SELECT owner_id FROM rooms WHERE id=$1", [roomId]);
  if (!owner.rows[0]) { res.status(404).json({ error: "ROOM_NOT_FOUND" }); return; }
  if (owner.rows[0].owner_id !== user.id) { res.status(403).json({ error: "OWNER_REQUIRED" }); return; }
  if (!breakoutManager.delete(roomId, req.params.breakoutId.toUpperCase())) {
    res.status(404).json({ error: "BREAKOUT_NOT_FOUND" });
    return;
  }
  res.status(204).end();
});

app.get("/api/rooms/:id/breakouts", async (req, res) => {
  if (!db) { res.status(503).json({ error: "DATABASE_NOT_CONFIGURED" }); return; }
  const user = await currentUser(req);
  if (!user) { res.status(401).json({ error: "AUTH_REQUIRED" }); return; }
  const roomId = req.params.id.toUpperCase();
  const member = await db.query("SELECT 1 FROM room_members WHERE room_id=$1 AND user_id=$2", [roomId, user.id]);
  if (!member.rows[0]) { res.status(403).json({ error: "ROOM_MEMBERSHIP_REQUIRED" }); return; }
  res.json({ rooms: breakoutManager.list(roomId).map(r => ({ id: r.id, name: r.name, parentRoomId: r.parentRoomId, participants: Object.keys(r.participants) })) });
});

app.post("/api/rooms/:id/breakouts/:breakoutId/assign", async (req, res) => {
  if (!db) { res.status(503).json({ error: "DATABASE_NOT_CONFIGURED" }); return; }
  if (!roomAccessSecret) { res.status(503).json({ error: "ROOM_ACCESS_NOT_CONFIGURED" }); return; }
  const user = await currentUser(req);
  if (!user) { res.status(401).json({ error: "AUTH_REQUIRED" }); return; }
  const roomId = req.params.id.toUpperCase();
  const breakoutId = req.params.breakoutId.toUpperCase();
  const owner = await db.query("SELECT owner_id FROM rooms WHERE id=$1", [roomId]);
  if (!owner.rows[0]) { res.status(404).json({ error: "ROOM_NOT_FOUND" }); return; }
  if (owner.rows[0].owner_id !== user.id) { res.status(403).json({ error: "OWNER_REQUIRED" }); return; }
  const memberId = typeof req.body?.userId === "string" ? req.body.userId : "";
  if (!memberId) { res.status(400).json({ error: "INVALID_USER_ID" }); return; }
  const member = await db.query("SELECT 1 FROM room_members WHERE room_id=$1 AND user_id=$2", [roomId, memberId]);
  if (!member.rows[0]) { res.status(404).json({ error: "MEMBER_NOT_FOUND" }); return; }
  if (!breakoutManager.assign(roomId, breakoutId, memberId)) {
    res.status(404).json({ error: "BREAKOUT_NOT_FOUND" }); return;
  }
  res.json({ ok: true, breakoutId, userId: memberId });
});

app.post("/api/rooms/:id/breakouts/:breakoutId/join", async (req, res) => {
  if (!db) { res.status(503).json({ error: "DATABASE_NOT_CONFIGURED" }); return; }
  if (!roomAccessSecret) { res.status(503).json({ error: "ROOM_ACCESS_NOT_CONFIGURED" }); return; }
  const user = await currentUser(req);
  if (!user) { res.status(401).json({ error: "AUTH_REQUIRED" }); return; }
  const roomId = req.params.id.toUpperCase();
  const breakoutId = req.params.breakoutId.toUpperCase();
  const member = await db.query("SELECT 1 FROM room_members WHERE room_id=$1 AND user_id=$2", [roomId, user.id]);
  if (!member.rows[0]) { res.status(403).json({ error: "ROOM_MEMBERSHIP_REQUIRED" }); return; }
  if (!breakoutManager.get(roomId, breakoutId)) { res.status(404).json({ error: "BREAKOUT_NOT_FOUND" }); return; }
  if (!breakoutManager.assign(roomId, breakoutId, user.id)) {
    res.status(409).json({ error: "BREAKOUT_ASSIGN_FAILED" }); return;
  }
  res.json({ accessToken: issueRoomAccessToken(breakoutId, user.id, "member"), role: "member", parentRoomId: roomId, breakoutId });
});

app.delete("/api/rooms/:id/breakouts/:breakoutId/participants/:userId", async (req, res) => {
  if (!db) { res.status(503).json({ error: "DATABASE_NOT_CONFIGURED" }); return; }
  const user = await currentUser(req);
  if (!user) { res.status(401).json({ error: "AUTH_REQUIRED" }); return; }
  const roomId = req.params.id.toUpperCase();
  const breakoutId = req.params.breakoutId.toUpperCase();
  const owner = await db.query("SELECT owner_id FROM rooms WHERE id=$1", [roomId]);
  if (!owner.rows[0]) { res.status(404).json({ error: "ROOM_NOT_FOUND" }); return; }
  if (owner.rows[0].owner_id !== user.id) { res.status(403).json({ error: "OWNER_REQUIRED" }); return; }
  const targetUserId = req.params.userId;
  if (!breakoutManager.remove(roomId, breakoutId, targetUserId)) {
    res.status(404).json({ error: "BREAKOUT_PARTICIPANT_NOT_FOUND" }); return;
  }
  res.json({ ok: true });
});

app.get("/api/rooms/:id/membership", async (req, res) => {
  if (!db) { res.status(503).json({ error: "DATABASE_NOT_CONFIGURED" }); return; }
  const user = await currentUser(req);
  if (!user) { res.status(401).json({ error: "AUTH_REQUIRED" }); return; }
  const result = await db.query("SELECT role FROM room_members WHERE room_id=$1 AND user_id=$2", [req.params.id.toUpperCase(), user.id]);
  res.json({ member: Boolean(result.rows[0]), role: result.rows[0]?.role ?? null });
});

app.get("/api/rooms/:id/members", async (req, res) => {
  if (!db) { res.status(503).json({ error: "DATABASE_NOT_CONFIGURED" }); return; }
  const user = await currentUser(req);
  if (!user) { res.status(401).json({ error: "AUTH_REQUIRED" }); return; }
  const roomId = req.params.id.toUpperCase();
  const owner = await db.query("SELECT owner_id FROM rooms WHERE id=$1", [roomId]);
  if (!owner.rows[0]) { res.status(404).json({ error: "ROOM_NOT_FOUND" }); return; }
  if (owner.rows[0].owner_id !== user.id) { res.status(403).json({ error: "OWNER_REQUIRED" }); return; }
  const members = await db.query(
    "SELECT u.id,u.username,rm.role,rm.created_at FROM room_members rm JOIN users u ON u.id=rm.user_id WHERE rm.room_id=$1 ORDER BY CASE rm.role WHEN 'host' THEN 0 WHEN 'cohost' THEN 1 ELSE 2 END,u.username",
    [roomId]
  );
  res.json({ members: members.rows });
});

app.patch("/api/rooms/:id/members/:userId", async (req, res) => {
  if (!db) { res.status(503).json({ error: "DATABASE_NOT_CONFIGURED" }); return; }
  const user = await currentUser(req);
  if (!user) { res.status(401).json({ error: "AUTH_REQUIRED" }); return; }
  const roomId = req.params.id.toUpperCase();
  const role = req.body?.role;
  if (role !== "cohost" && role !== "member") { res.status(400).json({ error: "INVALID_ROLE" }); return; }
  const owner = await db.query("SELECT owner_id FROM rooms WHERE id=$1", [roomId]);
  if (!owner.rows[0]) { res.status(404).json({ error: "ROOM_NOT_FOUND" }); return; }
  if (owner.rows[0].owner_id !== user.id) { res.status(403).json({ error: "OWNER_REQUIRED" }); return; }
  if (req.params.userId === user.id) { res.status(400).json({ error: "CANNOT_CHANGE_OWNER" }); return; }
  const result = await db.query("UPDATE room_members SET role=$1 WHERE room_id=$2 AND user_id=$3 RETURNING role", [role,roomId,req.params.userId]);
  if (!result.rows[0]) { res.status(404).json({ error: "MEMBER_NOT_FOUND" }); return; }
  if (mediaControlSecret) {
    try {
      await fetch(mediaControlUrl.replace(/\\/$/, "") + "/control/role", {
        method: "POST",
        headers: { "Content-Type": "application/json", "Authorization": "Bearer " + mediaControlSecret },
        body: JSON.stringify({ roomId, userId: req.params.userId, role })
      });
    } catch (error) { console.error("media role sync failed", error); }
  }
  res.json({ role: result.rows[0].role });
});
const server = createServer(app);
attachSignaling(server);

initDatabase().then(() => {
  if (db) {
    const cleanup = setInterval(() => {
      void db.query("DELETE FROM sessions WHERE expires_at <= now()").catch(error => console.error("session cleanup failed", error));
    }, 60 * 60 * 1000);
    cleanup.unref();
  }
  server.listen(port, "0.0.0.0", () => {
    console.log(`языкOn server listening on http://0.0.0.0:${port}`);
    console.log(`языкOn signaling listening on ws://0.0.0.0:${port}/ws`);
  });
});

async function shutdown(signal: string) {
  console.log(`языкOn server shutting down (${signal})`);
  server.close(async () => {
    await db?.end();
    await closeRedis();
    process.exit(0);
  });
  setTimeout(() => process.exit(1), 10_000).unref();
}
process.on("SIGTERM", () => void shutdown("SIGTERM"));
process.on("SIGINT", () => void shutdown("SIGINT"));
