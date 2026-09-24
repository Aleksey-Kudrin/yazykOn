import { randomBytes, scryptSync, timingSafeEqual } from "node:crypto";

export interface Room {
  id: string;
  name: string;
  createdAt: string;
  requiresPassword: boolean;
}

interface StoredRoom extends Room {
  passwordHash?: string;
  passwordSalt?: string;
}

const rooms = new Map<string, StoredRoom>();

function generateId(): string {
  return Math.random().toString(36).slice(2, 8).toUpperCase();
}

export function createRoom(name = "Новая конференция", password?: string): Room {
  let id = generateId();
  while (rooms.has(id)) id = generateId();
  const normalizedPassword = typeof password === "string" ? password.trim() : "";
  const room: StoredRoom = { id, name, createdAt: new Date().toISOString(), requiresPassword: Boolean(normalizedPassword) };
  if (normalizedPassword) {
    const salt = randomBytes(16).toString("hex");
    room.passwordSalt = salt;
    room.passwordHash = scryptSync(normalizedPassword, salt, 32).toString("hex");
  }
  rooms.set(id, room);
  return publicRoom(room);
}

function publicRoom(room: StoredRoom): Room {
  return { id: room.id, name: room.name, createdAt: room.createdAt, requiresPassword: room.requiresPassword };
}

export function getRoom(id: string): Room | undefined {
  const room = rooms.get(id);
  return room ? publicRoom(room) : undefined;
}

export function verifyRoomPassword(id: string, password: string): boolean {
  const room = rooms.get(id);
  if (!room) return false;
  if (!room.passwordHash || !room.passwordSalt) return !room.requiresPassword;
  const actual = scryptSync(password, room.passwordSalt, 32);
  const expected = Buffer.from(room.passwordHash, "hex");
  return actual.length === expected.length && timingSafeEqual(actual, expected);
}
