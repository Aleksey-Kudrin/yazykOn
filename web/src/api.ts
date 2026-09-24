export interface Room {
  id: string;
  name: string;
  createdAt: string;
  requiresPassword: boolean;
  accessToken?: string | null;
}

const API_URL = import.meta.env.VITE_API_URL ?? "http://localhost:3000";

export async function createRoom(name: string, password = ""): Promise<Room> {
  const response = await fetch(`${API_URL}/api/rooms`, {
    method: "POST",
    credentials: "include",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ name, password })
  });
  if (!response.ok) throw new Error("Не удалось создать конференцию");
  return response.json();
}

export async function getRoom(id: string): Promise<Room> {
  const response = await fetch(`${API_URL}/api/rooms/${encodeURIComponent(id)}`, { credentials: "include" });
  if (!response.ok) throw new Error("Комната не найдена");
  return response.json();
}

export async function accessRoom(id: string, password = ""): Promise<{ accessToken: string | null; role: "host" | "cohost" | "member" | null }> {
  const response = await fetch(`${API_URL}/api/rooms/${encodeURIComponent(id)}/access`, {
    method: "POST",
    credentials: "include",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ password })
  });
  const data = await response.json().catch(() => ({}));
  if (!response.ok) {
    throw new Error(data.error === "INVALID_ROOM_PASSWORD" ? "Неверный пароль комнаты" : "Не удалось получить доступ к комнате");
  }
  return data;
}

export interface User { id: string; username: string; }
export async function getMe(): Promise<User | null> {
  const response = await fetch(`${API_URL}` + "/api/auth/me", { credentials: "include" });
  if (!response.ok) return null;
  const data = await response.json();
  return data.user ?? null;
}
export async function register(username: string, password: string): Promise<User> {
  const response = await fetch(`${API_URL}` + "/api/auth/register", { method: "POST", credentials: "include", headers: { "Content-Type": "application/json" }, body: JSON.stringify({ username, password }) });
  const data = await response.json().catch(() => ({}));
  if (!response.ok) throw new Error(data.error === "USERNAME_TAKEN" ? "Имя уже занято" : "Не удалось зарегистрироваться");
  return data;
}
export async function login(username: string, password: string): Promise<User> {
  const response = await fetch(`${API_URL}` + "/api/auth/login", { method: "POST", credentials: "include", headers: { "Content-Type": "application/json" }, body: JSON.stringify({ username, password }) });
  const data = await response.json().catch(() => ({}));
  if (!response.ok) throw new Error("Неверное имя или пароль");
  return data;
}
export async function logout(): Promise<void> { await fetch(`${API_URL}` + "/api/auth/logout", { method: "POST", credentials: "include" }); }

export interface RoomMember { id: string; username: string; role: "host" | "cohost" | "member"; created_at: string; }
export async function getRoomMembers(id: string): Promise<RoomMember[]> {
  const response = await fetch(`${API_URL}/api/rooms/${encodeURIComponent(id)}/members`, { credentials: "include" });
  const data = await response.json().catch(() => ({}));
  if (!response.ok) throw new Error(data.error ?? "Не удалось получить участников");
  return data.members ?? [];
}
export async function setRoomMemberRole(roomId: string, userId: string, role: "cohost" | "member"): Promise<void> {
  const response = await fetch(`${API_URL}/api/rooms/${encodeURIComponent(roomId)}/members/${encodeURIComponent(userId)}`, {
    method: "PATCH", credentials: "include", headers: { "Content-Type": "application/json" }, body: JSON.stringify({ role })
  });
  if (!response.ok) throw new Error("Не удалось изменить роль");
}

export interface RoomMessage {
  id: string;
  userId: string;
  username: string;
  text: string;
  timestamp: number;
}

export async function getRoomMessages(id: string): Promise<RoomMessage[]> {
  const response = await fetch(`${API_URL}/api/rooms/${encodeURIComponent(id)}/messages`, { credentials: "include" });
  const data = await response.json().catch(() => ({}));
  if (!response.ok) throw new Error(data.error ?? "Не удалось получить историю чата");
  return data.messages ?? [];
}

export async function sendRoomMessage(id: string, text: string): Promise<RoomMessage> {
  const response = await fetch(`${API_URL}/api/rooms/${encodeURIComponent(id)}/messages`, {
    method: "POST",
    credentials: "include",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ text })
  });
  const data = await response.json().catch(() => ({}));
  if (!response.ok) throw new Error(data.error ?? "Не удалось отправить сообщение");
  return data;
}

export interface BreakoutRoom {
  id: string;
  name: string;
  parentRoomId: string;
  participants: string[];
}

export async function getBreakoutRooms(id: string): Promise<BreakoutRoom[]> {
  const response = await fetch(`${API_URL}/api/rooms/${encodeURIComponent(id)}/breakouts`, { credentials: "include" });
  const data = await response.json().catch(() => ({}));
  if (!response.ok) throw new Error(data.error ?? "Не удалось получить breakout-комнаты");
  return data.rooms ?? [];
}

export async function createBreakoutRoom(id: string, name: string): Promise<BreakoutRoom> {
  const response = await fetch(`${API_URL}/api/rooms/${encodeURIComponent(id)}/breakouts`, {
    method: "POST", credentials: "include", headers: { "Content-Type": "application/json" }, body: JSON.stringify({ name })
  });
  const data = await response.json().catch(() => ({}));
  if (!response.ok) throw new Error(data.error ?? "Не удалось создать breakout-комнату");
  return data;
}

export async function assignBreakoutParticipant(roomId: string, breakoutId: string, userId: string): Promise<void> {
  const response = await fetch(`${API_URL}/api/rooms/${encodeURIComponent(roomId)}/breakouts/${encodeURIComponent(breakoutId)}/assign`, {
    method: "POST", credentials: "include", headers: { "Content-Type": "application/json" }, body: JSON.stringify({ userId })
  });
  if (!response.ok) throw new Error("Не удалось назначить участника");
}

export async function joinBreakoutRoom(roomId: string, breakoutId: string): Promise<{ accessToken: string; parentRoomId: string; breakoutId: string }> {
  const response = await fetch(`${API_URL}/api/rooms/${encodeURIComponent(roomId)}/breakouts/${encodeURIComponent(breakoutId)}/join`, {
    method: "POST", credentials: "include"
  });
  const data = await response.json().catch(() => ({}));
  if (!response.ok) throw new Error(data.error ?? "Не удалось войти в breakout-комнату");
  return data;
}

export async function removeBreakoutParticipant(roomId: string, breakoutId: string, userId: string): Promise<void> {
  const response = await fetch(`${API_URL}/api/rooms/${encodeURIComponent(roomId)}/breakouts/${encodeURIComponent(breakoutId)}/participants/${encodeURIComponent(userId)}`, {
    method: "DELETE", credentials: "include"
  });
  if (!response.ok) throw new Error("Не удалось удалить участника из breakout-комнаты");
}
