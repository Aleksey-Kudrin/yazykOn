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
  const response = await fetch(\`${API_URL}\` + "/api/auth/me", { credentials: "include" });
  if (!response.ok) return null;
  const data = await response.json();
  return data.user ?? null;
}
export async function register(username: string, password: string): Promise<User> {
  const response = await fetch(\`${API_URL}\` + "/api/auth/register", { method: "POST", credentials: "include", headers: { "Content-Type": "application/json" }, body: JSON.stringify({ username, password }) });
  const data = await response.json().catch(() => ({}));
  if (!response.ok) throw new Error(data.error === "USERNAME_TAKEN" ? "Имя уже занято" : "Не удалось зарегистрироваться");
  return data;
}
export async function login(username: string, password: string): Promise<User> {
  const response = await fetch(\`${API_URL}\` + "/api/auth/login", { method: "POST", credentials: "include", headers: { "Content-Type": "application/json" }, body: JSON.stringify({ username, password }) });
  const data = await response.json().catch(() => ({}));
  if (!response.ok) throw new Error("Неверное имя или пароль");
  return data;
}
export async function logout(): Promise<void> { await fetch(\`${API_URL}\` + "/api/auth/logout", { method: "POST", credentials: "include" }); }
