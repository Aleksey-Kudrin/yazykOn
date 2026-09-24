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
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ name, password })
  });
  if (!response.ok) throw new Error("Не удалось создать конференцию");
  return response.json();
}

export async function getRoom(id: string): Promise<Room> {
  const response = await fetch(`${API_URL}/api/rooms/${encodeURIComponent(id)}`);
  if (!response.ok) throw new Error("Комната не найдена");
  return response.json();
}

export async function accessRoom(id: string, password = ""): Promise<{ accessToken: string | null }> {
  const response = await fetch(`${API_URL}/api/rooms/${encodeURIComponent(id)}/access`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ password })
  });
  const data = await response.json().catch(() => ({}));
  if (!response.ok) {
    throw new Error(data.error === "INVALID_ROOM_PASSWORD" ? "Неверный пароль комнаты" : "Не удалось получить доступ к комнате");
  }
  return data;
}
