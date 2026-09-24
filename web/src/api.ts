export interface Room {
  id: string;
  name: string;
  createdAt: string;
}

const API_URL = import.meta.env.VITE_API_URL ?? "http://localhost:3000";

export async function createRoom(name: string): Promise<Room> {
  const response = await fetch(`${API_URL}/api/rooms`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ name })
  });

  if (!response.ok) throw new Error("Не удалось создать конференцию");
  return response.json();
}
