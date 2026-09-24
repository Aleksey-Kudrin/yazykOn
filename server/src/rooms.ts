export interface Room {
  id: string;
  name: string;
  createdAt: string;
}

const rooms = new Map<string, Room>();

function generateId(): string {
  return Math.random().toString(36).slice(2, 8).toUpperCase();
}

export function createRoom(name = "Новая конференция"): Room {
  let id = generateId();
  while (rooms.has(id)) id = generateId();

  const room: Room = {
    id,
    name,
    createdAt: new Date().toISOString()
  };

  rooms.set(id, room);
  return room;
}

export function getRoom(id: string): Room | undefined {
  return rooms.get(id);
}
