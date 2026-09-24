export type BreakoutRoom = {
  id: string;
  parentRoomId: string;
  name: string;
  participants: Record<string, string>;
};

export class BreakoutManager {
  private readonly rooms = new Map<string, BreakoutRoom>();

  create(parentRoomId: string, id: string, name: string): BreakoutRoom {
    const room: BreakoutRoom = {
      id,
      parentRoomId,
      name,
      participants: {}
    };
    this.rooms.set(id, room);
    return this.clone(room);
  }

  assign(parentRoomId: string, breakoutId: string, userId: string): boolean {
    const target = this.rooms.get(breakoutId);
    if (!target || target.parentRoomId !== parentRoomId) return false;
    for (const room of this.rooms.values()) {
      delete room.participants[userId];
    }
    target.participants[userId] = breakoutId;
    return true;
  }

  remove(parentRoomId: string, breakoutId: string, userId: string): boolean {
    const room = this.rooms.get(breakoutId);
    if (!room || room.parentRoomId !== parentRoomId || !(userId in room.participants)) return false;
    delete room.participants[userId];
    return true;
  }

  get(parentRoomId: string, breakoutId: string): BreakoutRoom | undefined {
    const room = this.rooms.get(breakoutId);
    return room && room.parentRoomId === parentRoomId ? this.clone(room) : undefined;
  }

  list(parentRoomId: string): BreakoutRoom[] {
    return [...this.rooms.values()]
      .filter(room => room.parentRoomId === parentRoomId)
      .map(room => this.clone(room));
  }

  private clone(room: BreakoutRoom): BreakoutRoom {
    return { ...room, participants: { ...room.participants } };
  }
}
