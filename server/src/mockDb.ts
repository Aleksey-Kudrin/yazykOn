export interface MockUser {
  id: string;
  username: string;
  password_hash: string;
  created_at: Date;
}

export interface MockSession {
  id: string;
  user_id: string;
  expires_at: Date;
  created_at: Date;
}

export interface MockRoom {
  id: string;
  name: string;
  owner_id: string | null;
  password_hash: string | null;
  password_salt: string | null;
  created_at: Date;
}

export interface MockRoomMember {
  room_id: string;
  user_id: string;
  role: "host" | "cohost" | "member";
  created_at: Date;
}

export interface MockRoomMessage {
  id: string;
  room_id: string;
  user_id: string;
  text: string;
  created_at: Date;
}

export interface MockAuditEvent {
  id: string;
  user_id: string | null;
  room_id: string | null;
  action: string;
  target_user_id: string | null;
  ip: string | null;
  created_at: Date;
}

export class InMemoryDb {
  users = new Map<string, MockUser>(); // id -> MockUser
  usersByUsername = new Map<string, MockUser>(); // username -> MockUser
  sessions = new Map<string, MockSession>(); // id -> MockSession
  rooms = new Map<string, MockRoom>(); // id -> MockRoom
  roomMembers = new Map<string, MockRoomMember>(); // "roomId:userId" -> MockRoomMember
  messages: MockRoomMessage[] = [];
  auditEvents: MockAuditEvent[] = [];

  async query(sql: string, params: unknown[] = []): Promise<{ rows: any[] }> {
    const trimmed = sql.trim();

    // DDL & Ping
    if (trimmed.startsWith("CREATE TABLE") || trimmed.startsWith("CREATE INDEX")) {
      return { rows: [] };
    }
    if (trimmed === "SELECT 1") {
      return { rows: [{ "?column?": 1 }] };
    }
    if (trimmed === "BEGIN" || trimmed === "COMMIT" || trimmed === "ROLLBACK") {
      return { rows: [] };
    }

    // USERS
    if (trimmed.startsWith("INSERT INTO users")) {
      const [id, username, password_hash] = params as [string, string, string];
      if (this.usersByUsername.has(username)) {
        const err = new Error("duplicate key value violates unique constraint \"users_username_key\"");
        (err as any).code = "23505";
        throw err;
      }
      const user: MockUser = { id, username, password_hash, created_at: new Date() };
      this.users.set(id, user);
      this.usersByUsername.set(username, user);
      return { rows: [user] };
    }

    if (trimmed.startsWith("SELECT id, username, password_hash FROM users WHERE username=$1")) {
      const [username] = params as [string];
      const user = this.usersByUsername.get(username);
      return { rows: user ? [{ id: user.id, username: user.username, password_hash: user.password_hash }] : [] };
    }

    if (trimmed.startsWith("UPDATE users SET password_hash=$1 WHERE id=$2")) {
      const [hash, id] = params as [string, string];
      const user = this.users.get(id);
      if (user) {
        user.password_hash = hash;
      }
      return { rows: user ? [user] : [] };
    }

    // SESSIONS
    if (trimmed.startsWith("INSERT INTO sessions")) {
      const [id, user_id, seconds] = params as [string, string, number];
      const expires_at = new Date(Date.now() + (Number(seconds) || 604800) * 1000);
      const session: MockSession = { id, user_id, expires_at, created_at: new Date() };
      this.sessions.set(id, session);
      return { rows: [session] };
    }

    if (trimmed.includes("FROM sessions s JOIN users u ON u.id=s.user_id WHERE s.id=$1 AND s.expires_at > now()")) {
      const [id] = params as [string];
      const session = this.sessions.get(id);
      if (session && session.expires_at.getTime() > Date.now()) {
        const user = this.users.get(session.user_id);
        if (user) {
          return { rows: [{ id: user.id, username: user.username }] };
        }
      }
      return { rows: [] };
    }

    if (trimmed.startsWith("DELETE FROM sessions WHERE id=$1")) {
      const [id] = params as [string];
      this.sessions.delete(id);
      return { rows: [] };
    }

    if (trimmed.startsWith("DELETE FROM sessions WHERE expires_at <= now()")) {
      const now = Date.now();
      for (const [id, session] of this.sessions) {
        if (session.expires_at.getTime() <= now) {
          this.sessions.delete(id);
        }
      }
      return { rows: [] };
    }

    // ROOMS
    if (trimmed.startsWith("INSERT INTO rooms")) {
      const [id, name, owner_id, password_hash, password_salt] = params as [
        string,
        string,
        string | null,
        string | null,
        string | null
      ];
      if (this.rooms.has(id)) {
        const err = new Error("duplicate key value violates unique constraint \"rooms_pkey\"");
        (err as any).code = "23505";
        throw err;
      }
      const room: MockRoom = {
        id,
        name,
        owner_id: owner_id ?? null,
        password_hash: password_hash ?? null,
        password_salt: password_salt ?? null,
        created_at: new Date()
      };
      this.rooms.set(id, room);
      return {
        rows: [{
          id: room.id,
          name: room.name,
          password_hash: room.password_hash,
          password_salt: room.password_salt,
          created_at: room.created_at.toISOString()
        }]
      };
    }

    if (trimmed.startsWith("SELECT id,name,owner_id,password_hash,password_salt,created_at FROM rooms WHERE id=$1")) {
      const [id] = params as [string];
      const room = this.rooms.get(id);
      return { rows: room ? [room] : [] };
    }

    if (trimmed.startsWith("SELECT id,name,password_hash,password_salt,created_at FROM rooms WHERE id=$1")) {
      const [id] = params as [string];
      const room = this.rooms.get(id);
      return { rows: room ? [room] : [] };
    }

    if (trimmed.startsWith("SELECT owner_id FROM rooms WHERE id=$1")) {
      const [id] = params as [string];
      const room = this.rooms.get(id);
      return { rows: room ? [{ owner_id: room.owner_id }] : [] };
    }

    // ROOM MEMBERS
    if (trimmed.startsWith("INSERT INTO room_members")) {
      const [room_id, user_id, role] = params as [string, string, "host" | "cohost" | "member"];
      const key = `${room_id}:${user_id}`;
      const member: MockRoomMember = { room_id, user_id, role: role || "member", created_at: new Date() };
      this.roomMembers.set(key, member);
      return { rows: [member] };
    }

    if (trimmed.startsWith("SELECT 1 FROM room_members WHERE room_id=$1 AND user_id=$2")) {
      const [room_id, user_id] = params as [string, string];
      const key = `${room_id}:${user_id}`;
      return { rows: this.roomMembers.has(key) ? [{ "?column?": 1 }] : [] };
    }

    if (trimmed.startsWith("SELECT role FROM room_members WHERE room_id=$1 AND user_id=$2")) {
      const [room_id, user_id] = params as [string, string];
      const key = `${room_id}:${user_id}`;
      const member = this.roomMembers.get(key);
      return { rows: member ? [{ role: member.role }] : [] };
    }

    if (trimmed.includes("FROM room_members rm JOIN users u ON u.id=rm.user_id WHERE rm.room_id=$1")) {
      const [room_id] = params as [string];
      const results: any[] = [];
      for (const member of this.roomMembers.values()) {
        if (member.room_id === room_id) {
          const user = this.users.get(member.user_id);
          if (user) {
            results.push({
              id: user.id,
              username: user.username,
              role: member.role,
              created_at: member.created_at.toISOString()
            });
          }
        }
      }
      const roleRank: Record<string, number> = { host: 0, cohost: 1, member: 2 };
      results.sort((a, b) => (roleRank[a.role] ?? 9) - (roleRank[b.role] ?? 9) || a.username.localeCompare(b.username));
      return { rows: results };
    }

    if (trimmed.startsWith("UPDATE room_members SET role=$1 WHERE room_id=$2 AND user_id=$3 RETURNING role")) {
      const [role, room_id, user_id] = params as ["cohost" | "member", string, string];
      const key = `${room_id}:${user_id}`;
      const member = this.roomMembers.get(key);
      if (member) {
        member.role = role;
        return { rows: [{ role: member.role }] };
      }
      return { rows: [] };
    }

    // ROOM MESSAGES
    if (trimmed.startsWith("INSERT INTO room_messages")) {
      const [id, room_id, user_id, text] = params as [string, string, string, string];
      const message: MockRoomMessage = { id, room_id, user_id, text, created_at: new Date() };
      this.messages.push(message);
      return { rows: [{ id: message.id, user_id: message.user_id, text: message.text, created_at: message.created_at.toISOString() }] };
    }

    if (trimmed.includes("FROM room_messages m JOIN users u ON u.id=m.user_id WHERE m.room_id=$1")) {
      const room_id = params[0] as string;
      const isCursor = trimmed.includes("(m.created_at,m.id) <");
      const cursorDate = isCursor ? new Date(params[1] as string).getTime() : 0;
      const cursorId = isCursor ? (params[2] as string) : "";
      const limit = Number(isCursor ? params[3] : params[1]) || 50;

      let filtered = this.messages.filter(m => m.room_id === room_id);
      if (isCursor) {
        filtered = filtered.filter(m => {
          const mTime = m.created_at.getTime();
          if (mTime < cursorDate) return true;
          if (mTime === cursorDate && m.id < cursorId) return true;
          return false;
        });
      }

      filtered.sort((a, b) => b.created_at.getTime() - a.created_at.getTime() || b.id.localeCompare(a.id));
      const sliced = filtered.slice(0, limit);
      const rows = sliced.map(m => {
        const user = this.users.get(m.user_id);
        return {
          id: m.id,
          user_id: m.user_id,
          text: m.text,
          created_at: m.created_at.toISOString(),
          username: user?.username ?? "unknown"
        };
      });
      return { rows };
    }

    // AUDIT EVENTS
    if (trimmed.startsWith("INSERT INTO audit_events")) {
      const [id, user_id, room_id, action, target_user_id, ip] = params as [
        string,
        string | null,
        string | null,
        string,
        string | null,
        string | null
      ];
      const event: MockAuditEvent = {
        id,
        user_id: user_id ?? null,
        room_id: room_id ?? null,
        action,
        target_user_id: target_user_id ?? null,
        ip: ip ?? null,
        created_at: new Date()
      };
      this.auditEvents.push(event);
      return { rows: [] };
    }

    if (trimmed.includes("FROM audit_events a LEFT JOIN users u ON u.id=a.user_id WHERE a.room_id=$1")) {
      const room_id = params[0] as string;
      const limit = Number(params[1]) || 100;
      const filtered = this.auditEvents.filter(a => a.room_id === room_id);
      filtered.sort((a, b) => b.created_at.getTime() - a.created_at.getTime());
      const rows = filtered.slice(0, limit).map(a => {
        const user = a.user_id ? this.users.get(a.user_id) : null;
        return {
          id: a.id,
          action: a.action,
          user_id: a.user_id,
          target_user_id: a.target_user_id,
          ip: a.ip,
          created_at: a.created_at.toISOString(),
          username: user?.username ?? null
        };
      });
      return { rows };
    }

    if (trimmed.startsWith("DELETE FROM audit_events")) {
      return { rows: [] };
    }

    return { rows: [] };
  }

  async connect() {
    return {
      query: (sql: string, params?: unknown[]) => this.query(sql, params),
      release: () => {}
    };
  }

  async end() {}
}

export const mockDb = new InMemoryDb();
