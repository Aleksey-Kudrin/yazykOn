import { createClient, type RedisClientType } from "redis";

let client: RedisClientType | null = null;
let connectPromise: Promise<RedisClientType | null> | null = null;
const subscriberClients = new Map<string, RedisClientType>();

export function redisEnabled() {
  return Boolean(process.env.REDIS_URL);
}

export async function getRedis(): Promise<RedisClientType | null> {
  const url = process.env.REDIS_URL;
  if (!url) return null;
  if (client?.isReady) return client;
  if (connectPromise) return connectPromise;

  client = createClient({ url }) as RedisClientType;
  client.on("error", error => console.error("Redis error", error));
  connectPromise = client.connect()
    .then(() => client)
    .catch(error => {
      console.error("Redis connection failed", error);
      client = null;
      return null;
    })
    .finally(() => { connectPromise = null; });
  return connectPromise;
}

const presenceKey = (roomId: string, userId: string) => `yazykon:presence:${roomId}:${userId}`;
const peerKey = (roomId: string, peerId: string) => `yazykon:peer:${roomId}:${peerId}`;

export function redisRoomChannel(roomId: string) {
  return `yazykon:room:${roomId}`;
}

export async function redisSetPresence(roomId: string, userId: string, ttlSeconds = 60) {
  const redis = await getRedis();
  if (!redis) return false;
  await redis.set(presenceKey(roomId, userId), "1", { EX: ttlSeconds });
  return true;
}

export async function redisClearPresence(roomId: string, userId: string) {
  const redis = await getRedis();
  if (!redis) return false;
  await redis.del(presenceKey(roomId, userId));
  return true;
}

export async function redisGetPresence(roomId: string, userId: string) {
  const redis = await getRedis();
  if (!redis) return false;
  return (await redis.exists(presenceKey(roomId, userId))) === 1;
}

export async function redisListPresence(roomId: string) {
  const redis = await getRedis();
  if (!redis) return [];
  const prefix = `yazykon:presence:${roomId}:`;
  const users: string[] = [];
  for await (const key of redis.scanIterator({ MATCH: `${prefix}*`, COUNT: 100 })) {
    users.push(key.slice(prefix.length));
  }
  return users;
}

export interface RedisPeerRecord {
  nodeId: string;
  peerId: string;
  sessionId: string;
}

export async function redisRegisterPeer(
  roomId: string,
  peerId: string,
  nodeId: string,
  sessionId: string,
  ttlSeconds = 60
): Promise<RedisPeerRecord | null> {
  const redis = await getRedis();
  if (!redis) return null;
  const key = peerKey(roomId, peerId);
  const previousRaw = await redis.get(key);
  let previous: RedisPeerRecord | null = null;
  if (previousRaw) {
    try {
      const value = JSON.parse(previousRaw) as Partial<RedisPeerRecord>;
      if (value.nodeId && value.peerId && value.sessionId) {
        previous = { nodeId: value.nodeId, peerId: value.peerId, sessionId: value.sessionId };
      }
    } catch { /* replace malformed registry entries */ }
  }
  await redis.set(key, JSON.stringify({ nodeId, peerId, sessionId }), { EX: ttlSeconds });
  return previous;
}

export async function redisRefreshPeer(roomId: string, peerId: string, nodeId: string, sessionId: string, ttlSeconds = 60) {
  const redis = await getRedis();
  if (!redis) return false;
  const key = peerKey(roomId, peerId);
  const raw = await redis.get(key);
  if (!raw) return false;
  try {
    const value = JSON.parse(raw) as { nodeId?: string };
    if (value.nodeId !== nodeId) return false;
  } catch {
    return false;
  }
  await redis.expire(key, ttlSeconds);
  return true;
}

export async function redisRemovePeer(roomId: string, peerId: string, nodeId: string, sessionId: string) {
  const redis = await getRedis();
  if (!redis) return false;
  const key = peerKey(roomId, peerId);
  const raw = await redis.get(key);
  if (!raw) return false;
  try {
    const value = JSON.parse(raw) as { nodeId?: string };
    if (value.nodeId !== nodeId) return false;
  } catch {
    return false;
  }
  await redis.del(key);
  return true;
}

export async function redisListPeers(roomId: string) {
  const redis = await getRedis();
  if (!redis) return [];
  const prefix = `yazykon:peer:${roomId}:`;
  const peers: RedisPeerRecord[] = [];
  for await (const key of redis.scanIterator({ MATCH: `${prefix}*`, COUNT: 100 })) {
    const raw = await redis.get(key);
    if (!raw) continue;
    try {
      const value = JSON.parse(raw) as Partial<RedisPeerRecord>;
      if (value.peerId && value.nodeId && value.sessionId) {
        peers.push({ peerId: value.peerId, nodeId: value.nodeId, sessionId: value.sessionId });
      }
    } catch { /* ignore malformed registry entries */ }
  }
  return peers;
}

export async function redisSubscribe(channel: string, handler: (payload: unknown) => void) {
  if (subscriberClients.has(channel)) return true;
  const redis = await getRedis();
  if (!redis) return false;
  const subscriber = redis.duplicate();
  subscriberClients.set(channel, subscriber);
  subscriber.on("error", error => console.error("Redis subscriber error", error));
  try {
    await subscriber.connect();
    await subscriber.subscribe(channel, raw => {
      try { handler(JSON.parse(raw)); } catch { /* ignore malformed pubsub payloads */ }
    });
    return true;
  } catch (error) {
    subscriberClients.delete(channel);
    if (subscriber.isOpen) await subscriber.quit().catch(() => undefined);
    console.error("Redis subscribe failed", error);
    return false;
  }
}

export async function redisUnsubscribe(channel: string) {
  const subscriber = subscriberClients.get(channel);
  if (!subscriber) return false;
  subscriberClients.delete(channel);
  if (subscriber.isOpen) await subscriber.unsubscribe(channel).catch(() => undefined);
  if (subscriber.isOpen) await subscriber.quit().catch(() => undefined);
  return true;
}

export async function redisPublish(channel: string, payload: unknown) {
  const redis = await getRedis();
  if (!redis) return false;
  await redis.publish(channel, JSON.stringify(payload));
  return true;
}

export async function redisClaimRoom(roomId: string, nodeId: string, ttlSeconds = 30) {
  const redis = await getRedis();
  if (!redis) return null;
  const key = `yazykon:room-owner:${roomId}`;
  const claimed = await redis.set(key, nodeId, { NX: true, EX: ttlSeconds });
  if (claimed === "OK") return nodeId;
  const owner = await redis.get(key);
  return owner === nodeId ? nodeId : owner;
}

export async function redisReleaseRoom(roomId: string, nodeId: string) {
  const redis = await getRedis();
  if (!redis) return false;
  const key = `yazykon:room-owner:${roomId}`;
  const owner = await redis.get(key);
  if (owner !== nodeId) return false;
  await redis.del(key);
  return true;
}

export async function closeRedis() {
  for (const [channel, subscriber] of subscriberClients) {
    if (subscriber.isOpen) await subscriber.unsubscribe(channel).catch(() => undefined);
    if (subscriber.isOpen) await subscriber.quit().catch(() => undefined);
  }
  subscriberClients.clear();
  if (client?.isOpen) await client.quit();
  client = null;
}