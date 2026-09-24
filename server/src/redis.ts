import { createClient, type RedisClientType } from "redis";

let client: RedisClientType | null = null;
let connectPromise: Promise<RedisClientType | null> | null = null;
let subscriberClients: RedisClientType[] = [];

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

export async function redisSetPresence(roomId: string, userId: string, ttlSeconds = 60) {
  const redis = await getRedis();
  if (!redis) return false;
  await redis.set(`yazykon:presence:${roomId}:${userId}`, "1", { EX: ttlSeconds });
  return true;
}

export async function redisClearPresence(roomId: string, userId: string) {
  const redis = await getRedis();
  if (!redis) return false;
  await redis.del(`yazykon:presence:${roomId}:${userId}`);
  return true;
}

export async function redisGetPresence(roomId: string, userId: string) {
  const redis = await getRedis();
  if (!redis) return false;
  return (await redis.exists(`yazykon:presence:${roomId}:${userId}`)) === 1;
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

export async function redisSubscribe(channel: string, handler: (payload: unknown) => void) {
  const redis = await getRedis();
  if (!redis) return false;
  const subscriber = redis.duplicate();
  subscriberClients.push(subscriber);
  subscriber.on("error", error => console.error("Redis subscriber error", error));
  await subscriber.connect();
  await subscriber.subscribe(channel, raw => {
    try { handler(JSON.parse(raw)); } catch { /* ignore malformed pubsub payloads */ }
  });
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

export async function redisPublish(channel: string, payload: unknown) {
  const redis = await getRedis();
  if (!redis) return false;
  await redis.publish(channel, JSON.stringify(payload));
  return true;
}

export async function closeRedis() {
  if (client?.isOpen) await client.quit();
  for (const subscriber of subscriberClients) { if (subscriber.isOpen) await subscriber.quit(); }
  subscriberClients = [];
  client = null;
}
