import { createClient, type RedisClientType } from "redis";

let client: RedisClientType | null = null;
let connectPromise: Promise<RedisClientType | null> | null = null;

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

export async function redisPublish(channel: string, payload: unknown) {
  const redis = await getRedis();
  if (!redis) return false;
  await redis.publish(channel, JSON.stringify(payload));
  return true;
}

export async function closeRedis() {
  if (client?.isOpen) await client.quit();
  client = null;
}
