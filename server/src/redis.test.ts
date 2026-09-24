import test from "node:test";
import assert from "node:assert/strict";
import { redisClearPresence, redisEnabled, redisPublish, redisSetPresence } from "./redis.js";

test("Redis is optional when REDIS_URL is unset", async () => {
  const previous = process.env.REDIS_URL;
  delete process.env.REDIS_URL;
  assert.equal(redisEnabled(), false);
  assert.equal(await redisSetPresence("ROOM", "USER"), false);
  assert.equal(await redisClearPresence("ROOM", "USER"), false);
  assert.equal(await redisPublish("room:test", { ok: true }), false);
  if (previous !== undefined) process.env.REDIS_URL = previous;
});
