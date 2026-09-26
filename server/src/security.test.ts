import test from "node:test";
import assert from "node:assert/strict";
import { safeEqualHex, validateCredentials, verifyPassword } from "./security.js";

test("validateCredentials normalizes usernames and enforces bounds", () => {
  assert.deepEqual(validateCredentials(" Alice ", "12345678"), { username: "alice", password: "12345678" });
  assert.equal(validateCredentials("ab", "12345678"), null);
  assert.equal(validateCredentials("alice", "short"), null);
  assert.equal(validateCredentials("alice", "x".repeat(129)), null);
});

test("safeEqualHex compares values without throwing on malformed input", () => {
  assert.equal(safeEqualHex("aabb", "aabb"), true);
  assert.equal(safeEqualHex("aabb", "aacc"), false);
  assert.equal(safeEqualHex("bad", "aabb"), false);
});


test("verifyPassword rejects malformed hashes without throwing", () => {
  assert.equal(verifyPassword("password", "v2$bad"), false);
  assert.equal(verifyPassword("password", "v2$salt$not-hex"), false);
  assert.equal(verifyPassword("password", "broken"), false);
});
