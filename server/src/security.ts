import { randomBytes, scryptSync, timingSafeEqual } from "node:crypto";

export const USERNAME_PATTERN = /^[a-z0-9_.-]{3,32}$/;

export function normalizeUsername(value: unknown): string {
  return typeof value === "string" ? value.trim().toLowerCase() : "";
}

export function validateCredentials(username: unknown, password: unknown): { username: string; password: string } | null {
  const normalized = normalizeUsername(username);
  const pass = typeof password === "string" ? password : "";
  if (!USERNAME_PATTERN.test(normalized) || pass.length < 8 || pass.length > 128) return null;
  return { username: normalized, password: pass };
}

export function safeEqualHex(actualHex: string, expectedHex: string): boolean {
  try {
    const actual = Buffer.from(actualHex, "hex");
    const expected = Buffer.from(expectedHex, "hex");
    return actual.length === expected.length && timingSafeEqual(actual, expected);
  } catch {
    return false;
  }
}


export function hashPassword(password: string): string {
  const salt = randomBytes(16).toString("hex");
  const hash = scryptSync(password, salt, 32, { N: 32768, r: 8, p: 2, maxmem: 64 * 1024 * 1024 }).toString("hex");
  return `v2$${salt}$${hash}`;
}

export function verifyPassword(password: string, stored: string): boolean {
  const parts = stored.split("$");
  if (parts.length === 3 && parts[0] === "v2") {
    const actual = scryptSync(password, parts[1], 32, { N: 32768, r: 8, p: 2, maxmem: 64 * 1024 * 1024 });
    const expected = Buffer.from(parts[2], "hex");
    return actual.length === expected.length && timingSafeEqual(actual, expected);
  }
  const legacy = stored.split(":");
  if (legacy.length !== 2) return false;
  const actual = scryptSync(password, legacy[0], 32).toString("hex");
  return safeEqualHex(actual, legacy[1]);
}
