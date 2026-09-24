import { timingSafeEqual } from "node:crypto";

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
