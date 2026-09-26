import { defineConfig } from "@playwright/test";

const mediaOnlyRun = process.argv.some(arg => arg.includes("sfu-media-e2e.spec.ts"));

export default defineConfig({
  testDir: "./tests",
  timeout: 30000,
  use: {
    baseURL: process.env.BASE_URL ?? "http://localhost:5173",
    trace: "retain-on-failure"
  },
  webServer: process.env.BASE_URL || mediaOnlyRun
    ? undefined
    : {
        command: "npm --prefix ../web run dev -- --host 127.0.0.1",
        port: 5173,
        reuseExistingServer: true
      }
});
