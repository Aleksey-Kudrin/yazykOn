import { defineConfig } from "@playwright/test";

export default defineConfig({
  testDir: "./tests",
  // Live SFU failover/soak probes intentionally run longer than the browser default.
  // Individual tests still impose their own tighter assertions where appropriate.
  timeout: 300000,
  use: {
    baseURL: process.env.BASE_URL ?? "http://localhost:5173",
    trace: "retain-on-failure",
    permissions: ["camera", "microphone"],
    launchOptions: {
      args: [
        "--use-fake-device-for-media-stream",
        "--use-fake-ui-for-media-stream"
      ]
    }
  },
  webServer: process.env.BASE_URL ? undefined : {
    command: "npm --prefix ../web run dev -- --host 127.0.0.1",
    port: 5173,
    reuseExistingServer: true
  }
});
