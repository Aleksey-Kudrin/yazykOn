import { createServer } from "node:http";
import path from "node:path";
import { fileURLToPath } from "node:url";
import express from "express";
import { app, initDatabase, db } from "./server/src/index.js";
import { attachSignaling } from "./server/src/signaling.js";
import { closeRedis } from "./server/src/redis.js";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const port = Number(process.env.PORT || 3000);

const server = createServer(app);
attachSignaling(server);

async function start() {
  await initDatabase();

  if (process.env.NODE_ENV !== "production") {
    const { createServer: createViteServer } = await import("vite");
    const vite = await createViteServer({
      server: { middlewareMode: true },
      appType: "spa",
      root: path.resolve(__dirname, "web"),
    });
    app.use(vite.middlewares);
  } else {
    const distPath = path.resolve(__dirname, "dist");
    app.use(express.static(distPath));
    app.get("*", (req, res, next) => {
      if (req.path.startsWith("/api") || req.path === "/ws") {
        return next();
      }
      res.sendFile(path.resolve(distPath, "index.html"));
    });
  }

  server.listen(port, "0.0.0.0", () => {
    console.log(`языкOn full-stack server running on http://0.0.0.0:${port}`);
    console.log(`WebRTC signaling running on ws://0.0.0.0:${port}/ws`);
  });
}

async function shutdown(signal: string) {
  console.log(`языкOn shutting down (${signal})`);
  server.close(async () => {
    await db?.end?.();
    await closeRedis();
    process.exit(0);
  });
  setTimeout(() => process.exit(1), 5000).unref();
}

process.on("SIGTERM", () => void shutdown("SIGTERM"));
process.on("SIGINT", () => void shutdown("SIGINT"));

start().catch((err) => {
  console.error("Failed to start server:", err);
  process.exit(1);
});
