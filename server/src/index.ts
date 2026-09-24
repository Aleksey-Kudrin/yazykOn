import cors from "cors";
import express from "express";
import helmet from "helmet";

const app = express();
const port = Number(process.env.PORT ?? 3000);

app.use(helmet());
app.use(cors());
app.use(express.json());

app.get("/api/health", (_req, res) => {
  res.json({
    ok: true,
    service: "yazykOn-server",
    version: "0.1.0",
    time: new Date().toISOString()
  });
});

app.get("/api", (_req, res) => {
  res.json({
    name: "языкOn",
    message: "Backend API is running"
  });
});

app.listen(port, "0.0.0.0", () => {
  console.log(`языкOn server listening on http://0.0.0.0:${port}`);
});
