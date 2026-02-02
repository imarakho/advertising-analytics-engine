import "dotenv/config";
import express from "express";
import rateLimit from "express-rate-limit";
import Redis from "ioredis";
import pino from "pino";
import { RedisReply, RedisStore } from "rate-limit-redis";

import { producer } from "./kafka";
import { publishWithRetry } from './publish'
import { AdEventSchema } from "./schemas";
import { EnrichedAdEvent } from "./interfaces";

const logger = pino({
  transport: { target: "pino-pretty", options: { colorize: true } }
});

const app = express();
app.use(express.json({ limit: "1mb" }));

const PORT = Number(process.env.PORT ?? 3000);

const windowMs = Number(process.env.RATE_LIMIT_WINDOW_MS ?? 1000);
const max = Number(process.env.RATE_LIMIT_MAX ?? 200);

const redisUrl = process.env.REDIS_URL ?? "redis://localhost:6379";
const redis = new Redis(redisUrl);

app.use(
  rateLimit({
    windowMs,
    max,
    standardHeaders: true,
    legacyHeaders: false,
    store: new RedisStore({
      sendCommand: (...args: string[]) => redis.call(...args) as Promise<RedisReply>,
    })
  })
);

app.post("/events", (req, res) => {
  const parsed = AdEventSchema.safeParse(req.body);
  if (!parsed.success) {
    return res.status(400).json({
      error: "Invalid payload",
      details: parsed.error.flatten()
    });
  }

  const input = parsed.data;
  const enriched: EnrichedAdEvent = {
    ...input,
    ts: input.ts ?? new Date().toISOString(),
    received_at: new Date().toISOString()
  };

  res.status(202).json({ status: "accepted", event_id: enriched.event_id });

  publishWithRetry(enriched).catch((err) => {
    logger.error({ err, event_id: enriched.event_id }, "Failed to publish event to kafka after retries");
  });
});

app.get("/health", (_req, res) => res.json({ ok: true }));

async function main() {
  await producer.connect();
  app.listen(PORT, () => logger.info(`API listening on http://localhost:${PORT}`));
}

main().catch((err) => {
  logger.error(err, "fatal");
  process.exit(1);
});
