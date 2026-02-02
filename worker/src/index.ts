import "dotenv/config";
import { Kafka } from "kafkajs";
import pino from "pino";

import { pool, bucketMinute } from "./db";
import { EnrichedAdEventSchema } from "./schemas";
import { EnrichedAdEvent } from "./interfaces";

const logger = pino({
  transport: { target: "pino-pretty", options: { colorize: true } }
});

const TOPIC = process.env.KAFKA_TOPIC ?? "ad-events";
const brokers = (process.env.KAFKA_BROKERS ?? "localhost:9092").split(",");
const clientId = process.env.KAFKA_CLIENT_ID ?? "ad-api";
const groupId = process.env.KAFKA_GROUP_ID ?? "ad-api";

const kafka = new Kafka({ clientId, brokers });
const consumer = kafka.consumer({ groupId });

const chunk = <T>(arr: T[], size: number): T[][] => {
  const out: T[][] = [];
  for (let i = 0; i < arr.length; i += size) out.push(arr.slice(i, i + size));
  return out;
}

type AggKey = string;
type AggRow = { bucket_start: string; ad_id: string; impressions: number; clicks: number };

const aggKey = (bucket_start: string, ad_id: string): AggKey => {
  return `${bucket_start}::${ad_id}`;
}

// batching for postgres request optimization
const processBatch = async (events: EnrichedAdEvent[]): Promise<{ processed: number; deduped: number }> => {
  if (events.length === 0) return { processed: 0, deduped: 0 };

  const client = await pool.connect();
  try {
    await client.query("BEGIN");
    const ids = events.map((e) => e.event_id);

    // insert all new event ids if existed skipping it
    const inserted = await client.query<{ event_id: string }>(
      `INSERT INTO processed_events(event_id)
       SELECT * FROM UNNEST($1::text[])
       ON CONFLICT DO NOTHING
       RETURNING event_id`,
      [ids]
    );

    const newIds = new Set(inserted.rows.map((r) => r.event_id));

    const newEvents = events.filter((e) => newIds.has(e.event_id));
    const deduped = events.length - newEvents.length;

    // if we haven't new ids close transaction 
    if (newEvents.length === 0) {
      await client.query("COMMIT");
      return { processed: 0, deduped };
    }

    const agg = new Map<AggKey, AggRow>();

    // count clicks and impressions, group by minute and ad_id
    for (const e of newEvents) {
      const bucket_start = bucketMinute(e.ts);
      const key = aggKey(bucket_start, e.ad_id);

      const row = agg.get(key) ?? { bucket_start, ad_id: e.ad_id, impressions: 0, clicks: 0 };

      if (e.type === "impression") row.impressions += 1;
      else row.clicks += 1;

      agg.set(key, row);
    }

    const rows = Array.from(agg.values());
    const chunkSize = 200;

    for (const part of chunk(rows, chunkSize)) {
      const values: string[] = [];
      const params: Array<string | number> = [];
      let i = 1;

      for (const r of part) {
        values.push(`($${i++}, $${i++}, $${i++}, $${i++})`);
        params.push(r.bucket_start, r.ad_id, r.impressions, r.clicks);
      }

      await client.query(
        `INSERT INTO ad_stats_minute(bucket_start, ad_id, impressions, clicks)
         VALUES ${values.join(",")}
         ON CONFLICT (bucket_start, ad_id)
         DO UPDATE SET
           impressions = ad_stats_minute.impressions + EXCLUDED.impressions,
           clicks = ad_stats_minute.clicks + EXCLUDED.clicks,
           updated_at = now()`,
        params
      );
    }

    await client.query("COMMIT");
    return { processed: newEvents.length, deduped };
  } catch (err) {
    await client.query("ROLLBACK");
    throw err;
  } finally {
    client.release();
  }
}

const main = async () => {
  await consumer.connect();
  await consumer.subscribe({ topic: TOPIC, fromBeginning: false });

  await consumer.run({
    eachBatchAutoResolve: false,

    eachBatch: async ({ batch, resolveOffset, heartbeat, commitOffsetsIfNecessary, isRunning, isStale }) => {
      const parsedEvents: EnrichedAdEvent[] = [];

      for (const message of batch.messages) {
        if (!isRunning() || isStale()) break;
        if (!message.value) {
          resolveOffset(message.offset);
          continue;
        }

        let raw: unknown;
        try {
          raw = JSON.parse(message.value.toString("utf-8"));
        } catch {
          logger.warn({ offset: message.offset, partition: batch.partition }, "Skip message, bad JSON");
          resolveOffset(message.offset);
          continue;
        }

        const parsed = EnrichedAdEventSchema.safeParse(raw);
        if (!parsed.success) {
          logger.warn(
            { offset: message.offset, partition: batch.partition, error: parsed.error.flatten() },
            "Skip message invalid event"
          );
          resolveOffset(message.offset);
          continue;
        }

        parsedEvents.push(parsed.data);
        resolveOffset(message.offset);
        // pass heartbeat to kafka if batch is too long
        if (parsedEvents.length % 200 === 0) await heartbeat();
      }

      if (parsedEvents.length === 0) {
        await commitOffsetsIfNecessary();
        await heartbeat();
        return;
      }

      try {
        const r = await processBatch(parsedEvents);
        logger.info(
          { topic: batch.topic, partition: batch.partition, batchSize: batch.messages.length, processed: r.processed, deduped: r.deduped },
          "Batch processed"
        );

        await commitOffsetsIfNecessary();
        await heartbeat();
      } catch (err) {
        logger.error({ err, topic: batch.topic, partition: batch.partition }, "batch_failed_no_offset_commit");
        await heartbeat();
      }
    }
  });

  logger.info("worker started (batch mode)");
}

main().catch((err) => {
  logger.error(err, "fatal");
  process.exit(1);
});