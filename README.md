# Advertising Analytics Ingestion Engine (Express + Kafka + PostgreSQL + Redis)

Simplified analytics ingestion pipeline for an advertising platform.

## Components
- **API (Node.js + Express, TypeScript)**: accepts JSON events over HTTP and acknowledges immediately (non-blocking).
- **Kafka**: transport layer for high-volume event ingestion.
- **Worker (TypeScript)**: consumes Kafka stream, validates events, aggregates in real time, and persists to Postgres.
- **PostgreSQL**: stores aggregated stats and processed event ids for idempotency.
- **Redis**: distributed rate limiting for API (handles spikes + multiple API instances).

## Event format

### POST `/events`
Example payload:

```json
{
  "event_id": "0f7a4d9a-2f2e-4c6c-9d1b-7b0c9c9b3c31",
  "type": "impression",
  "ad_id": "1",
  "campaign_id": "1",
  "user_id": "1",
  "ts": "2026-02-02T12:34:56.789Z"
}
```

Fields:
- `event_id` (string) – unique id (UUID recommended)
- `type` – `"impression"` | `"click"`
- `ad_id` (string)
- optional: `campaign_id`, `user_id`
- `ts` (ISO datetime) – optional; API sets to `now()` if missing

API enriches:
- `received_at` (ISO datetime)

Expected API response:
```json
{"status":"accepted","event_id":"..."}
```

**Note:** API responds immediately (202) and publishes to Kafka asynchronously.

---

## Storage schema (Postgres)

### Tables
- `processed_events(event_id PK)` — used for deduplication/idempotency
- `ad_stats_minute(bucket_start, ad_id PK)` — aggregated counters by minute

Aggregation bucket:
- `bucket_start = date_trunc('minute', ts)` (UTC)


## Prerequisites
- Node.js 18+ (recommended 20+)
- Docker + Docker Compose

---

## Setup

### 1) Start infrastructure (Kafka, Postgres, Redis)
```bash
docker-compose up -d
```

### 2) Install dependencies
```bash
npm install
```

### 3) Configure environment
Create `.env` in repo root, .env.example is attached

## Run

### API
```bash
npm run dev:api
```

### Worker (batch mode)
```bash
npm run dev:worker
```

---

## API

### Healthcheck
```bash
curl http://localhost:3000/health
```

### Send one event
```bash
curl -X POST http://localhost:3000/events \
  -H "Content-Type: application/json" \
  -d '{"event_id":"qssd-dsdf-sdfd-fdf-dfdc","type":"impression","ad_id":"1","ts":"2026-02-01T12:34:56.789Z"}'
```

Expected response:
```json
{"status":"accepted","event_id":"qssd-dsdf-sdfd-fdf-dfdc"}
```

---

## Rate limiting (Redis-backed)
The API uses `express-rate-limit` + Redis store.
If you exceed the configured rate, API returns **HTTP 429**.

Tune:
- `RATE_LIMIT_WINDOW_MS`
- `RATE_LIMIT_MAX`

---

## Worker: validation + batching

### Validation
Worker validates Kafka messages with Zod:
- Invalid JSON or invalid schema is skipped (does not crash the worker).

### Batching
Worker uses KafkaJS `eachBatch` and processes messages in batches:
- Parses + validates messages
- Bulk-inserts `event_id` to dedupe
- Aggregates counters in-memory
- Bulk-upserts aggregated rows
- Commits Kafka offsets only after successful DB commit

You will see logs like:
- `batch_processed` with `batchSize`, `processed`, `deduped`

## Linting

Run ESLint:
```bash
npm run lint
```

Auto-fix:
```bash
npm run lint:fix
```

## Testing

Run unit tests:
```bash
npm run test
```
