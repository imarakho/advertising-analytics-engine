CREATE TABLE IF NOT EXISTS processed_events (
  event_id TEXT PRIMARY KEY,
  received_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS ad_stats_minute (
  bucket_start TIMESTAMPTZ NOT NULL,
  ad_id TEXT NOT NULL,
  impressions BIGINT NOT NULL DEFAULT 0,
  clicks BIGINT NOT NULL DEFAULT 0,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (bucket_start, ad_id)
);
