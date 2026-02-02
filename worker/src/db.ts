import { Pool } from "pg";

export const pool = new Pool({
  host: process.env.PGHOST ?? "localhost",
  port: Number(process.env.PGPORT ?? 5432),
  user: process.env.PGUSER ?? "app",
  password: process.env.PGPASSWORD ?? "app",
  database: process.env.PGDATABASE ?? "analytics",
  max: 10
});

// return beginning of minute to group stats by minute
export const bucketMinute = (tsIso: string): string => {
  const d = new Date(tsIso);
  d.setUTCSeconds(0, 0);
  return d.toISOString();
}
