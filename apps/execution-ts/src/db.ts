import BetterSqlite3 from "better-sqlite3";
import { mkdirSync } from "fs";
import path from "path";
import { fileURLToPath } from "url";

// --- тип БД ---
export type DB = InstanceType<typeof BetterSqlite3>;

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const REPO_ROOT = path.resolve(__dirname, "../../../");
export const DB_PATH = path.join(REPO_ROOT, "data", "metrics.db");

export function openDB(): DB {
  mkdirSync(path.dirname(DB_PATH), { recursive: true });
  const db = new BetterSqlite3(DB_PATH);
  db.pragma("journal_mode = WAL");
  db.pragma("synchronous = NORMAL");
  db.pragma("foreign_keys = ON");
  migrate(db);
  return db;
}

function migrate(db: DB) {
  db.exec(`
  CREATE TABLE IF NOT EXISTS snapshots (
    ts INTEGER NOT NULL,
    mint TEXT NOT NULL,
    symbol TEXT,
    decimals INTEGER NOT NULL DEFAULT 0,
    mint_authority_null INTEGER NOT NULL DEFAULT 0,
    freeze_authority_null INTEGER NOT NULL DEFAULT 0,
    token2022_danger INTEGER NOT NULL DEFAULT 0,
    lp_verified INTEGER NOT NULL DEFAULT 0,
    tvl_usd REAL,
    impact_1k_pct REAL,
    spread_pct REAL,
    r_1m REAL, r_5m REAL, r_15m REAL,
    volume_5m REAL,
    unique_buyers_5m INTEGER,
    net_buy_usd_5m REAL,
    top10_pct REAL,
    flags_json TEXT,
    PRIMARY KEY (ts, mint)
  );

  CREATE TABLE IF NOT EXISTS prices (
    ts INTEGER NOT NULL,
    mint TEXT NOT NULL,
    price_usd REAL NOT NULL,
    PRIMARY KEY (ts, mint)
  );

  CREATE TABLE IF NOT EXISTS tokens (
    mint TEXT PRIMARY KEY,
    symbol TEXT,
    name TEXT,
    decimals INTEGER NOT NULL DEFAULT 0,
    verified INTEGER NOT NULL DEFAULT 0
  );

  CREATE TABLE IF NOT EXISTS meta (
    key TEXT PRIMARY KEY,
    value TEXT
  );
  `);
}

export function setMeta(db: DB, key: string, value: string) {
  db.prepare(
    `INSERT INTO meta(key,value) VALUES(?,?)
     ON CONFLICT(key) DO UPDATE SET value=excluded.value`
  ).run(key, value);
}

export function pruneOlderThanMs(db: DB, olderThanMs: number) {
  db.prepare(`DELETE FROM snapshots WHERE ts < ?`).run(olderThanMs);
  db.prepare(`DELETE FROM prices   WHERE ts < ?`).run(olderThanMs);
}

export function countRows(db: DB, table: string): number {
  const row = db.prepare(`SELECT COUNT(*) as c FROM ${table}`).get() as { c: number };
  return row.c;
}

export function upsertTokens(
  db: DB,
  rows: Array<{ mint: string; symbol?: string | null; name?: string | null; decimals: number; verified: number }>
) {
  if (!rows.length) return;
  const stmt = db.prepare(`
    INSERT INTO tokens (mint, symbol, name, decimals, verified)
    VALUES (?, ?, ?, ?, ?)
    ON CONFLICT(mint) DO UPDATE SET
      symbol=excluded.symbol,
      name=excluded.name,
      decimals=excluded.decimals,
      verified=excluded.verified
  `);
  const tx = db.transaction((arr: any[]) => {
    for (const r of arr) stmt.run(r.mint, r.symbol ?? null, r.name ?? null, r.decimals, r.verified);
  });
  tx(rows);
}

export function insertSnapshots(db: DB, rows: Array<any>) {
  if (!rows.length) return;
  const stmt = db.prepare(`
    INSERT INTO snapshots (
      ts, mint, symbol, decimals,
      mint_authority_null, freeze_authority_null, token2022_danger, lp_verified,
      tvl_usd, impact_1k_pct, spread_pct,
      r_1m, r_5m, r_15m, volume_5m,
      unique_buyers_5m, net_buy_usd_5m, top10_pct, flags_json
    ) VALUES (
      ?, ?, ?, ?,
      ?, ?, ?, ?,
      ?, ?, ?,
      ?, ?, ?, ?,
      ?, ?, ?, ?
    )
  `);

  const tx = db.transaction((arr: any[]) => {
    for (const r of arr) {
      stmt.run(
        r.ts,
        r.mint,
        r.symbol ?? null,
        r.decimals ?? 0,
        r.mint_authority_null ?? 0,
        r.freeze_authority_null ?? 0,
        r.token2022_danger ?? 0,
        r.lp_verified ?? 0,
        r.tvl_usd ?? null,
        r.impact_1k_pct ?? null,
        r.spread_pct ?? null,
        r.r_1m ?? 0,
        r.r_5m ?? 0,
        r.r_15m ?? 0,
        r.volume_5m ?? 0,
        r.unique_buyers_5m ?? 0,
        r.net_buy_usd_5m ?? 0,
        r.top10_pct ?? null,
        r.flags_json ?? null
      );
    }
  });
  tx(rows);
}

export function insertPrices(
  db: DB,
  rows: Array<{ ts: number; mint: string; price_usd: number }>
) {
  if (!rows.length) return;
  const stmt = db.prepare(
    `INSERT OR REPLACE INTO prices (ts, mint, price_usd) VALUES (?, ?, ?)`
  );
  const tx = db.transaction((arr: any[]) => {
    for (const r of arr) stmt.run(r.ts, r.mint, r.price_usd);
  });
  tx(rows);
}
