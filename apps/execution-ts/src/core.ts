import * as dotenv from "dotenv";
import { z } from "zod";
import axios from "axios";
import path from "path";
import { fileURLToPath } from "url";
import { existsSync, readFileSync, writeFileSync, mkdirSync } from "fs";
import { Connection, PublicKey } from "@solana/web3.js";
import { getMint, TOKEN_PROGRAM_ID, TOKEN_2022_PROGRAM_ID } from "@solana/spl-token";

import type { DB } from "./db.js";
import {
  setMeta,
  pruneOlderThanMs,
  countRows,
  upsertTokens,
  insertSnapshots,
  insertPrices,
} from "./db.js";

// ---------- env & paths ----------
const __dirname = path.dirname(fileURLToPath(import.meta.url));
const REPO_ROOT = path.resolve(__dirname, "../../../");

dotenv.config(); // apps/execution-ts/.env
dotenv.config({ path: path.join(REPO_ROOT, ".env") }); // корневой .env

const Env = z.object({
  RPC_URL: z.string().default("https://api.mainnet-beta.solana.com"),
  TOKEN_LIMIT: z.coerce.number().int().positive().default(50),

  // Jupiter API
  JUP_API_KEY: z.string().optional().default(""),

  // оффлайн из .env (перебивается FORCE_ONLINE)
  JUP_OFFLINE: z.string().optional(),
  FORCE_ONLINE: z.string().optional(),

  JUP_TOKENS_CACHE_TTL_MS: z.coerce.number().int().positive().default(60 * 60 * 1000),

  // котировки
  QUOTES_ENABLED: z.string().optional(),
  QUOTE_MAX_TOKENS: z.coerce.number().int().positive().default(25),
  QUOTE_IMPACT_USD: z.coerce.number().positive().default(1000),
  QUOTE_SPREAD_USD: z.coerce.number().positive().default(20),
  QUOTE_BASE_MINT: z
    .string()
    .default("EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"), // USDC
});
const env = Env.parse(process.env);

const asBool = (v?: string) => {
  if (!v) return false;
  const s = v.trim().toLowerCase();
  return s === "1" || s === "true";
};

// Итоговый режим оффлайн/онлайн
const offline = asBool(env.JUP_OFFLINE) && !asBool(env.FORCE_ONLINE);
const quotesEnabled = asBool(env.QUOTES_ENABLED);

const DATA_DIR = path.join(REPO_ROOT, "data");
const TOKENS_CACHE_PATH = path.join(DATA_DIR, "jup_verified_cache.json");
const TOKENS_SEED_PATH = path.join(DATA_DIR, "tokens.seed.json");

console.log(
  `[exec] Quotes ${quotesEnabled ? "ON" : "OFF"} (offline=${offline}), base=${(env.QUOTE_BASE_MINT || "").slice(
    0,
    4
  )}…, max=${env.QUOTE_MAX_TOKENS}, $spread=${env.QUOTE_SPREAD_USD}, $impact=${env.QUOTE_IMPACT_USD}`
);

// ---------- consts & types ----------
const USDC_MINT = env.QUOTE_BASE_MINT;
type TokenRow = { address: string; symbol?: string; name?: string; decimals?: number; tags?: string[] };

export type SnapshotResult = {
  ts: number;
  savedTokens: number;
  prunedRows: { snapshots: number; prices: number } | null;
};

// ---------- main ----------
export async function snapshot(db: DB): Promise<SnapshotResult> {
  const ts = Date.now();

  // 1) verified-токены
  let tokens: TokenRow[] = [];
  try {
    tokens = await loadVerifiedTokens(env.TOKEN_LIMIT);
  } catch (e: any) {
    console.warn(`[exec] Не удалось получить список токенов: ${e?.message ?? e}`);
    tokens = [];
  }

  // 2) ретеншн + метаданные
  const cutoff = ts - 48 * 60 * 60 * 1000; // 48h
  const beforeSnaps = countRows(db, "snapshots");
  const beforePrices = countRows(db, "prices");
  pruneOlderThanMs(db, cutoff);
  const afterSnaps = countRows(db, "snapshots");
  const afterPrices = countRows(db, "prices");

  setMeta(db, "last_snapshot_ts", String(ts));
  setMeta(db, "rpc_url", env.RPC_URL);
  setMeta(db, "token_limit", String(env.TOKEN_LIMIT));

  if (!tokens.length) {
    return {
      ts,
      savedTokens: 0,
      prunedRows: { snapshots: beforeSnaps - afterSnaps, prices: beforePrices - afterPrices },
    };
  }

  // 3) базовые ончейн-поля
  const conn = new Connection(env.RPC_URL, "confirmed");
  const rpcLimit = pLimit(5);
  const rows: any[] = [];
  const tokenRows: any[] = [];
  const priceRows: Array<{ ts: number; mint: string; price_usd: number }> = [];

  await Promise.all(
    tokens.map((t) =>
      rpcLimit(async () => {
        try {
          const mintPk = new PublicKey(t.address);
          const accInfo = await conn.getAccountInfo(mintPk);
          if (!accInfo) return;

          const isToken2022 = accInfo.owner.equals(TOKEN_2022_PROGRAM_ID);
          const programId = isToken2022 ? TOKEN_2022_PROGRAM_ID : TOKEN_PROGRAM_ID;
          const mintInfo = await getMint(conn, mintPk, "confirmed", programId);

          const decimals = (typeof t.decimals === "number" ? t.decimals : mintInfo.decimals) ?? 0;
          const mintAuthorityNull = mintInfo.mintAuthority === null ? 1 : 0;
          const freezeAuthorityNull = mintInfo.freezeAuthority === null ? 1 : 0;

          rows.push({
            ts,
            mint: t.address,
            symbol: t.symbol ?? null,
            decimals,
            mint_authority_null: mintAuthorityNull,
            freeze_authority_null: freezeAuthorityNull,
            token2022_danger: 0, // TODO
            lp_verified: 1,      // TODO
            tvl_usd: null,
            impact_1k_pct: null,
            spread_pct: null,
            r_1m: 0,
            r_5m: 0,
            r_15m: 0,
            volume_5m: 0,
            unique_buyers_5m: 0,
            net_buy_usd_5m: 0,
            top10_pct: null,
            flags_json: null,
          });

          tokenRows.push({
            mint: t.address,
            symbol: t.symbol ?? null,
            name: t.name ?? null,
            decimals,
            verified: 1,
          });
        } catch {
          // skip
        }
      })
    )
  );

  // 4) котировки Jupiter (impact, spread, mid price→prices)
  const metricsMap: Record<string, { impact_1k_pct: number | null; spread_pct: number | null }> = {};

  if (!offline && quotesEnabled) {
    const toQuote = tokens
      .filter((t) => t.address !== USDC_MINT)
      .slice(0, Math.min(env.QUOTE_MAX_TOKENS, tokens.length));
    console.log(`[exec] Will quote ${toQuote.length} token(s) via Jupiter`);
    const qLimit = pLimit(2);

    await Promise.all(
      toQuote.map((t) =>
        qLimit(async () => {
          try {
            const dec = (typeof t.decimals === "number" ? t.decimals : 0) || 0;

            // Impact: покупка на QUOTE_IMPACT_USD (в USDC)
            const buyAmountUSDC = BigInt(Math.round(env.QUOTE_IMPACT_USD * 1_000_000));
            const qi = await jupQuote({
              inputMint: USDC_MINT,
              outputMint: t.address,
              amount: buyAmountUSDC.toString(),
            });
            const impactPct = qi?.priceImpactPct != null ? Number(qi.priceImpactPct) : null;

            // Spread + mid price: round-trip на QUOTE_SPREAD_USD
            const smallUsd = BigInt(Math.round(env.QUOTE_SPREAD_USD * 1_000_000));
            const qBuy = await jupQuote({
              inputMint: USDC_MINT,
              outputMint: t.address,
              amount: smallUsd.toString(),
            });

            let spreadPct: number | null = null;

            if (qBuy && qBuy.outAmount) {
              const outTok = Number(qBuy.outAmount) / 10 ** dec;
              const pBuy = Number(env.QUOTE_SPREAD_USD) / (outTok || 1e-12);

              const qSell = await jupQuote({
                inputMint: t.address,
                outputMint: USDC_MINT,
                amount: qBuy.outAmount,
              });

              if (qSell && qSell.outAmount) {
                const outUsd = Number(qSell.outAmount) / 1e6;
                const inTok = Number(qBuy.outAmount) / 10 ** dec;
                const pSell = (outUsd || 0) / (inTok || 1e-12);

                const mid = (pBuy + pSell) / 2;
                if (mid > 0 && Number.isFinite(mid)) {
                  spreadPct = ((pBuy - pSell) / mid) * 100;
                  priceRows.push({ ts, mint: t.address, price_usd: mid });
                }
              }
            }

            metricsMap[t.address] = { impact_1k_pct: impactPct, spread_pct: spreadPct };
          } catch {
            metricsMap[t.address] = { impact_1k_pct: null, spread_pct: null };
          }
        })
      )
    );
  }

  // База: USDC = $1
  priceRows.push({ ts, mint: USDC_MINT, price_usd: 1.0 });

  // 5) применяем метрики и сохраняем
  if (Object.keys(metricsMap).length) {
    for (const r of rows) {
      const m = metricsMap[r.mint];
      if (!m) continue;
      if (typeof m.impact_1k_pct === "number") r.impact_1k_pct = m.impact_1k_pct;
      if (typeof m.spread_pct === "number") r.spread_pct = m.spread_pct;
    }
  }

  if (tokenRows.length) upsertTokens(db, tokenRows);
  if (rows.length) insertSnapshots(db, rows);
  if (priceRows.length) insertPrices(db, priceRows);

  return {
    ts,
    savedTokens: rows.length,
    prunedRows: { snapshots: beforeSnaps - afterSnaps, prices: beforePrices - afterPrices },
  };
}

// ---------- Jupiter helpers ----------
type QuoteResp = {
  inputMint: string;
  outputMint: string;
  inAmount: string;
  outAmount: string;
  priceImpactPct?: string | number | null;
};

async function jupQuote(params: { inputMint: string; outputMint: string; amount: string }): Promise<QuoteResp | null> {
  const base = env.JUP_API_KEY ? "https://api.jup.ag" : "https://lite-api.jup.ag";
  const url =
    `${base}/swap/v1/quote` +
    `?inputMint=${encodeURIComponent(params.inputMint)}` +
    `&outputMint=${encodeURIComponent(params.outputMint)}` +
    `&amount=${encodeURIComponent(params.amount)}` +
    `&slippageBps=50` +
    `&restrictIntermediateTokens=true`;

  const headers: Record<string, string> = {
    accept: "application/json",
    "user-agent": "solana-sniper/0.1",
  };
  if (env.JUP_API_KEY) headers["x-api-key"] = env.JUP_API_KEY;

  try {
    const data = await fetchJsonWithBackoff(url, headers, 3, 1500);
    return data as QuoteResp;
  } catch (e: any) {
    const status = e?.response?.status;
    if (status === 429) console.warn("429 from Jupiter (quote), skip");
    return null;
  }
}

// ---------- token-list helpers ----------
function readSeed(limit: number): TokenRow[] {
  try {
    if (existsSync(TOKENS_SEED_PATH)) {
      const arr = JSON.parse(readFileSync(TOKENS_SEED_PATH, "utf-8"));
      if (Array.isArray(arr) && arr.length) return arr.slice(0, limit);
    }
  } catch {}
  return [
    { address: "So11111111111111111111111111111111111111112", symbol: "SOL", name: "Solana", decimals: 9, tags: ["verified"] },
    { address: USDC_MINT, symbol: "USDC", name: "USD Coin", decimals: 6, tags: ["verified"] },
  ].slice(0, limit);
}

function readCache(limit: number, soft = false): TokenRow[] | null {
  try {
    if (!existsSync(TOKENS_CACHE_PATH)) return null;
    const raw = JSON.parse(readFileSync(TOKENS_CACHE_PATH, "utf-8"));
    if (!raw || !Array.isArray(raw.items)) return null;
    const age = Date.now() - (raw.ts ?? 0);
    if (soft) return (raw.items as TokenRow[]).slice(0, limit);
    if (age < env.JUP_TOKENS_CACHE_TTL_MS) return (raw.items as TokenRow[]).slice(0, limit);
  } catch {}
  return null;
}

async function loadVerifiedTokens(limit: number): Promise<TokenRow[]> {
  mkdirSync(DATA_DIR, { recursive: true });

  if (offline) {
    const cached = readCache(limit, true) ?? readSeed(limit);
    try {
      writeFileSync(TOKENS_CACHE_PATH, JSON.stringify({ ts: Date.now(), items: cached }, null, 2));
    } catch {}
    return cached;
  }

  const cached = readCache(limit);
  if (cached) return cached;

  const base = env.JUP_API_KEY ? "https://api.jup.ag" : "https://lite-api.jup.ag";
  const headers: Record<string, string> = { accept: "application/json", "user-agent": "solana-sniper/0.1" };
  if (env.JUP_API_KEY) headers["x-api-key"] = env.JUP_API_KEY;

  let tokens: TokenRow[] | null = null;
  try {
    // токены с тегом verified (v2)
    const urlV2 = `${base}/tokens/v2/tag?query=verified`;
    const data = await fetchJsonWithBackoff(urlV2, headers, 3, 1500);
    const arr = (Array.isArray(data) ? data : (data as any)?.tokens) as any[];
    if (Array.isArray(arr) && arr.length) {
      tokens = arr
        .filter((x) => typeof x?.id === "string")
        .map((x) => ({ address: x.id, symbol: x.symbol, name: x.name, decimals: x.decimals, tags: x.tags }));
    }
  } catch {}

  const finalList = (tokens && tokens.length ? tokens : readCache(limit, true) ?? readSeed(limit)).slice(0, limit);
  try {
    writeFileSync(TOKENS_CACHE_PATH, JSON.stringify({ ts: Date.now(), items: finalList }, null, 2));
  } catch {}
  return finalList;
}

// ---------- generic helpers ----------
function pLimit(concurrency: number) {
  let active = 0;
  const queue: Array<() => void> = [];
  const next = () => {
    active--;
    if (queue.length) queue.shift()!();
  };
  return function <T>(fn: () => Promise<T>): Promise<T> {
    return new Promise<T>((resolve, reject) => {
      const run = () => {
        active++;
        fn()
          .then((v) => {
            resolve(v);
            next();
          })
          .catch((e) => {
            reject(e);
            next();
          });
      };
      active < concurrency ? run() : queue.push(run);
    });
  };
}

async function fetchJsonWithBackoff(
  url: string,
  headers: Record<string, string>,
  attempts = 3,
  maxDelayMs = 1500
): Promise<any> {
  let lastErr: any;
  for (let i = 0; i < attempts; i++) {
    try {
      const { data } = await axios.get(url, { timeout: 10000, headers });
      return data;
    } catch (e: any) {
      const status = e?.response?.status;
      lastErr = e;
      if (status === 429 || (status >= 500 && status < 600)) {
        const delay = Math.min(maxDelayMs, 400 * Math.pow(2, i)) + Math.floor(Math.random() * 150);
        console.warn(`Server responded with ${status}.  Retrying after ${delay}ms delay...`);
        await new Promise((r) => setTimeout(r, delay));
        continue;
      }
      throw e;
    }
  }
  throw lastErr;
}
