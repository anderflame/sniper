import { Command } from "commander";
import { openDB, DB_PATH } from "./db.js";
import { snapshot } from "./core.js";

const program = new Command();
program.name("exec").description("execution CLI").version("0.0.1");

program
  .command("snapshot")
  .description("take snapshot and store in sqlite")
  .action(async () => {
    console.log(`[exec] Using DB: ${DB_PATH}`);
    console.log(`[exec] Env: QUOTES_ENABLED=${process.env.QUOTES_ENABLED ?? "-"} JUP_OFFLINE=${process.env.JUP_OFFLINE ?? "-"}`);
    const db = openDB();
    try {
      const res = await snapshot(db);
      console.log(`[exec] Snapshot ts=${res.ts}`);
      console.log(`[exec] Pruned: snapshots(${res.prunedRows?.snapshots ?? 0}), prices(${res.prunedRows?.prices ?? 0})`);
      console.log(`[exec] Saved tokens: ${res.savedTokens}`);
    } catch (e: any) {
      console.error("[exec] Snapshot failed:", e?.message ?? e);
      process.exitCode = 1;
    } finally {
      db.close();
    }
  });

program.parse();
