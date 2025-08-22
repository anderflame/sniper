import json
import os
import sqlite3
import sys
from typing import Any, Dict, List, Optional, Tuple

import typer

from score import Row, score_row
from rich.console import Console
from rich.table import Table

from .score import Row, score_row  # <-- логика вынесена в score.py

app = typer.Typer(add_completion=False)
console = Console()

DB_PATH = os.path.normpath("./data/metrics.db")
EXPORT_JSON_PATH = os.path.normpath("./data/top25.json")


def fetch_latest_rows(db_path: str) -> Tuple[int, List[Row]]:
    con = sqlite3.connect(db_path)
    con.row_factory = sqlite3.Row
    cur = con.cursor()

    cur.execute("SELECT MAX(ts) AS max_ts FROM snapshots")
    row = cur.fetchone()
    if not row or row["max_ts"] is None:
        con.close()
        return (0, [])

    ts = int(row["max_ts"])
    cur.execute(
        """
        SELECT s.mint,
               COALESCE(s.symbol, t.symbol) AS symbol,
               s.decimals,
               s.mint_authority_null,
               s.freeze_authority_null,
               s.token2022_danger,
               s.lp_verified,
               s.tvl_usd,
               s.impact_1k_pct,
               s.spread_pct,
               s.r_1m, s.r_5m, s.r_15m,
               s.volume_5m,
               COALESCE(s.unique_buyers_5m, 0) AS unique_buyers_5m,
               COALESCE(s.net_buy_usd_5m, 0.0) AS net_buy_usd_5m,
               s.top10_pct
        FROM snapshots s
        LEFT JOIN tokens t USING(mint)
        WHERE s.ts = ?
        """,
        (ts,),
    )
    rows: List[Row] = []
    for x in cur.fetchall():
        rows.append(
            Row(
                mint=x["mint"],
                symbol=x["symbol"],
                decimals=int(x["decimals"] or 0),
                mint_authority_null=int(x["mint_authority_null"] or 0),
                freeze_authority_null=int(x["freeze_authority_null"] or 0),
                token2022_danger=int(x["token2022_danger"] or 0),
                lp_verified=int(x["lp_verified"] or 0),
                tvl_usd=(float(x["tvl_usd"]) if x["tvl_usd"] is not None else None),
                impact_1k_pct=(float(x["impact_1k_pct"]) if x["impact_1k_pct"] is not None else None),
                spread_pct=(float(x["spread_pct"]) if x["spread_pct"] is not None else None),
                r_1m=float(x["r_1m"] or 0.0),
                r_5m=float(x["r_5m"] or 0.0),
                r_15m=float(x["r_15m"] or 0.0),
                volume_5m=float(x["volume_5m"] or 0.0),
                unique_buyers_5m=int(x["unique_buyers_5m"] or 0),
                net_buy_usd_5m=float(x["net_buy_usd_5m"] or 0.0),
                top10_pct=(float(x["top10_pct"]) if x["top10_pct"] is not None else None),
            )
        )
    con.close()
    return (ts, rows)


def print_table(items: List[Dict[str, Any]]):
    table = Table(show_header=True, header_style="bold")
    table.add_column("#", justify="right")
    table.add_column("Symbol")
    table.add_column("Mint")
    table.add_column("Score", justify="right")
    table.add_column("Safety", justify="right")
    table.add_column("Spread%", justify="right")
    table.add_column("Impact1k%", justify="right")
    table.add_column("r5m%", justify="right")
    table.add_column("NetBuy5m$", justify="right")

    for i, it in enumerate(items, start=1):
        mint_short = it["mint"][:4] + "…" + it["mint"][-4:]
        spread = it.get("spread_pct")
        impact = it.get("impact_1k_pct")
        r5m = it.get("r_5m", 0.0)
        netbuy = it.get("net_buy_usd_5m", 0.0)
        table.add_row(
            str(i),
            it.get("symbol") or "",
            mint_short,
            f"{it['score']*100:5.1f}",
            f"{it['safety']*100:5.1f}",
            (f"{spread:5.2f}" if spread is not None else "  –  "),
            (f"{impact:5.2f}" if impact is not None else "  –  "),
            f"{r5m*100:5.2f}",
            f"{netbuy:,.0f}",
        )
    console.print(table)


@app.command()
def score(
    print_: bool = typer.Option(True, "--print/--no-print", help="Печатать таблицу в консоль"),
    export_json: bool = typer.Option(True, "--export-json/--no-export-json", help="Сохранить ./data/top25.json"),
    limit: int = typer.Option(25, "--limit", min=1, max=100, help="Сколько строк вывести"),
):
    if not os.path.exists(DB_PATH):
        typer.secho(f"DB not found: {DB_PATH}", fg=typer.colors.RED)
        raise typer.Exit(code=2)

    ts, rows = fetch_latest_rows(DB_PATH)
    if ts == 0 or not rows:
        typer.secho("Нет данных снапшота. Сначала запусти TS snapshot.", fg=typer.colors.YELLOW)
        raise typer.Exit(code=0)

    items: List[Dict[str, Any]] = []
    for r in rows:
        parts = score_row(r)
        items.append({**r.__dict__, **parts})

    items.sort(key=lambda x: x["score"], reverse=True)
    top = items[:limit]

    if print_:
        console.print(f"[bold]Top-{min(limit, len(items))} @ ts={ts}[/bold]")
        print_table(top)

    if export_json:
        os.makedirs(os.path.dirname(EXPORT_JSON_PATH), exist_ok=True)
        with open(EXPORT_JSON_PATH, "w", encoding="utf-8") as f:
            json.dump(
                [
                    {
                        "mint": it["mint"],
                        "symbol": it.get("symbol"),
                        "score": round(it["score"], 6),
                        "safety": round(it["safety"], 6)
                    }
                    for it in top
                ],
                f,
                ensure_ascii=False,
                indent=2,
            )
        typer.secho(f"Saved: {EXPORT_JSON_PATH}", fg=typer.colors.GREEN)


if __name__ == "__main__":
    app()
