import os, sqlite3, json

DB_PATH = os.path.normpath("./data/metrics.db")

con = sqlite3.connect(DB_PATH)
con.row_factory = sqlite3.Row
cur = con.cursor()

# Какие есть таблицы
tables = [r["name"] for r in cur.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()]
print("Tables:", tables)

# Последний снэпшот
ts = cur.execute("SELECT MAX(ts) AS ts FROM snapshots").fetchone()["ts"]
print("Last ts:", ts)

count = cur.execute("SELECT COUNT(*) AS c FROM snapshots WHERE ts=?", (ts,)).fetchone()["c"]
print("Rows in last snapshot:", count)

# Несколько строк для примера
rows = cur.execute("""
  SELECT s.mint, COALESCE(s.symbol,t.symbol) AS symbol, s.decimals,
         s.mint_authority_null, s.freeze_authority_null
  FROM snapshots s LEFT JOIN tokens t USING(mint)
  WHERE s.ts=? ORDER BY s.mint LIMIT 10
""", (ts,)).fetchall()

for r in rows:
  print(dict(r))

con.close()