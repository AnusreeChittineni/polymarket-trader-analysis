import duckdb
import pathlib

BASE_DIR = pathlib.Path(__file__).resolve().parents[1]
DATA_DIR = BASE_DIR / "data" / "polymarket"
MARKETS_DIR = DATA_DIR / "markets"
TRADES_DIR = DATA_DIR / "trades"

DB_PATH = BASE_DIR / "polymarket.duckdb"

print(f"Creating warehouse at: {DB_PATH}")

con = duckdb.connect(str(DB_PATH))


# ---------------------------
# Clean parquet file loader
# ---------------------------
def get_clean_parquet_list(folder: pathlib.Path):
    return [
        str(f)
        for f in folder.glob("*.parquet")
        if not f.name.startswith("._")
    ]


market_files = get_clean_parquet_list(MARKETS_DIR)
trade_files = get_clean_parquet_list(TRADES_DIR)


# ---------------------------
# Drop existing objects safely
# ---------------------------
con.execute("DROP TABLE IF EXISTS polymarket_markets;")
con.execute("DROP TABLE IF EXISTS polymarket_trades;")
con.execute("DROP VIEW IF EXISTS polymarket_trades;")


# ---------------------------
# Create materialized markets table
# ---------------------------
print("Loading markets table (materialized)...")

con.execute(f"""
CREATE TABLE polymarket_markets AS
SELECT *
FROM read_parquet({market_files});
""")


# ---------------------------
# Create trades VIEW (no disk blowup)
# ---------------------------
print("Creating trades view (zero-copy)...")

con.execute(f"""
CREATE VIEW polymarket_trades AS
SELECT *
FROM read_parquet({trade_files});
""")


print("Done.")
con.close()