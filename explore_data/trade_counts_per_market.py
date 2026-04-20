import duckdb
import pathlib

# Setup paths
SCRIPT_DIR = pathlib.Path(__file__).parent
DATA_ROOT = SCRIPT_DIR.parent / "data"

# Glob patterns that avoid the "._" metadata files
poly_trades_glob = str(DATA_ROOT / "polymarket" / "trades" / "[!.]*.parquet").replace("\\", "/")
poly_markets_glob = str(DATA_ROOT / "polymarket" / "markets" / "[!.]*.parquet").replace("\\", "/")

con = duckdb.connect()

def explore_categories():
    print("Exploring PolyMarket Categories...")

    # 1. First, let's categorize the markets (much smaller than trades)
    con.execute(f"""
        CREATE OR REPLACE VIEW categorized_markets AS 
        SELECT 
            market_maker_address,
            question,
            volume as market_total_volume,
            CASE 
                WHEN question ILIKE '%crypto%' OR question ILIKE '%bitcoin%' OR question ILIKE '%eth%' OR question ILIKE '%solana%' THEN 'Crypto'
                WHEN question ILIKE '%election%' OR question ILIKE '%trump%' OR question ILIKE '%poll%' OR question ILIKE '%biden%' OR question ILIKE '%harris%' OR question ILIKE '%netanyahu%' THEN 'Politics'
                WHEN question ILIKE '%nfl%' OR question ILIKE '%nba%' OR question ILIKE '%mlb%' OR question ILIKE '%championship%' OR question ILIKE '%win%' OR question ILIKE '%close%' OR question ILIKE '%points%' OR question ILIKE '%lose%' THEN 'Sports'
                WHEN question ILIKE '%fed%' OR question ILIKE '%rate%' OR question ILIKE '%inflation%' OR question ILIKE '%recession%' or question ILIKE '%$%' THEN 'Economics'
                WHEN question ILIKE '%oscars%' OR question ILIKE '%grammys%' OR question ILIKE '%movie%' or question ILIKE '%album%' or question ILIKE '%music%' or question ILIKE '%netflix%' or question ILIKE '%divorce%' or question ILIKE '%Zendaya%'  or question ILIKE '%Coachella%' or question ILIKE '%sstreams%' or question ILIKE '%Kardashian%' THEN 'Pop Culture'
                ELSE 'Other/Misc'
            END AS category
        FROM read_parquet('{poly_markets_glob}')
    """)

    # 2. Peek at the Market-level distribution
    print("\n--- Market Distribution by Category ---")
    market_dist = con.execute("""
        SELECT 
            category, 
            COUNT(*) as num_unique_trades,
            ROUND(SUM(market_total_volume)/1e6, 2) as total_vol_m_usd
        FROM categorized_markets 
        GROUP BY category 
        ORDER BY num_unique_trades DESC
    """).fetchdf()
    print(market_dist)

    # 3. Join with Trades to see activity distribution
    print("\n--- Trade Activity by Category ---")
    query = f"""
        SELECT 
            m.category,
            COUNT(*) as trade_count,
            ROUND(SUM(CAST(t.maker_amount AS DECIMAL) + CAST(t.taker_amount AS DECIMAL)) / 2e6, 2) as estimated_vol_usd
        FROM (SELECT * FROM read_parquet('{poly_trades_glob}') LIMIT 100000) t
        JOIN categorized_markets m ON (
            LOWER(t.maker_asset_id) LIKE '%' || m.condition_id || '%' OR 
            LOWER(t.taker_asset_id) LIKE '%' || m.condition_id || '%'
        )
        GROUP BY m.category
        ORDER BY trade_count DESC
    """

    trade_dist = con.execute(query).fetchdf()
    print(trade_dist)

    # 4. Show a few examples of "Other/Misc" to help refine the CASE statement
    print("\n--- Sample of 'Other/Misc' Questions (to refine filters) ---")
    misc_samples = con.execute("""
        SELECT question 
        FROM categorized_markets 
        WHERE category = 'Other/Misc' 
        LIMIT 10
    """).fetchdf()
    print(misc_samples)

if __name__ == "__main__":
    explore_categories()
    con.close()