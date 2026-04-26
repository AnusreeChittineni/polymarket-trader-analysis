import duckdb
import pandas as pd
import pathlib
import os

# Configuration
SCRIPT_DIR = pathlib.Path(__file__).parent
DATA_ROOT = SCRIPT_DIR.parent / "data"
SAMPLES_PATH =  str(SCRIPT_DIR.parent / "samples" / "trades_sampled.parquet").replace("\\", "/")
POLY_MARKETS_GLOB = str(DATA_ROOT / "polymarket" / "markets" / "[!.]*.parquet").replace("\\", "/")
OUTPUT_CSV = str(SCRIPT_DIR.parent / "samples" / "updated_samples.csv").replace("\\", "/")
OUTPUT_PARQUET = str(SCRIPT_DIR.parent / "samples" / "updated_samples.parquet").replace("\\", "/")

def run_transformation():
    # Initialize DuckDB connection
    con = duckdb.connect(database=':memory:')

    con.execute("INSTALL json; LOAD json;")
    
    print("Step 1: Creating categorized view of markets...")
    # Your logic integrated directly
    con.execute(f"""
        CREATE OR REPLACE VIEW markets_expanded AS 
        SELECT 
            unnest(from_json(CAST(clob_token_ids AS JSON), '["VARCHAR"]')) AS token_id,
            question,
            market_maker_address,
            CASE 
                WHEN question ILIKE '%crypto%' OR question ILIKE '%bitcoin%' OR question ILIKE '%eth%' OR question ILIKE '%solana%' THEN 'Crypto'
                WHEN question ILIKE '%US%' OR question ILIKE '%election%' OR question ILIKE '%trump%' OR question ILIKE '%poll%' OR question ILIKE '%biden%' OR question ILIKE '%harris%' OR question ILIKE '%netanyahu%' THEN 'Politics'
                WHEN question ILIKE '%arsenal%' OR question ILIKE '%goals%' OR question ILIKE '%nhl%' OR question ILIKE '%nfl%' OR question ILIKE '%nba%' OR question ILIKE '%mlb%' OR question ILIKE '%championship%' OR question ILIKE '%win%' OR question ILIKE '%close%' OR question ILIKE '%points%' OR question ILIKE '%lose%' THEN 'Sports'
                WHEN question ILIKE '%fed%' OR question ILIKE '%rate%' OR question ILIKE '%inflation%' OR question ILIKE '%recession%' or question ILIKE '%$%' THEN 'Economics'
                WHEN question ILIKE '%oscars%' OR question ILIKE '%grammys%' OR question ILIKE '%movie%' or question ILIKE '%album%' or question ILIKE '%music%' or question ILIKE '%netflix%' or question ILIKE '%divorce%' or question ILIKE '%Zendaya%'  or question ILIKE '%Coachella%' or question ILIKE '%sstreams%' or question ILIKE '%Kardashian%' THEN 'Pop Culture'
                ELSE 'Other/Misc'
            END AS category
        FROM read_parquet('{POLY_MARKETS_GLOB}')
    """)

    trader_stats_path = str(SCRIPT_DIR.parent / "create_subset" / "trader_stats.csv").replace("\\", "/")

    con.execute(f"CREATE OR REPLACE TABLE existing_stats AS SELECT * FROM read_csv_auto('{trader_stats_path}')")

    cols = con.execute("PRAGMA table_info('existing_stats')").df()['name'].tolist()
    has_primary_cat = 'primary_category' in cols

    # 3. Setup the column names for the query
    history_col = "s.primary_category" if has_primary_cat else "NULL"
    exclude_clause = "EXCLUDE (primary_category)" if has_primary_cat else ""

    final_query = f"""
        WITH joined_data AS (
            SELECT 
                t.*,
                -- Check taker_asset_id first, then maker_asset_id
                COALESCE(m_taker.category, m_maker.category) AS cat_by_token,
                {history_col} AS cat_by_history
            FROM read_parquet('{SAMPLES_PATH}') t
            -- Join on Taker Asset
            LEFT JOIN markets_expanded m_taker 
                ON LOWER(CAST(t.taker_asset_id AS VARCHAR)) = LOWER(CAST(m_taker.token_id AS VARCHAR))
            -- Join on Maker Asset (Backup)
            LEFT JOIN markets_expanded m_maker 
                ON LOWER(CAST(t.maker_asset_id AS VARCHAR)) = LOWER(CAST(m_maker.token_id AS VARCHAR))
            -- Join on Trader History
            LEFT JOIN existing_stats s 
                ON LOWER(CAST(t.maker AS VARCHAR)) = LOWER(CAST(s.trader AS VARCHAR))
        )
        SELECT 
            *,
            -- PRIORITY: 
            -- 1. Category found via Asset ID match
            -- 2. Category found via historical Trader ID match
            -- 3. Default to Other/Misc
            COALESCE(cat_by_token, cat_by_history, 'Other/Misc') AS category
        FROM joined_data
    """

    print("Step 2: Creating updated_trader_stats.csv...")

    print(con.execute(f"SELECT * FROM read_parquet('{SAMPLES_PATH}') LIMIT 0").df().columns)
    
    # 1. First, identify the "Primary Category" for each trader based on trade frequency
    con.execute(f"""
        CREATE OR REPLACE TABLE trader_category_mapping AS
        WITH counts AS (
            SELECT 
                maker AS trader,
                category,
                COUNT(*) as cat_count
            FROM ({final_query})
            GROUP BY 1, 2
        )
        SELECT trader, category AS primary_category
        FROM (
            SELECT *, ROW_NUMBER() OVER(PARTITION BY trader ORDER BY cat_count DESC) as r
            FROM counts
        ) WHERE r = 1
    """)

    # 2. Join this category onto your existing trader_stats.csv
    # We use read_csv_auto to pull in the existing stats you already have
    
    exclude_clause = "EXCLUDE (primary_category)" if has_primary_cat else ""

    con.execute(f"""
        CREATE OR REPLACE TABLE final_stats AS
        SELECT 
            -- This line ensures if the mapping found nothing, it becomes 'Other/Misc'
            COALESCE(m.primary_category, 'Other/Misc') AS primary_category,
            s.* {exclude_clause} 
        FROM existing_stats s
        LEFT JOIN trader_category_mapping m ON s.trader = m.trader
    """)

    # 3. Export the final result
    UPDATED_STATS_OUTPUT = str(SCRIPT_DIR.parent / "samples" / "updated_trader_stats.csv").replace("\\", "/")
    con.execute(f"COPY final_stats TO '{UPDATED_STATS_OUTPUT}' (HEADER, DELIMITER ',')")

    print(f"Successfully created: {UPDATED_STATS_OUTPUT}")

if __name__ == "__main__":
    run_transformation()