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
            condition_id,
            market_maker_address,
            CASE 
                WHEN question ILIKE '%crypto%' OR question ILIKE '%bitcoin%' THEN 'Crypto'
                WHEN question ILIKE '%election%' OR question ILIKE '%trump%' OR question ILIKE '%biden%' THEN 'Politics'
                WHEN question ILIKE '%nfl%' OR question ILIKE '%nba%' THEN 'Sports'
                WHEN question ILIKE '%fed%' OR question ILIKE '%rate%' THEN 'Economics'
                WHEN question ILIKE '%Kardashian%' OR question ILIKE '%Kanye%' THEN 'Pop Culture'
                ELSE 'Other/Misc'
            END AS category
        FROM read_parquet('{POLY_MARKETS_GLOB}')
        WHERE clob_token_ids IS NOT NULL AND clob_token_ids != '[]'
    """)

    query = con.execute(f"""
        SELECT 
            t.transaction_hash,
            t.maker,
            t.taker_asset_id,
            m.question,
            m.market_maker_address,
            m.clob_token_ids
        FROM read_parquet('{SAMPLES_PATH}') t
        JOIN read_parquet('{POLY_MARKETS_GLOB}') m
            ON (
                -- Check if the asset ID exists within the market's token list string
                m.clob_token_ids LIKE '%' || t.taker_asset_id || '%'
                OR 
                m.clob_token_ids LIKE '%' || t.maker_asset_id || '%'
            )
        LIMIT 10
    """).df()

    print("Sample joined data:")
    print(query.head())

    print("Step 2: Joining categories to samples...")
    # We join on market_maker_address (common identifier in PolyMarket data)
    final_query = f"""
        SELECT 
            t.*, 
            m.category,
            m.question
        FROM read_parquet('{SAMPLES_PATH}') t
        INNER JOIN markets_expanded m 
            ON CAST(t.taker_asset_id AS VARCHAR) = CAST(m.token_id AS VARCHAR)
    """
    
    # Execute and fetch to a dataframe
    df_final = con.execute(final_query).df()

    print("Step 4: Creating updated_trader_stats.csv...")
    
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
    trader_stats_path = str(SCRIPT_DIR.parent / "samples" / "trader_stats.csv").replace("\\", "/")
    
    con.execute(f"""
        CREATE OR REPLACE TABLE updated_stats AS
        SELECT 
            m.primary_category,
            s.*
        FROM read_csv_auto('{trader_stats_path}') s
        LEFT JOIN trader_category_mapping m ON s.trader = m.trader
    """)

    # 3. Export the final result
    UPDATED_STATS_OUTPUT = str(SCRIPT_DIR.parent / "samples" / "updated_trader_stats.csv").replace("\\", "/")
    con.execute(f"COPY updated_stats TO '{UPDATED_STATS_OUTPUT}' (HEADER, DELIMITER ',')")

    print(f"Successfully created: {UPDATED_STATS_OUTPUT}")

    print("Step 3: Exporting files...")
    # Export to Parquet
    df_final.to_parquet(OUTPUT_PARQUET, index=False)
    
    # Export to CSV
    df_final.to_csv(OUTPUT_CSV, index=False)

    print(f"Done! Created {OUTPUT_PARQUET} and {OUTPUT_CSV}")

if __name__ == "__main__":
    run_transformation()