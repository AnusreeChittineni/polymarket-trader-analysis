import duckdb
import pathlib
import pandas as pd

SCRIPT_DIR = pathlib.Path(__file__).parent
DATA_ROOT = SCRIPT_DIR.parent / "data"
OUTPUT_DIR = SCRIPT_DIR / "processed_data"
OUTPUT_DIR.mkdir(exist_ok=True)

# connect to DuckDB
con = duckdb.connect()

def create_trader_subdatasets():
    print("Starting Trader Feature Extraction...")

    # Define paths to parquet chunks
    poly_trades_glob = str(DATA_ROOT / "polymarket" / "trades" / "[!.]*.parquet").replace("\\", "/")
    poly_markets_path = DATA_ROOT / "polymarket" / "markets"

    """
    valid_files = [
        str(f).replace("\\", "/") # DuckDB prefers forward slashes even on Windows
        for f in poly_trades_path.glob("*.parquet") 
        if not f.name.startswith("._")
    ]
    """

    # extract PolyMarket trader features
    # treat both 'maker' and 'taker' as traders
    print(f"--- Processing PolyMarket Data Files ---")
    con.execute(f"""
        CREATE OR REPLACE VIEW raw_poly_trades AS 
        SELECT 
            maker as trader_id, 
            maker_amount as amount, 
            timestamp, 
            _contract 
        FROM read_parquet('{poly_trades_glob}')
        
        UNION ALL
        
        SELECT 
            taker as trader_id, 
            taker_amount as amount, 
            timestamp, 
            _contract 
        FROM read_parquet('{poly_trades_glob}')
    """)
    
    # aggregate features per trader
    # maker_amount/taker_amount in PolyMarket is in 6-decimal USDC
    poly_features = con.execute("""
        SELECT 
            trader_id,
            COUNT(*) as total_trades,
            SUM(amount) / 1e6 as total_volume_usd,
            AVG(amount) / 1e6 as avg_trade_size_usd,
            COUNT(DISTINCT _contract) as unique_markets_traded,
            MAX(timestamp) - MIN(timestamp) as trading_lifespan_seconds
        FROM raw_poly_trades
        WHERE trader_id IS NOT NULL
        GROUP BY trader_id
        HAVING total_trades > 1
    """).fetchdf()

    print(f"Success! Processed {len(poly_features)} traders.")

    # export for D3 and clustering
    # Parquet to preserve types, CSV for D3 testing
    poly_features.to_parquet(OUTPUT_DIR / "polymarket_trader_features.parquet")
    poly_features.head(5000).to_csv(OUTPUT_DIR / "d3_trader_sample.csv", index=False)
    
    print(f"Created {len(poly_features)} unique trader profiles.")
    print(f"Saved to: {OUTPUT_DIR}")

if __name__ == "__main__":
    create_trader_subdatasets()
    con.close()