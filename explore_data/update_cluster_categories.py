import duckdb
import pandas as pd
import pathlib
import os

# Configuration
SCRIPT_DIR = pathlib.Path(__file__).parent

trader_stats_path = str(SCRIPT_DIR.parent / "samples" / "updated_trader_stats.csv").replace("\\", "/")
clustered_stats_path = str(SCRIPT_DIR.parent / "clustering" / "clustered_traders.csv").replace("\\", "/")

con = duckdb.connect(database=':memory:')
con.execute("INSTALL json; LOAD json;")

# 1. Load both the recently updated stats and the existing clustered data
con.execute(f"CREATE OR REPLACE TABLE updated_stats AS SELECT * FROM read_csv_auto('{trader_stats_path}')")
con.execute(f"CREATE OR REPLACE TABLE clustered_data AS SELECT * FROM read_csv_auto('{clustered_stats_path}')")

# 2. Update the boolean columns based on the primary_category
# We select the updated metrics and category from updated_stats and 
# keep the cluster/projection columns from the original clustered_data
con.execute("""
    CREATE OR REPLACE TABLE updated_clusters AS
    SELECT 
        s.*,  -- All updated metrics and the primary_category
        
        -- One-hot encode the booleans based on the new primary_category
        (s.primary_category = 'Crypto') AS "category_Crypto",
        (s.primary_category = 'Economics') AS "category_Economics",
        (s.primary_category = 'Other/Misc') AS "category_Other/Misc",
        (s.primary_category = 'Politics') AS "category_Politics",
        (s.primary_category = 'Pop Culture') AS "category_Pop Culture",
        (s.primary_category = 'Sports') AS "category_Sports",
        
        -- Pull the machine learning outputs from the existing clustered table
        c.kmeans_cluster,
        c.hdbscan_cluster,
        c.tsne_1,
        c.tsne_2
    FROM updated_stats s
    LEFT JOIN clustered_data c ON LOWER(s.trader) = LOWER(c.trader)
""")

# 3. Export to the final CSV
con.execute(f"COPY updated_clusters TO '{SCRIPT_DIR.parent / 'samples' / 'clustered_traders.csv'}' (HEADER, DELIMITER ',')")

print("Successfully updated boolean categories and preserved cluster assignments.")