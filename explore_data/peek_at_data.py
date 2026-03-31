import duckdb
import pathlib
import pandas as pd

SCRIPT_DIR = pathlib.Path(__file__).parent
DATA_ROOT = SCRIPT_DIR.parent / "data"
OUTPUT_FILE = "parquet_peeks.txt"

# Connect to temporary in-memory DB (no persistence)
con = duckdb.connect()

# Show all columns
pd.set_option('display.max_columns', None)

# Show all rows (or a high number)
pd.set_option('display.max_rows', 100)

# Prevent column content truncation
pd.set_option('display.max_colwidth', None)

def inspect_parquet_folder(folder: pathlib.Path, max_files=3, file_handle=None):
    separator = "="*80
    print(f"\n{separator}", file=file_handle)
    print(f"Inspecting folder: {folder}", file=file_handle)
    print(f"{separator}", file=file_handle)

    parquet_files = [f for f in folder.glob("*.parquet") if not f.name.startswith("._")]

    if not parquet_files:
        print("No parquet files found.")
        return

    for file in parquet_files[:max_files]:
        print(f"\n File: {file.name}", file=file_handle)

        # print schema
        schema = con.execute(f"DESCRIBE SELECT * FROM read_parquet('{file}')").fetchdf()
        print("\nSchema:", file=file_handle)
        print(schema.to_string(index=False), file=file_handle)

        # print row count
        row_count = con.execute(f"SELECT COUNT(*) FROM read_parquet('{file}')").fetchone()[0]
        print(f"\nTotal rows: {row_count}", file=file_handle)

        # first 5 rows
        preview = con.execute(f"SELECT * FROM read_parquet('{file}') LIMIT 5").fetchdf()
        print("\nFirst 5 rows:", file=file_handle)
        print(preview.to_string(index=False), file=file_handle)


# loop through subfolders 
with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
    for subfolder in DATA_ROOT.rglob("*"):
        if any(x in subfolder.parts for x in ("blocks", "legacy_trades")):
            continue
        if subfolder.is_dir():
            inspect_parquet_folder(subfolder, max_files=3, file_handle=f)


con.close()