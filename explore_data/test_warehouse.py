import duckdb
import pathlib

# Locate database file
BASE_DIR = pathlib.Path(__file__).resolve().parents[1]
DB_PATH = BASE_DIR / "polymarket.duckdb"

print(f"Connecting to: {DB_PATH}")

con = duckdb.connect(str(DB_PATH))

print("\n--- Checking if polymarket_trades exists ---")

result = con.execute("""
SELECT table_name, table_type
FROM information_schema.tables
WHERE table_name = 'polymarket_trades';
""").fetchall()

if not result:
    print("polymarket_trades does NOT exist.")
else:
    name, obj_type = result[0]
    print(f"Found {name} of type: {obj_type}")

print("\n--- Checking view definition (if applicable) ---")

view_def = con.execute("""
SELECT view_definition
FROM information_schema.views
WHERE table_name = 'polymarket_trades';
""").fetchall()

if view_def:
    print("View definition found.")
    print("\nPreview:")
    print(view_def[0][0][:500], "...\n")
else:
    print("Not a view (or view definition not found).")

print("\n--- Row count test ---")

try:
    count = con.execute(
        "SELECT COUNT(*) FROM polymarket_trades;"
    ).fetchone()[0]
    print(f"Query works. Row count: {count:,}")
except Exception as e:
    print("Failed to query polymarket_trades:")
    print(e)

con.close()
print("\nDone.")