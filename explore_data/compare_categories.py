import pandas as pd
import pathlib

def compare_csv_categories():
    print("Loading datasets...")

    # Configuration
    SCRIPT_DIR = pathlib.Path(__file__).parent

    trader_stats_path = str(SCRIPT_DIR.parent / "samples" / "updated_trader_stats.csv").replace("\\", "/")
    clustered_stats_path = str(SCRIPT_DIR.parent / "clustering" / "clustered_traders.csv").replace("\\", "/")
    
    try:
        df_stats = pd.read_csv(trader_stats_path)
        df_clusters = pd.read_csv(clustered_stats_path)
    except FileNotFoundError as e:
        print(f"Error: Ensure both CSV files are in the current directory.")
        return

    # 1. Reconstruct the category from boolean columns in the cluster file
    # Find all columns that start with 'category_'
    cat_cols = [c for c in df_clusters.columns if c.startswith('category_')]
    
    def get_category(row):
        for col in cat_cols:
            if row[col] == True:
                # Returns the name without the 'category_' prefix
                return col.replace('category_', '')
        return 'Other/Misc'

    print("Collapsing boolean columns in clusters...")
    df_clusters['primary_category'] = df_clusters.apply(get_category, axis=1)

    # 2. Calculate counts for stats file
    stats_counts = df_stats['primary_category'].value_counts().reset_index()
    stats_counts.columns = ['category', 'in_updated_stats']

    # 3. Calculate counts for clusters file
    cluster_counts = df_clusters['primary_category'].value_counts().reset_index()
    cluster_counts.columns = ['category', 'in_clustered_traders']

    # 4. Merge side-by-side
    comparison = pd.merge(stats_counts, cluster_counts, on='category', how='outer').fillna(0)
    comparison['difference'] = comparison['in_updated_stats'] - comparison['in_clustered_traders']
    
    # Format numbers as integers
    cols_to_fix = ['in_updated_stats', 'in_clustered_traders', 'difference']
    comparison[cols_to_fix] = comparison[cols_to_fix].astype(int)

    print("\n--- Category Count Comparison ---")
    print(comparison.to_string(index=False))

    # 5. Summary Logic
    total_diff = comparison['difference'].abs().sum()
    if total_diff == 0:
        print("\n✅ Perfect Match: All categories align between files.")
    else:
        print(f"\n⚠️ Mismatch: {total_diff} total records differ.")
        print("Note: If 'Other/Misc' is higher in stats, some traders may have been filtered out.")

if __name__ == "__main__":
    compare_csv_categories()