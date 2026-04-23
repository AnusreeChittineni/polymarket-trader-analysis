import pandas as pd

# Path to your updated CSV
CSV_PATH = "../samples/updated_samples.csv"

def analyze_distribution():
    try:
        # 1. Load the data
        print(f"Loading {CSV_PATH}...")
        df = pd.read_csv(CSV_PATH)
        
        if 'category' not in df.columns:
            print("Error: 'category' column not found. Run the append script first!")
            return

        # 2. Calculate Counts and Percentages
        counts = df['category'].value_counts()
        percentages = df['category'].value_counts(normalize=True) * 100
        
        # 3. Combine into a Summary Table
        summary = pd.DataFrame({
            'Count': counts,
            'Percentage': percentages
        })
        
        # 4. Display results
        print("\n" + "="*30)
        print(" CATEGORY DISTRIBUTION")
        print("="*30)
        
        # Formatting the percentage to 2 decimal places
        print(summary.to_string(formatters={'Percentage': '{:,.2f}%'.format}))
        
        print(f"\nTotal Categorized Trades: {len(df)}")
        print("="*30)

    except FileNotFoundError:
        print(f"Error: Could not find {CSV_PATH}. Check your file paths!")
    except Exception as e:
        print(f" An error occurred: {e}")

if __name__ == "__main__":
    analyze_distribution()