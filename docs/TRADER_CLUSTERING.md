# Trader Clustering Handoff

## Repo Fit

What already existed:
- `src/analysis/` contains Python analysis modules and the repo already writes analysis outputs into `output/`.
- `scripts/` is currently used for operational helpers, so it is a reasonable place for a direct clustering runner.
- There is no existing `requirements.txt`, `package.json`, frontend app, or D3 scaffold in this repo.
- The current repo is Python-first, with dependencies managed in [pyproject.toml](/C:/Users/jayja/OneDrive/Desktop/college%20folder/Spring%2026/DATA%20VIZ/cse-6242-prediction-market-analysis/pyproject.toml).

What was added:
- [src/analysis/polymarket/trader_clustering_pipeline.py](/C:/Users/jayja/OneDrive/Desktop/college%20folder/Spring%2026/DATA%20VIZ/cse-6242-prediction-market-analysis/src/analysis/polymarket/trader_clustering_pipeline.py): reusable schema validation, preprocessing, KMeans clustering, evaluation, summaries, and visualization metadata.
- [scripts/run_trader_clustering.py](/C:/Users/jayja/OneDrive/Desktop/college%20folder/Spring%2026/DATA%20VIZ/cse-6242-prediction-market-analysis/scripts/run_trader_clustering.py): direct CLI entry point.
- [tests/test_trader_clustering_pipeline.py](/C:/Users/jayja/OneDrive/Desktop/college%20folder/Spring%2026/DATA%20VIZ/cse-6242-prediction-market-analysis/tests/test_trader_clustering_pipeline.py): targeted coverage for schema handling and output creation.

Why this fit is clean:
- The clustering logic is Polymarket-specific analysis work, so it lives beside other Polymarket analysis modules instead of creating a separate framework.
- The runner is a thin script because this workflow needs a concrete input path and multiple output artifacts, which does not match the existing `Analysis.save()` shape cleanly.
- Outputs default to `output/trader_clustering/`, matching the repo's current analysis-output convention.

## Expected Input Schema

The pipeline expects one row per trader. It resolves canonical fields through aliases so teammate-produced column names do not have to match exactly.

Required canonical fields:
- `trader_id`
- `win_rate`
- `avg_trade_size`
- `total_trade_volume`
- `total_trade_number`
- `trades_per_day`
- `net_gains_loss`
- `avg_odds`

Optional metadata fields:
- `most_common_market`
- `category`

Current alias mapping:
- `trader_id`: `trader_id`, `trader`, `user`, `address`, `wallet`, `maker`
- `win_rate`: `win_rate`, `win_pct`, `pct_wins`
- `avg_trade_size`: `avg_trade_size`, `average_trade_size`, `mean_trade_size`
- `total_trade_volume`: `total_trade_volume`, `trade_volume`, `volume_usd`, `total_volume`
- `total_trade_number`: `total_trade_number`, `trade_count`, `num_trades`, `total_trades`
- `trades_per_day`: `trades_per_day`, `avg_trades_per_day`, `trade_frequency`
- `net_gains_loss`: `net_gains_loss`, `net_gain_loss`, `net_pnl`, `pnl`, `net_profit`
- `avg_odds`: `avg_odds`, `average_odds`, `mean_odds`
- `most_common_market`: `most_common_market`, `preferred_market`, `top_market`
- `category`: `category`, `most_common_category`, `preferred_category`, `top_category`

If any required clustering fields are missing after alias resolution, the pipeline raises a helpful schema error listing:
- the missing canonical fields
- accepted aliases for each missing field
- the actual columns found in the input file

## Preprocessing Defaults

The preprocessing layer is explicit and recorded in `clustering_metadata.json`.

Defaults:
- Numeric features are median-imputed.
- Categorical metadata fields are filled with `"Unknown"`.
- `avg_trade_size`, `total_trade_volume`, `total_trade_number`, and `trades_per_day` use `log1p` before scaling.
- `net_gains_loss` uses a signed `log1p` transform so negative PnL values remain usable.
- `StandardScaler` is applied to the clustering matrix before KMeans.

The final visualization-ready table keeps:
- original input columns
- canonical plotting columns
- transformed clustering columns such as `log_total_trade_volume`
- the final `cluster` label

## Outputs

Default output directory: `output/trader_clustering/`

Files:
- `clustered_traders.csv`
- `clustered_traders.parquet`
- `cluster_summary.csv`
- `cluster_eval_metrics.csv`
- `cluster_eval_plot.png`
- `visualization_config.json`
- `clustering_metadata.json`

The visualization team should primarily consume `clustered_traders.csv` plus `visualization_config.json`.

## Local Usage

Install project dependencies:

```bash
uv sync
```

Run on the final teammate-produced trader table:

```bash
uv run python scripts/run_trader_clustering.py --input path/to/trader_features.csv --k 4
```

Run the full workflow without a final input file yet:

```bash
uv run python scripts/run_trader_clustering.py
```

When no input is provided, the runner generates a mock trader feature table at `output/trader_clustering/mock_trader_features.csv` and runs the full clustering pipeline on that file.

Useful CLI switches:
- `--features win_rate avg_trade_size total_trade_volume total_trade_number trades_per_day net_gains_loss avg_odds`
- `--metadata-fields most_common_market category`
- `--eval-min-k 2 --eval-max-k 8`
- `--output-format csv`
