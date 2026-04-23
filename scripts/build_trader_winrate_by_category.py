"""Build trader-level win rate by market category from the `samples/updated_samples.*` toy data.

Important note:
- The toy `samples/updated_samples.csv` does not include market resolutions/outcomes, so a *true*
  win rate cannot be computed from it alone.
- This script therefore computes a *proxy* win-rate per trader/category using a simple heuristic
  based on how close a trade's implied probability is to 50% (most informative) and trader activity.

Outputs:
- samples/trader_win_rate_by_category.csv
- samples/trader_win_rate_by_category.parquet

Columns:
- trader, category, win_rate_proxy, n_positions, avg_implied_price, avg_position_size,
    total_position_size, n_fills, total_trade_number
"""

from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd


def _implied_price_from_amounts(maker_amount: pd.Series, taker_amount: pd.Series) -> pd.Series:
    """Compute implied price (in %) from maker/taker amounts.

    Works vectorized over pandas Series.
    """

    denom = maker_amount.astype(float) + taker_amount.astype(float)
    with np.errstate(divide="ignore", invalid="ignore"):
        price = 100.0 * maker_amount.astype(float) / denom
    price = price.mask(denom <= 0)
    return price


def main() -> None:
    repo_root = Path(__file__).resolve().parents[1]
    samples_dir = repo_root / "samples"
    src_path = samples_dir / "updated_samples.csv"

    if not src_path.exists():
        raise FileNotFoundError(f"Missing input file: {src_path}")

    df = pd.read_csv(src_path)

    required = {
        "maker",
        "taker",
        "maker_amount",
        "taker_amount",
        "category",
    "transaction_hash",
    "log_index",
    }
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"Input missing required columns: {sorted(missing)}")

    # Build two-sided positions so each fill contributes a position to maker and taker.
    # A "fill" here is identified by (transaction_hash, log_index) in the toy dataset.
    maker_price = _implied_price_from_amounts(df["maker_amount"], df["taker_amount"])  # type: ignore[arg-type]
    taker_price = 100.0 - maker_price

    fill_id = df["transaction_hash"].astype(str) + ":" + df["log_index"].astype(str)

    maker_positions = pd.DataFrame(
        {
            "trader": df["maker"].astype(str),
            "category": df["category"].astype(str),
            "implied_price": maker_price,
            "position_size": df["maker_amount"].astype(float),
            "fill_id": fill_id,
        }
    )

    taker_positions = pd.DataFrame(
        {
            "trader": df["taker"].astype(str),
            "category": df["category"].astype(str),
            "implied_price": taker_price,
            "position_size": df["taker_amount"].astype(float),
            "fill_id": fill_id,
        }
    )

    pos = pd.concat([maker_positions, taker_positions], ignore_index=True)
    pos = pos.replace([np.inf, -np.inf], np.nan).dropna(
        subset=["trader", "category", "implied_price", "fill_id"]
    )

    # Proxy win rate heuristic:
    # - Score each position by closeness to 50%: score in [0,1], 1 is best.
    # - Then map to win-rate-ish range [0.25, 0.75] with small activity-based bump.
    closeness = 1.0 - (pos["implied_price"].sub(50.0).abs() / 50.0)
    closeness = closeness.clip(lower=0.0, upper=1.0)
    pos["score"] = closeness

    agg = (
        pos.groupby(["trader", "category"], as_index=False)
        .agg(
            n_positions=("score", "size"),
        n_fills=("fill_id", "nunique"),
            avg_score=("score", "mean"),
            avg_implied_price=("implied_price", "mean"),
            avg_position_size=("position_size", "mean"),
            total_position_size=("position_size", "sum"),
        )
    )

    # Clearer alias used by downstream consumers: trade count ≈ fill count in this dataset.
    agg["total_trade_number"] = agg["n_fills"]

    # Activity bump: up to +0.05 for very active trader/category combos.
    activity_bump = (np.log1p(agg["n_positions"]) / np.log(1 + 500)).clip(0, 1) * 0.05

    # Map score to proxy win rate.
    win_rate_proxy = 0.40 + agg["avg_score"] * 0.30 + activity_bump
    agg["win_rate_proxy"] = win_rate_proxy.clip(0.25, 0.75)

    # Round for nicer CSVs.
    agg["win_rate_proxy"] = agg["win_rate_proxy"].round(4)
    agg["avg_implied_price"] = agg["avg_implied_price"].round(2)
    agg["avg_position_size"] = agg["avg_position_size"].round(2)
    agg["total_position_size"] = agg["total_position_size"].round(2)

    out_csv = samples_dir / "trader_win_rate_by_category.csv"
    out_parquet = samples_dir / "trader_win_rate_by_category.parquet"

    agg.to_csv(out_csv, index=False)
    agg.to_parquet(out_parquet, index=False)

    print(f"Wrote {len(agg):,} rows -> {out_csv}")
    print(f"Wrote {len(agg):,} rows -> {out_parquet}")


if __name__ == "__main__":
    main()
