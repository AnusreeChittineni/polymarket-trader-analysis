"""Thin CLI wrapper for the trader clustering pipeline."""

from __future__ import annotations

from src.analysis.polymarket.trader_clustering_pipeline import main


if __name__ == "__main__":
    raise SystemExit(main())
