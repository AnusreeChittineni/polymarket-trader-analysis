"""Tests for the trader clustering handoff pipeline."""

from __future__ import annotations

import importlib.util
import json
from pathlib import Path

import pandas as pd
import pytest

from src.analysis.polymarket.trader_clustering_pipeline import (
    DEFAULT_NUMERIC_FEATURES,
    DEFAULT_SIGNED_LOG_FEATURES,
    SchemaValidationError,
    generate_mock_trader_features,
    preprocess_for_clustering,
    resolve_schema,
    run_trader_clustering_pipeline,
)


def test_resolve_schema_accepts_aliases():
    df = pd.DataFrame(
        {
            "trader": ["a", "b"],
            "win_pct": [0.5, 0.6],
            "average_trade_size": [10, 20],
            "volume_usd": [100, 200],
            "trade_count": [4, 6],
            "avg_trades_per_day": [0.2, 0.4],
            "net_pnl": [5, 10],
            "average_odds": [0.4, 0.6],
            "preferred_category": ["Politics", "Sports"],
        }
    )

    resolved = resolve_schema(df)

    assert resolved.canonical_to_source["trader_id"] == "trader"
    assert resolved.canonical_to_source["avg_odds"] == "average_odds"
    assert resolved.metadata_fields == ["category"]


def test_resolve_schema_raises_helpful_error_for_missing_required_columns():
    df = pd.DataFrame({"trader": ["a"], "win_rate": [0.5]})

    with pytest.raises(SchemaValidationError) as exc_info:
        resolve_schema(df)

    assert "Missing required clustering columns" in str(exc_info.value)
    assert "total_trade_volume" in str(exc_info.value)


def test_preprocess_builds_clean_matrix():
    pytest.importorskip("sklearn")

    df = generate_mock_trader_features(n_traders=24)
    df.loc[df.index[0], "net_gains_loss"] = None
    df.loc[df.index[1], "category"] = None

    preprocessed = preprocess_for_clustering(
        df,
        numeric_features=list(DEFAULT_NUMERIC_FEATURES),
        metadata_fields=["category", "most_common_market"],
        signed_log_features=list(DEFAULT_SIGNED_LOG_FEATURES),
    )

    assert preprocessed.scaled_matrix.shape[0] == len(df)
    assert not preprocessed.clustering_frame.isna().any().any()
    assert "signed_log_net_gains_loss" in preprocessed.cleaned_frame.columns
    assert set(preprocessed.categorical_fill_fields) == {"category", "most_common_market"}


@pytest.mark.skipif(importlib.util.find_spec("sklearn") is None, reason="sklearn not installed")
def test_pipeline_writes_expected_outputs(tmp_path: Path):
    input_path = tmp_path / "mock_traders.csv"
    generate_mock_trader_features(n_traders=40).to_csv(input_path, index=False)

    output_paths = run_trader_clustering_pipeline(
        input_path=input_path,
        output_dir=tmp_path / "outputs",
        n_clusters=4,
    )

    assert output_paths.clustered_traders_csv.exists()
    assert output_paths.cluster_summary_csv.exists()
    assert output_paths.cluster_eval_metrics_csv.exists()
    assert output_paths.visualization_config_json.exists()
    assert output_paths.clustering_metadata_json.exists()

    clustered_df = pd.read_csv(output_paths.clustered_traders_csv)
    assert "cluster" in clustered_df.columns
    assert clustered_df["cluster"].nunique() == 4

    summary_df = pd.read_csv(output_paths.cluster_summary_csv)
    assert {"cluster", "num_traders", "mean_win_rate", "median_win_rate"}.issubset(summary_df.columns)

    viz_config = json.loads(output_paths.visualization_config_json.read_text())
    assert viz_config["id_field"] == "trader_id"
    assert "win_rate" in viz_config["numeric_plot_fields"]
