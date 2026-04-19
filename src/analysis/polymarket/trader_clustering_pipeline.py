"""Trader clustering pipeline and visualization handoff outputs for Polymarket."""

from __future__ import annotations

import argparse
import json
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

DEFAULT_ALIAS_MAP: dict[str, list[str]] = {
    "trader_id": ["trader_id", "trader", "user", "address", "wallet", "maker"],
    "win_rate": ["win_rate", "win_pct", "pct_wins"],
    "avg_trade_size": ["avg_trade_size", "average_trade_size", "mean_trade_size"],
    "total_trade_volume": ["total_trade_volume", "trade_volume", "volume_usd", "total_volume"],
    "total_trade_number": ["total_trade_number", "trade_count", "num_trades", "total_trades"],
    "trades_per_day": ["trades_per_day", "avg_trades_per_day", "trade_frequency"],
    "net_gains_loss": ["net_gains_loss", "net_gain_loss", "net_pnl", "pnl", "net_profit"],
    "avg_odds": ["avg_odds", "average_odds", "mean_odds"],
    "most_common_market": ["most_common_market", "preferred_market", "top_market"],
    "category": ["category", "most_common_category", "preferred_category", "top_category"],
}

DEFAULT_NUMERIC_FEATURES = [
    "win_rate",
    "avg_trade_size",
    "total_trade_volume",
    "total_trade_number",
    "trades_per_day",
    "net_gains_loss",
    "avg_odds",
]
DEFAULT_METADATA_FIELDS = ["most_common_market", "category"]
DEFAULT_LOG_FEATURES = [
    "avg_trade_size",
    "total_trade_volume",
    "total_trade_number",
    "trades_per_day",
]
DEFAULT_SIGNED_LOG_FEATURES = ["net_gains_loss"]


class SchemaValidationError(ValueError):
    """Raised when the incoming trader feature table cannot be mapped cleanly."""


@dataclass
class ResolvedSchema:
    """Canonical-to-source mapping after schema validation."""

    canonical_to_source: dict[str, str]
    id_field: str
    numeric_features: list[str]
    metadata_fields: list[str]
    available_columns: list[str]


@dataclass
class PreprocessingArtifacts:
    """Intermediate outputs from preprocessing."""

    cleaned_frame: pd.DataFrame
    clustering_frame: pd.DataFrame
    scaled_matrix: np.ndarray
    median_imputations: dict[str, float]
    categorical_fill_fields: list[str]
    transformed_columns: dict[str, str]
    standardized_columns: list[str]
    imputed_counts: dict[str, int]


@dataclass
class PipelineOutputs:
    """Saved output paths from a clustering run."""

    clustered_traders_csv: Path | None
    clustered_traders_parquet: Path | None
    cluster_summary_csv: Path
    cluster_eval_metrics_csv: Path
    cluster_eval_plot_png: Path | None
    clustering_metadata_json: Path
    visualization_config_json: Path
    mock_input_csv: Path | None = None


def _load_sklearn() -> tuple[Any, Any, Any]:
    """Import sklearn lazily so the module still imports before dependencies are installed."""

    try:
        from sklearn.cluster import KMeans
        from sklearn.metrics import silhouette_score
        from sklearn.preprocessing import StandardScaler
    except ImportError as exc:
        raise ImportError(
            "scikit-learn is required for trader clustering. Run `uv sync` before executing this pipeline."
        ) from exc

    return KMeans, StandardScaler, silhouette_score


def humanize_label(field_name: str) -> str:
    """Convert a snake_case field into a plot-friendly label."""

    label = field_name.replace("_", " ").strip()
    if label.lower() == "avg odds":
        return "Average Odds"
    if label.lower() == "avg trade size":
        return "Average Trade Size"
    return label.title()


def load_feature_table(path: Path | str) -> pd.DataFrame:
    """Load a trader-level feature table from CSV or Parquet."""

    path = Path(path)
    if not path.exists():
        raise FileNotFoundError(f"Input feature table not found: {path}")

    suffix = path.suffix.lower()
    if suffix == ".csv":
        return pd.read_csv(path)
    if suffix in {".parquet", ".pq"}:
        return pd.read_parquet(path)

    raise ValueError(
        f"Unsupported input file format for {path.name}. Use CSV or Parquet."
    )


def _candidate_names(canonical_name: str, alias_map: dict[str, list[str]]) -> list[str]:
    aliases = alias_map.get(canonical_name, [])
    return list(dict.fromkeys([canonical_name, *aliases]))


def resolve_schema(
    df: pd.DataFrame,
    *,
    alias_map: dict[str, list[str]] | None = None,
    numeric_features: list[str] | None = None,
    metadata_fields: list[str] | None = None,
) -> ResolvedSchema:
    """Resolve canonical column names against an incoming feature table."""

    alias_map = alias_map or DEFAULT_ALIAS_MAP
    numeric_features = numeric_features or list(DEFAULT_NUMERIC_FEATURES)
    metadata_fields = metadata_fields or list(DEFAULT_METADATA_FIELDS)
    available_columns = list(df.columns)

    canonical_to_source: dict[str, str] = {}
    missing_required: list[str] = []

    for canonical_name in ["trader_id", *numeric_features]:
        source_name = next(
            (candidate for candidate in _candidate_names(canonical_name, alias_map) if candidate in df.columns),
            None,
        )
        if source_name is None:
            missing_required.append(canonical_name)
        else:
            canonical_to_source[canonical_name] = source_name

    if missing_required:
        expected = {
            name: _candidate_names(name, alias_map)
            for name in missing_required
        }
        raise SchemaValidationError(
            "Missing required clustering columns after alias resolution. "
            f"Missing: {missing_required}. Expected aliases: {expected}. "
            f"Available columns: {available_columns}"
        )

    resolved_metadata: list[str] = []
    for canonical_name in metadata_fields:
        source_name = next(
            (candidate for candidate in _candidate_names(canonical_name, alias_map) if candidate in df.columns),
            None,
        )
        if source_name is not None:
            canonical_to_source[canonical_name] = source_name
            resolved_metadata.append(canonical_name)

    if len(numeric_features) < 2:
        raise SchemaValidationError("At least two numeric features are recommended for trader clustering.")

    return ResolvedSchema(
        canonical_to_source=canonical_to_source,
        id_field="trader_id",
        numeric_features=numeric_features,
        metadata_fields=resolved_metadata,
        available_columns=available_columns,
    )


def add_canonical_columns(df: pd.DataFrame, resolved_schema: ResolvedSchema) -> pd.DataFrame:
    """Preserve original columns while adding canonical columns for downstream stability."""

    normalized = df.copy()
    for canonical_name, source_name in resolved_schema.canonical_to_source.items():
        normalized[canonical_name] = normalized[source_name]
    return normalized


def preprocess_for_clustering(
    df: pd.DataFrame,
    *,
    numeric_features: list[str],
    metadata_fields: list[str],
    log_features: list[str] | None = None,
    signed_log_features: list[str] | None = None,
) -> PreprocessingArtifacts:
    """Clean, transform, and standardize the trader feature matrix."""

    _, StandardScaler, _ = _load_sklearn()

    cleaned = df.copy()
    log_features = log_features or list(DEFAULT_LOG_FEATURES)
    signed_log_features = signed_log_features or list(DEFAULT_SIGNED_LOG_FEATURES)

    overlap = set(log_features) & set(signed_log_features)
    if overlap:
        raise ValueError(
            "The same feature cannot use both log and signed-log transforms. "
            f"Overlapping features: {sorted(overlap)}"
        )

    median_imputations: dict[str, float] = {}
    imputed_counts: dict[str, int] = {}
    for feature in numeric_features:
        numeric_series = pd.to_numeric(cleaned[feature], errors="coerce")
        missing_count = int(numeric_series.isna().sum())
        median_value = float(numeric_series.median()) if not numeric_series.dropna().empty else 0.0
        cleaned[feature] = numeric_series.fillna(median_value)
        median_imputations[feature] = median_value
        imputed_counts[feature] = missing_count

    categorical_fill_fields: list[str] = []
    for field in metadata_fields:
        cleaned[field] = (
            cleaned[field]
            .astype("string")
            .fillna("Unknown")
            .str.strip()
            .replace("", "Unknown")
        )
        categorical_fill_fields.append(field)

    clustering_frame = pd.DataFrame(index=cleaned.index)
    transformed_columns: dict[str, str] = {}

    for feature in numeric_features:
        series = cleaned[feature].astype(float)
        if feature in log_features:
            if (series < 0).any():
                raise ValueError(
                    f"Feature `{feature}` contains negative values and cannot use log1p. "
                    "Remove it from `log_features` or clean the input data."
                )
            transformed_name = f"log_{feature}"
            clustering_frame[transformed_name] = np.log1p(series)
            cleaned[transformed_name] = clustering_frame[transformed_name]
            transformed_columns[feature] = transformed_name
        elif feature in signed_log_features:
            transformed_name = f"signed_log_{feature}"
            clustering_frame[transformed_name] = np.sign(series) * np.log1p(np.abs(series))
            cleaned[transformed_name] = clustering_frame[transformed_name]
            transformed_columns[feature] = transformed_name
        else:
            clustering_frame[feature] = series

    scaler = StandardScaler()
    scaled_matrix = scaler.fit_transform(clustering_frame)

    return PreprocessingArtifacts(
        cleaned_frame=cleaned,
        clustering_frame=clustering_frame,
        scaled_matrix=scaled_matrix,
        median_imputations=median_imputations,
        categorical_fill_fields=categorical_fill_fields,
        transformed_columns=transformed_columns,
        standardized_columns=list(clustering_frame.columns),
        imputed_counts=imputed_counts,
    )


def evaluate_cluster_range(
    scaled_matrix: np.ndarray,
    *,
    cluster_values: list[int],
    random_state: int,
    n_init: int,
) -> pd.DataFrame:
    """Compute inertia and silhouette across a range of K values."""

    KMeans, _, silhouette_score = _load_sklearn()

    sample_count = scaled_matrix.shape[0]
    valid_cluster_values = [k for k in cluster_values if 1 < k < sample_count]
    if not valid_cluster_values:
        raise ValueError(
            f"No valid cluster counts found for {sample_count} rows. "
            "Use a smaller K range or a larger input table."
        )

    metrics: list[dict[str, float | int | None]] = []
    for k in valid_cluster_values:
        model = KMeans(n_clusters=k, random_state=random_state, n_init=n_init)
        labels = model.fit_predict(scaled_matrix)
        silhouette = float(silhouette_score(scaled_matrix, labels)) if k < sample_count else np.nan
        metrics.append(
            {
                "k": k,
                "inertia": float(model.inertia_),
                "silhouette_score": silhouette,
            }
        )

    return pd.DataFrame(metrics)


def fit_kmeans(
    scaled_matrix: np.ndarray,
    *,
    n_clusters: int,
    random_state: int,
    n_init: int,
) -> np.ndarray:
    """Fit the baseline KMeans model and return cluster labels."""

    KMeans, _, _ = _load_sklearn()

    sample_count = scaled_matrix.shape[0]
    if n_clusters < 2:
        raise ValueError("KMeans clustering requires k >= 2 for this workflow.")
    if n_clusters >= sample_count:
        raise ValueError(
            f"Requested k={n_clusters}, but only {sample_count} traders are available. "
            "Choose a smaller K."
        )

    model = KMeans(n_clusters=n_clusters, random_state=random_state, n_init=n_init)
    return model.fit_predict(scaled_matrix)


def summarize_clusters(
    clustered_df: pd.DataFrame,
    *,
    numeric_features: list[str],
    metadata_fields: list[str],
) -> pd.DataFrame:
    """Build a one-row-per-cluster summary table for interpretation."""

    summary_rows: list[dict[str, Any]] = []
    for cluster_id, group in clustered_df.groupby("cluster", sort=True):
        row: dict[str, Any] = {
            "cluster": int(cluster_id),
            "num_traders": int(len(group)),
        }
        for feature in numeric_features:
            row[f"mean_{feature}"] = float(group[feature].mean())
            row[f"median_{feature}"] = float(group[feature].median())

        for field in metadata_fields:
            if field in group.columns:
                mode = group[field].mode(dropna=True)
                row[f"top_{field}"] = mode.iloc[0] if not mode.empty else "Unknown"

        summary_rows.append(row)

    return pd.DataFrame(summary_rows).sort_values("cluster").reset_index(drop=True)


def build_visualization_config(
    *,
    numeric_features: list[str],
    metadata_fields: list[str],
    transformed_columns: dict[str, str],
) -> dict[str, Any]:
    """Return lightweight metadata for the visualization team."""

    clustering_input_fields = [transformed_columns.get(feature, feature) for feature in numeric_features]
    tooltip_fields = ["trader_id", "cluster", *metadata_fields, *numeric_features]
    for transformed_name in transformed_columns.values():
        tooltip_fields.append(transformed_name)

    unique_tooltip_fields = list(dict.fromkeys(tooltip_fields))
    all_categorical_fields = list(dict.fromkeys(["cluster", *metadata_fields]))

    return {
        "id_field": "trader_id",
        "cluster_field": "cluster",
        "numeric_plot_fields": numeric_features,
        "categorical_fields": all_categorical_fields,
        "transformed_numeric_fields": list(transformed_columns.values()),
        "clustering_input_fields": clustering_input_fields,
        "tooltip_fields": unique_tooltip_fields,
        "labels": {
            field: humanize_label(field)
            for field in ["trader_id", "cluster", *numeric_features, *metadata_fields, *transformed_columns.values()]
        },
    }


def build_run_metadata(
    *,
    input_path: Path,
    resolved_schema: ResolvedSchema,
    preprocessing: PreprocessingArtifacts,
    n_clusters: int,
    cluster_values: list[int],
    random_state: int,
    n_init: int,
    output_paths: PipelineOutputs,
) -> dict[str, Any]:
    """Collect transparency metadata for the clustering handoff."""

    return {
        "created_at_utc": datetime.now(timezone.utc).isoformat(),
        "input_path": str(input_path),
        "resolved_schema": {
            "canonical_to_source": resolved_schema.canonical_to_source,
            "numeric_features": resolved_schema.numeric_features,
            "metadata_fields": resolved_schema.metadata_fields,
        },
        "preprocessing": {
            "numeric_imputation": {
                "strategy": "median",
                "median_by_feature": preprocessing.median_imputations,
                "imputed_row_count_by_feature": preprocessing.imputed_counts,
            },
            "categorical_imputation": {
                "strategy": "fill_with_unknown",
                "fields": preprocessing.categorical_fill_fields,
            },
            "transformed_columns": preprocessing.transformed_columns,
            "standardized_columns": preprocessing.standardized_columns,
        },
        "clustering": {
            "algorithm": "kmeans",
            "n_clusters": n_clusters,
            "random_state": random_state,
            "n_init": n_init,
            "evaluated_k_values": cluster_values,
        },
        "outputs": {
            key: str(value)
            for key, value in asdict(output_paths).items()
            if value is not None
        },
    }


def save_evaluation_plot(metrics_df: pd.DataFrame, output_path: Path) -> Path | None:
    """Save a quick elbow/silhouette figure."""

    if metrics_df.empty:
        return None

    fig, axes = plt.subplots(1, 2, figsize=(12, 4))

    axes[0].plot(metrics_df["k"], metrics_df["inertia"], marker="o", color="#1f77b4")
    axes[0].set_title("KMeans Inertia")
    axes[0].set_xlabel("Number of Clusters (k)")
    axes[0].set_ylabel("Inertia")

    axes[1].plot(metrics_df["k"], metrics_df["silhouette_score"], marker="o", color="#ff7f0e")
    axes[1].set_title("Silhouette Score")
    axes[1].set_xlabel("Number of Clusters (k)")
    axes[1].set_ylabel("Silhouette")

    plt.tight_layout()
    fig.savefig(output_path, dpi=200, bbox_inches="tight")
    plt.close(fig)
    return output_path


def generate_mock_trader_features(
    *,
    n_traders: int = 120,
    random_state: int = 42,
) -> pd.DataFrame:
    """Create a local, clustering-friendly mock trader feature table."""

    rng = np.random.default_rng(random_state)
    archetypes = [
        {
            "name": "casual",
            "category": "Politics",
            "market": "Election 2028",
            "count": max(1, n_traders // 4),
            "win_rate": (0.44, 0.07),
            "avg_trade_size": (35, 12),
            "total_trade_volume": (800, 250),
            "total_trade_number": (18, 8),
            "trades_per_day": (0.4, 0.15),
            "net_gains_loss": (-45, 55),
            "avg_odds": (0.56, 0.08),
        },
        {
            "name": "high_volume",
            "category": "Sports",
            "market": "NBA Finals Winner",
            "count": max(1, n_traders // 4),
            "win_rate": (0.53, 0.05),
            "avg_trade_size": (180, 45),
            "total_trade_volume": (16000, 3500),
            "total_trade_number": (150, 25),
            "trades_per_day": (4.8, 1.0),
            "net_gains_loss": (420, 260),
            "avg_odds": (0.51, 0.06),
        },
        {
            "name": "specialist",
            "category": "Crypto",
            "market": "BTC > 100k",
            "count": max(1, n_traders // 4),
            "win_rate": (0.61, 0.06),
            "avg_trade_size": (110, 30),
            "total_trade_volume": (9500, 1800),
            "total_trade_number": (82, 18),
            "trades_per_day": (2.1, 0.5),
            "net_gains_loss": (1250, 420),
            "avg_odds": (0.44, 0.07),
        },
        {
            "name": "longshot",
            "category": "Entertainment",
            "market": "Oscars Surprise Winner",
            "count": n_traders - (3 * max(1, n_traders // 4)),
            "win_rate": (0.31, 0.08),
            "avg_trade_size": (70, 20),
            "total_trade_volume": (2400, 700),
            "total_trade_number": (44, 12),
            "trades_per_day": (1.1, 0.3),
            "net_gains_loss": (-360, 180),
            "avg_odds": (0.77, 0.08),
        },
    ]

    rows: list[dict[str, Any]] = []
    trader_index = 0
    for archetype in archetypes:
        for _ in range(archetype["count"]):
            trader_index += 1
            rows.append(
                {
                    "trader_id": f"0xmock{trader_index:036x}",
                    "win_rate": float(np.clip(rng.normal(*archetype["win_rate"]), 0.02, 0.98)),
                    "avg_trade_size": float(max(1.0, rng.normal(*archetype["avg_trade_size"]))),
                    "total_trade_volume": float(max(25.0, rng.normal(*archetype["total_trade_volume"]))),
                    "total_trade_number": int(max(1, round(rng.normal(*archetype["total_trade_number"])))),
                    "trades_per_day": float(max(0.05, rng.normal(*archetype["trades_per_day"]))),
                    "net_gains_loss": float(rng.normal(*archetype["net_gains_loss"])),
                    "avg_odds": float(np.clip(rng.normal(*archetype["avg_odds"]), 0.02, 0.99)),
                    "most_common_market": archetype["market"],
                    "category": archetype["category"],
                    "archetype_hint": archetype["name"],
                }
            )

    mock_df = pd.DataFrame(rows)

    missing_rows = mock_df.sample(frac=0.08, random_state=random_state).index
    mock_df.loc[missing_rows[: max(1, len(missing_rows) // 2)], "avg_odds"] = np.nan
    mock_df.loc[missing_rows[max(1, len(missing_rows) // 2) :], "category"] = None

    return mock_df


def run_trader_clustering_pipeline(
    *,
    input_path: Path,
    output_dir: Path,
    n_clusters: int = 4,
    numeric_features: list[str] | None = None,
    metadata_fields: list[str] | None = None,
    alias_map: dict[str, list[str]] | None = None,
    log_features: list[str] | None = None,
    signed_log_features: list[str] | None = None,
    cluster_values: list[int] | None = None,
    random_state: int = 42,
    n_init: int = 10,
    output_format: str = "both",
    mock_input_path: Path | None = None,
) -> PipelineOutputs:
    """Execute the full clustering + visualization handoff pipeline."""

    output_dir.mkdir(parents=True, exist_ok=True)

    numeric_features = numeric_features or list(DEFAULT_NUMERIC_FEATURES)
    metadata_fields = metadata_fields or list(DEFAULT_METADATA_FIELDS)
    cluster_values = cluster_values or list(range(2, 9))
    if not cluster_values:
        raise ValueError("At least one K value is required for evaluation.")

    feature_df = load_feature_table(input_path)
    resolved_schema = resolve_schema(
        feature_df,
        alias_map=alias_map,
        numeric_features=numeric_features,
        metadata_fields=metadata_fields,
    )
    normalized_df = add_canonical_columns(feature_df, resolved_schema)
    preprocessing = preprocess_for_clustering(
        normalized_df,
        numeric_features=resolved_schema.numeric_features,
        metadata_fields=resolved_schema.metadata_fields,
        log_features=log_features,
        signed_log_features=signed_log_features,
    )
    eval_metrics = evaluate_cluster_range(
        preprocessing.scaled_matrix,
        cluster_values=cluster_values,
        random_state=random_state,
        n_init=n_init,
    )
    cluster_labels = fit_kmeans(
        preprocessing.scaled_matrix,
        n_clusters=n_clusters,
        random_state=random_state,
        n_init=n_init,
    )

    clustered_df = preprocessing.cleaned_frame.copy()
    clustered_df["cluster"] = cluster_labels.astype(int)
    cluster_summary = summarize_clusters(
        clustered_df,
        numeric_features=resolved_schema.numeric_features,
        metadata_fields=resolved_schema.metadata_fields,
    )
    visualization_config = build_visualization_config(
        numeric_features=resolved_schema.numeric_features,
        metadata_fields=resolved_schema.metadata_fields,
        transformed_columns=preprocessing.transformed_columns,
    )

    clustered_csv_path: Path | None = None
    if output_format in {"csv", "both"}:
        clustered_csv_path = output_dir / "clustered_traders.csv"
        clustered_df.to_csv(clustered_csv_path, index=False)

    clustered_parquet_path: Path | None = None
    if output_format in {"parquet", "both"}:
        clustered_parquet_path = output_dir / "clustered_traders.parquet"
        clustered_df.to_parquet(clustered_parquet_path, index=False)

    cluster_summary_path = output_dir / "cluster_summary.csv"
    cluster_summary.to_csv(cluster_summary_path, index=False)

    eval_metrics_path = output_dir / "cluster_eval_metrics.csv"
    eval_metrics.to_csv(eval_metrics_path, index=False)

    eval_plot_path = save_evaluation_plot(eval_metrics, output_dir / "cluster_eval_plot.png")

    visualization_config_path = output_dir / "visualization_config.json"
    visualization_config_path.write_text(json.dumps(visualization_config, indent=2))

    output_paths = PipelineOutputs(
        clustered_traders_csv=clustered_csv_path,
        clustered_traders_parquet=clustered_parquet_path,
        cluster_summary_csv=cluster_summary_path,
        cluster_eval_metrics_csv=eval_metrics_path,
        cluster_eval_plot_png=eval_plot_path,
        clustering_metadata_json=output_dir / "clustering_metadata.json",
        visualization_config_json=visualization_config_path,
        mock_input_csv=mock_input_path,
    )

    metadata = build_run_metadata(
        input_path=input_path,
        resolved_schema=resolved_schema,
        preprocessing=preprocessing,
        n_clusters=n_clusters,
        cluster_values=cluster_values,
        random_state=random_state,
        n_init=n_init,
        output_paths=output_paths,
    )
    output_paths.clustering_metadata_json.write_text(json.dumps(metadata, indent=2))

    return output_paths


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    """Build the command-line interface."""

    parser = argparse.ArgumentParser(
        description="Run trader clustering and emit visualization-ready outputs."
    )
    parser.add_argument(
        "--input",
        type=Path,
        help="Path to the trader-level feature table (CSV or Parquet). If omitted, a mock dataset is generated.",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("output") / "trader_clustering",
        help="Directory for clustering outputs.",
    )
    parser.add_argument("--k", type=int, default=4, help="Number of KMeans clusters.")
    parser.add_argument(
        "--features",
        nargs="+",
        default=None,
        help="Canonical numeric feature names to cluster on.",
    )
    parser.add_argument(
        "--metadata-fields",
        nargs="*",
        default=None,
        help="Canonical metadata fields to carry into tooltips and summaries.",
    )
    parser.add_argument(
        "--log-features",
        nargs="*",
        default=list(DEFAULT_LOG_FEATURES),
        help="Canonical numeric features that should use log1p before scaling.",
    )
    parser.add_argument(
        "--signed-log-features",
        nargs="*",
        default=list(DEFAULT_SIGNED_LOG_FEATURES),
        help="Canonical numeric features that should use signed log1p before scaling.",
    )
    parser.add_argument("--eval-min-k", type=int, default=2, help="Minimum k to evaluate.")
    parser.add_argument("--eval-max-k", type=int, default=8, help="Maximum k to evaluate.")
    parser.add_argument("--random-state", type=int, default=42, help="Random seed for reproducibility.")
    parser.add_argument("--n-init", type=int, default=10, help="KMeans n_init value.")
    parser.add_argument(
        "--output-format",
        choices=["csv", "parquet", "both"],
        default="both",
        help="Whether to write the clustered trader table as CSV, Parquet, or both.",
    )
    parser.add_argument(
        "--mock-rows",
        type=int,
        default=120,
        help="Number of traders to generate when no input file is supplied.",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    """CLI entry point for local clustering runs."""

    args = parse_args(argv)
    args.output_dir.mkdir(parents=True, exist_ok=True)
    if args.eval_min_k > args.eval_max_k:
        raise ValueError("--eval-min-k must be less than or equal to --eval-max-k.")

    input_path = args.input
    mock_input_path: Path | None = None
    if input_path is None:
        mock_input_path = args.output_dir / "mock_trader_features.csv"
        generate_mock_trader_features(
            n_traders=args.mock_rows,
            random_state=args.random_state,
        ).to_csv(mock_input_path, index=False)
        input_path = mock_input_path

    output_paths = run_trader_clustering_pipeline(
        input_path=input_path,
        output_dir=args.output_dir,
        n_clusters=args.k,
        numeric_features=args.features,
        metadata_fields=args.metadata_fields,
        log_features=args.log_features,
        signed_log_features=args.signed_log_features,
        cluster_values=list(range(args.eval_min_k, args.eval_max_k + 1)),
        random_state=args.random_state,
        n_init=args.n_init,
        output_format=args.output_format,
        mock_input_path=mock_input_path,
    )

    print("Trader clustering outputs:")
    for field_name, value in asdict(output_paths).items():
        if value is not None:
            print(f"  {field_name}: {value}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
