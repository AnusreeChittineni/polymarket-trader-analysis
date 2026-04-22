import pandas as pd
import numpy as np

from sklearn.preprocessing import StandardScaler
from sklearn.cluster import KMeans
from sklearn.manifold import TSNE
from sklearn.decomposition import PCA

import matplotlib.pyplot as plt
import seaborn as sns
import hdbscan

df = pd.read_csv("/home/hice1/achakraborty75/scratch/dva/samples/updated_trader_stats.csv")

# win_rate_ignore_sales,avg_trade_size_ignore_sales,total_trade_volume_ignore_sales,total_trade_number_ignore_sales,frequency_ignore_sales,net_gains_loss_ignore_sales,avg_odds_ignore_sales,profit_per_trade_ignore_sales

# features = [
#     "win_rate_ignore_sales",
#     "avg_odds",
#     "net_gains_loss",
#     "profit_per_trade",
#     "total_trade_number_ignore_sales",
#     "frequency",
#     "avg_trade_size",
#     "total_trade_volume_ignore_sales"
# ]

features = [
    "win_rate_ignore_sales",
    "avg_odds_ignore_sales",
    "profit_per_trade_ignore_sales",

    # "net_gains_loss",

    "total_trade_number",
    "frequency",

    "avg_trade_size",
    "total_trade_volume"
]

df = pd.get_dummies(df, columns=["primary_category"], prefix="category")
category_cols = list(df.filter(like="category_").columns)

X_raw = df[features].copy()
X_raw = X_raw.replace([np.inf, -np.inf], np.nan).fillna(0)

# log_scale_cols = [
#     "total_trade_volume",
#     "avg_trade_size",
#     "total_trade_number"
# ]

log_scale_cols = [
    "total_trade_volume",
    "avg_trade_size",
    "total_trade_number",
    "frequency",
    # "net_gains_loss"
]


for col in log_scale_cols:
    X_raw[col] = np.sign(X_raw[col]) * np.log1p(np.abs(X_raw[col]))

X_raw["frequency"] = X_raw["frequency"] * 0.3
scaler = StandardScaler()
X_num = scaler.fit_transform(X_raw)
X_cat = df[category_cols].values
X = np.hstack([X_num, X_cat])



kmeans = KMeans(n_clusters=7, random_state=42, n_init=20)
df["kmeans_cluster"] = kmeans.fit_predict(X)

hdb = hdbscan.HDBSCAN(min_cluster_size=23, min_samples=1)
df["hdbscan_cluster"] = hdb.fit_predict(X)

tsne = TSNE(
    n_components=2,
    perplexity=min(30, max(5, len(df)//10)),
    learning_rate="auto",
    init="pca",
    random_state=42
)

X_2d = tsne.fit_transform(X)

df["tsne_1"] = X_2d[:, 0]
df["tsne_2"] = X_2d[:, 1]



kmeans_clusters = sorted(df["kmeans_cluster"].unique())

kmeans_palette = sns.color_palette("husl", len(kmeans_clusters))

color_map = {
    c: kmeans_palette[i]
    for i, c in enumerate(kmeans_clusters)
}

hdb_clusters = sorted(df["hdbscan_cluster"].unique())
non_noise = [c for c in hdb_clusters if c != -1]

hdb_palette = sns.color_palette("husl", len(non_noise))

hdb_color_map = {}

for i, c in enumerate(non_noise):
    hdb_color_map[c] = hdb_palette[i]

# noise = gray
if -1 in hdb_clusters:
    hdb_color_map[-1] = (0.6, 0.6, 0.6)


# avg_odds vs win_rate
plt.figure(figsize=(10, 7))
sns.scatterplot(
    data=df,
    x="avg_odds_ignore_sales",
    y="win_rate_ignore_sales",
    hue="hdbscan_cluster",
    palette=hdb_color_map,
    s=60,
    alpha=0.85
)


plt.title("avg_odds vs win_rate (HDBSCAN)")
plt.legend(bbox_to_anchor=(1.05, 1), loc="upper left")
plt.tight_layout()
plt.savefig("avg_odds_hdbscan.png")
plt.close()

plt.figure(figsize=(10, 7))
sns.scatterplot(
    data=df,
    x="avg_odds_ignore_sales",
    y="win_rate_ignore_sales",
    hue="kmeans_cluster",
    palette=color_map,
    s=60,
    alpha=0.85
)

plt.title("avg_odds vs win_rate (KMEANS)")
plt.legend(bbox_to_anchor=(1.05, 1), loc="upper left")
plt.tight_layout()
plt.savefig("avg_odds_kmeans.png")
plt.close()


# KMEANS
plt.figure(figsize=(10, 7))
sns.scatterplot(
    data=df,
    x="tsne_1",
    y="tsne_2",
    hue="kmeans_cluster",
    palette=color_map,
    s=60,
    alpha=0.85
)

plt.title("KMeans Clusters (t-SNE)")
plt.legend(bbox_to_anchor=(1.05, 1), loc="upper left")
plt.tight_layout()
plt.savefig("kmeans.png")
plt.close()


# HDBSCAN
plt.figure(figsize=(10, 7))
sns.scatterplot(
    data=df,
    x="tsne_1",
    y="tsne_2",
    hue="hdbscan_cluster",
    palette=hdb_color_map,
    s=60,
    alpha=0.9
)

plt.title("HDBSCAN Clusters (t-SNE)")
plt.legend(bbox_to_anchor=(1.05, 1), loc="upper left")
plt.tight_layout()
plt.savefig("hdbscan.png")
plt.close()

def build_cluster_report(df, cluster_col, feature_cols, name, output_file=None):
    global_mean = df[feature_cols].mean()
    global_median = df[feature_cols].median()

    lines = []
    lines.append(f"CLUSTER REPORT: {name}")
    lines.append("=" * 60)
    lines.append("")

    for c in sorted(df[cluster_col].unique()):
        lines.append("-" * 40)
        lines.append(f"Cluster {c}")
        lines.append("-" * 40)

        cluster_df = df[df[cluster_col] == c]

        for f in feature_cols:
            mean = cluster_df[f].mean()
            median = cluster_df[f].median()
            g = global_mean[f]

            diff = mean - g
            pct = (diff / g * 100) if g != 0 else 0

            lines.append(
                f"{f}: mean={mean:.4f}, median={median:.4f}, "
                f"global={g:.4f}, diff={diff:+.4f}, {pct:+.1f}%"
            )

        lines.append("")

    report = "\n".join(lines)

    if output_file:
        with open(output_file, "w") as f:
            f.write(report)

    print(report)


build_cluster_report(
    df,
    cluster_col="hdbscan_cluster",
    feature_cols=features,
    name="hdbscan",
    output_file="hdbscan_cluster_report.txt"
)

build_cluster_report(
    df,
    cluster_col="kmeans_cluster",
    feature_cols=features,
    name="kmeans",
    output_file="kmeans_cluster_report.txt"
)

df.to_csv("clustered_traders.csv", index=False)

print("Done.")