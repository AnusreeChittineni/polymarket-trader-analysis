/* global d3 */

// Canvas + D3 axes for large scatter.
// Each point: trader×category.

const DATA_URL = "clustered_traders.csv";

const KMEANS_NUM_CLUSTERS = 7;
const DBSCAN_MAX_COLORS = 21;

const kmeansClusterColor = d3
  .scaleOrdinal()
  .domain(d3.range(KMEANS_NUM_CLUSTERS))
  .range(d3.range(KMEANS_NUM_CLUSTERS).map((i) => `hsl(${i * (360 / KMEANS_NUM_CLUSTERS)}, 70%, 55%)`));

// DBSCAN/HDBSCAN can produce more clusters; cycle through a larger palette.
const dbscanClusterColor = d3
  .scaleOrdinal()
  .domain(d3.range(DBSCAN_MAX_COLORS))
  .range(d3.range(DBSCAN_MAX_COLORS).map((i) => `hsl(${i * (360 / DBSCAN_MAX_COLORS)}, 70%, 55%)`));

function getClusterColor(label) {
  const method = getClusterMethod();
  // For DBSCAN-like methods, -1 often means "noise".
  if (method === "dbscan" && label === -1) return "rgba(148, 163, 184, 0.9)";
  if (method === "dbscan") {
    // Keep colors stable even if labels are large/sparse.
    const idx = ((label % DBSCAN_MAX_COLORS) + DBSCAN_MAX_COLORS) % DBSCAN_MAX_COLORS;
    return dbscanClusterColor(idx);
  }
  return kmeansClusterColor(label);
}

const els = {
  canvas: document.getElementById("plot"),
  svg: document.getElementById("axes"),
  tooltip: document.getElementById("tooltip"),
  legend: document.getElementById("legend"),
  clusterInfo: document.getElementById("clusterInfo"),
  status: document.getElementById("status"),
  xAxis: document.getElementById("xAxis"),
  yAxis: document.getElementById("yAxis"),
  maxPoints: document.getElementById("maxPoints"),
  minPositions: document.getElementById("minPositions"),
  category: document.getElementById("category"),
  pointAlpha: document.getElementById("pointAlpha"),
  includeSales: document.getElementById("includeSales"),
  clusterMethod: document.getElementById("clusterMethod"),
  reload: document.getElementById("reload"),
  viz: document.getElementById("viz"),
  plotWrap: document.getElementById("plotWrap"),
};

const DPR = Math.max(1, Math.min(3, window.devicePixelRatio || 1));

const margin = { top: 14, right: 18, bottom: 42, left: 56 };

let raw = [];
let categoriesAll = [];
let xScale, yScale;
let xIsLog = false;
let yIsLog = false;
let xIsAsinh = false;
let yIsAsinh = false;
let points = []; // points with screen coords for hit-testing
let hovered = null; // { x, y, r, d }

function asinh(x) {
  // Math.asinh exists in modern browsers; keep a safe fallback.
  if (!Number.isFinite(x)) return NaN;
  if (typeof Math.asinh === "function") return Math.asinh(x);
  return Math.log(x + Math.sqrt(x * x + 1));
}

function clearHover() {
  hovered = null;
  els.tooltip.style.display = "none";
}

function fmt(n, digits = 0) {
  if (!Number.isFinite(n)) return "";
  return n.toLocaleString(undefined, {
    maximumFractionDigits: digits,
    minimumFractionDigits: digits,
  });
}

function median(arr) {
  const xs = arr.filter((v) => Number.isFinite(v)).slice().sort((a, b) => a - b);
  if (!xs.length) return NaN;
  const mid = Math.floor(xs.length / 2);
  return xs.length % 2 ? xs[mid] : (xs[mid - 1] + xs[mid]) / 2;
}

function buildKMeansLegendStats() {
  // Use the same include/exclude sales mode as the plot.
  const idx = els.includeSales.checked ? 0 : 1;

  const by = new Map();
  for (const d of raw) {
    const c = d.kmeans?.[idx];
    if (!Number.isFinite(c)) continue;
    if (!by.has(c)) by.set(c, []);
    by.get(c).push(d);
  }

  const clusters = [...by.keys()].sort((a, b) => a - b);
  const stats = clusters.map((c) => {
    const g = by.get(c);
    const win = median(g.map((d) => d.win_rate?.[idx]));
    const trades = median(g.map((d) => Math.exp(d.total_number?.[idx] ?? NaN)));
    const avgSize = median(g.map((d) => Math.exp(d.avg_size?.[idx] ?? NaN)));
    const ppt = median(g.map((d) => d.profit_per_trade?.[idx]));
    const odds = median(g.map((d) => d.avg_odds?.[idx]));

    return {
      cluster: c,
      n: g.length,
      win,
      trades,
      avgSize,
      ppt,
      odds,
    };
  });

  // Compute percentile ranks (0-1) within the clusters for readable naming.
  const rankOf = (key) => {
    const xs = stats.map((s) => s[key]);
    const sorted = xs.slice().sort((a, b) => a - b);
    return (v) => {
      if (!Number.isFinite(v)) return 0.5;
      // rank by position in sorted list
      const i = sorted.findIndex((x) => x >= v);
      const j = i === -1 ? sorted.length - 1 : i;
      return sorted.length <= 1 ? 1 : j / (sorted.length - 1);
    };
  };

  const rWin = rankOf("win");
  const rTrades = rankOf("trades");
  const rSize = rankOf("avgSize");
  const rPpt = rankOf("ppt");
  const rOdds = rankOf("odds");

  for (const s of stats) {
    s.ranks = {
      win: rWin(s.win),
      trades: rTrades(s.trades),
      size: rSize(s.avgSize),
      ppt: rPpt(s.ppt),
      odds: rOdds(s.odds),
    };

    // Heuristic archetype naming.
    const tags = [];
    if (s.ranks.win >= 0.8) tags.push("high win-rate");
    else if (s.ranks.win <= 0.2) tags.push("low win-rate");

    if (s.ranks.trades >= 0.8) tags.push("very active");
    else if (s.ranks.trades <= 0.2) tags.push("low activity");

    if (s.ranks.size >= 0.8) tags.push("large trades");
    else if (s.ranks.size <= 0.2) tags.push("small trades");

  if (s.ranks.odds >= 0.8) tags.push("longshot-leaning");

    if (s.ranks.ppt >= 0.8) tags.push("profitable per trade");
    else if (s.ranks.ppt <= 0.2) tags.push("unprofitable per trade");

  // Make a short 2–3 tag title.
  const title = tags.slice(0, 3).join(" · ") || "mixed";

  // Friendly, stable-ish archetype name derived from cluster medians.
  // (Heuristic, but more readable than "Cluster N".)
  let name = "Mixed";
  if (s.ranks.size >= 0.85 && s.ranks.trades >= 0.65) name = "Whales";
  else if (s.ranks.trades >= 0.85 && s.ranks.size <= 0.45) name = "Grinders";
  else if (s.ranks.trades <= 0.25 && s.ranks.size <= 0.35) name = "Gamblers";
  else if (s.ranks.win >= 0.80 && s.ranks.ppt >= 0.65) name = "Good Traders";
  else if (s.ranks.win <= 0.20 && s.ranks.ppt <= 0.35) name = "Bad Traders";
  else if (s.ranks.odds >= 0.85) name = "Longshot Bettors";
  else if (s.ranks.odds <= 0.15) name = "Favorites Bettors";

  // Manual overrides for specific cluster labels to match the narrative.
  if (s.cluster === 3) name = "Bonders";
  if (s.cluster === 5) name = "Suspiciously Good Traders";
  if (s.cluster === 1) name = "Average Traders";
  if (s.cluster === 4) name = "Slightly Richer Average Traders";

  // Clearer, sentence-like characterization.
  const activityWord = s.ranks.trades >= 0.75 ? "high" : s.ranks.trades <= 0.25 ? "low" : "moderate";
  const sizeWord = s.ranks.size >= 0.75 ? "large" : s.ranks.size <= 0.25 ? "small" : "medium";
  const winWord = s.ranks.win >= 0.75 ? "above-average" : s.ranks.win <= 0.25 ? "below-average" : "mixed";
  const pptWord = s.ranks.ppt >= 0.75 ? "strong" : s.ranks.ppt <= 0.25 ? "weak" : "mixed";
  const tiltWord = s.cluster === 3
    ? "takes trades with safe odds"
    : (s.ranks.odds >= 0.75 ? "leans longshots" : "mixed odds");

  const pretty = s.cluster === 5
    ? "few trades, unusually high win rate, and high volume."
    : s.cluster === 1
      ? "medium traders taking smallish trades with average profits."
      : s.cluster === 4
        ? "medium traders with slightly more spending money on medium sized trades."
        : `${activityWord} activity, ${sizeWord} trades, ${tiltWord}; win rate ${winWord} with ${pptWord} profit per trade.`;

  // Use tags as a short subtitle in the legend.
  const subtitle = title;

  s.name = name;
  s.pretty = pretty;
  s.title = `${name} (Cluster ${s.cluster})`;
  s.subtitle = subtitle;
  s.desc = `Traders: ${fmt(s.n)} · ${pretty}  Win rate: ${Number.isFinite(s.win) ? fmt(s.win, 3) : ""} · Trades/trader (median): ~${fmt(s.trades, 0)} · Avg size: ~${fmt(s.avgSize, 1)} · Profit/trade: ~${Number.isFinite(s.ppt) ? fmt(s.ppt, 3) : ""}`;
  }

  return stats;
}

function renderClusterInfoPanel() {
  if (!els.clusterInfo) return;

  const svg = d3.select(els.clusterInfo);
  svg.selectAll("*").remove();

  const method = getClusterMethod();
  if (method !== "kmeans") {
    const w = els.clusterInfo.clientWidth || els.clusterInfo.getBoundingClientRect().width || 600;
    const h = els.clusterInfo.clientHeight || els.clusterInfo.getBoundingClientRect().height || 140;
    svg
      .attr("width", w)
      .attr("height", h);
    svg
      .append("text")
      .attr("x", 12)
      .attr("y", 20)
      .attr("class", "sub")
      .text("Cluster descriptions are shown for KMeans mode.");
    return;
  }

  const stats = buildKMeansLegendStats();
  const w = els.clusterInfo.clientWidth || els.clusterInfo.getBoundingClientRect().width || 600;
  const h = els.clusterInfo.clientHeight || els.clusterInfo.getBoundingClientRect().height || 140;
  svg
    .attr("width", w)
    .attr("height", h);

  svg
    .append("text")
    .attr("x", 12)
    .attr("y", 18)
    .attr("class", "sub")
    .text("Each line: archetype name + plain-English characterization.");

  const rowH = 24;
  const startY = 44;
  const maxRows = Math.floor((h - startY - 10) / rowH);
  /*const rows = stats.slice(0, Math.max(0, maxRows));*/
  const rows = stats;

  rows.forEach((s, i) => {
    const y = startY + i * rowH;
  const color = getClusterColor(s.cluster);

    svg
      .append("circle")
      .attr("class", "sw")
      .attr("cx", 16)
      .attr("cy", y - 4)
      .attr("r", 5)
      .attr("fill", color);

  const label = `${s.name}: ${s.pretty}`;
    svg
      .append("text")
      .attr("x", 28)
      .attr("y", y)
      .text(label);
  });

  if (stats.length > rows.length) {
    svg
      .append("text")
      .attr("x", 12)
      .attr("y", h - 10)
      .attr("class", "sub")
      .text(`Showing ${rows.length}/${stats.length} clusters (panel height limited).`);
  }
}

function renderLegend() {
  if (!els.legend) return;

  const method = getClusterMethod();
  if (method !== "kmeans") {
    els.legend.innerHTML = `
      <h3>Cluster legend</h3>
      <p class="sub">Switch to KMeans to see cluster archetype descriptions.</p>
    `;
    return;
  }

  const stats = buildKMeansLegendStats();
  const mode = els.includeSales.checked ? "including sales" : "excluding sales";

  const rows = stats
    .map((s) => {
  const color = getClusterColor(s.cluster);
      return `
        <div class="row">
          <div class="swatch" style="background:${color}"></div>
          <div>
            <div class="title">${s.title}</div>
            <div class="desc">${s.desc}</div>
          </div>
        </div>
      `;
    })
    .join("");

  els.legend.innerHTML = `
    <h3>KMeans cluster legend</h3>
    <p class="sub">Archetypes are summarized from medians (${mode}).</p>
    <details style="margin:10px 0 12px 0;">
      <summary style="cursor:pointer; color:#cbd5e1;">Quick guide: best views & settings</summary>
      <div class="sub" style="margin-top:8px; line-height:1.35;">
        <div><b>Recommended axis combinations</b> (to see how clusters differ):</div>
        <ul style="margin:6px 0 8px 18px; padding:0;">
          <li><b>X:</b> Avg Odds vs <b>Y:</b> Avg Trade Size</li>
          <li><b>X:</b> Win Rate vs <b>Y:</b> Avg Trade Size</li>
          <li><b>X:</b> Avg Odds vs <b>Y:</b> Win Rate</li>
        </ul>

        <div style="margin-top:6px;"><b>Include sales</b>: counts both buys and sells when computing trade counts, volume, and averages.</div>
        <div style="margin-top:4px;">Turning <b>Include sales</b> off (<i>ignore sales</i>) can help when you want to focus on <b>opening bets</b> (entries) rather than exits/position management, which can otherwise dominate activity metrics for some traders.</div>

        <div style="margin-top:8px;"><b>KMeans vs DBSCAN</b>: KMeans tends to produce fewer, more stable clusters that are easier to interpret (the archetypes shown here).
        DBSCAN/HDBSCAN can find <b>more specific groups</b> (more clusters), but the results are often less interpretable and may include a larger “noise” set.</div>

  <div style="margin-top:8px;"><b>Tip</b>: Click a point to open that trader’s Polymarket profile in a new tab.
  Profiles follow <span class="mono">https://polymarket.com/profile/&lt;trader_id&gt;</span>, where <span class="mono">trader_id</span> comes from the CSV.</div>
      </div>
    </details>
    ${rows}
  `;
}

const CATEGORY_ORDER = ["Pop Culture", "Crypto", "Economics", "Other/Misc", "Politics", "Sports"];

function getClusterMethod() {
  return (els.clusterMethod && els.clusterMethod.value) || "kmeans";
}

function getClusterLabel(d, idx) {
  const method = getClusterMethod();
  if (method === "dbscan") return d.dbscan[idx];
  return d.kmeans[idx];
}

function getXAxisField() {
  return (els.xAxis && els.xAxis.value) || "tsne_1";
}

function getYAxisField() {
  return (els.yAxis && els.yAxis.value) || "tsne_2";
}

function isNumericField(field) {
  return [
    "win_rate",
    "avg_trade_size",
    "total_trade_volume",
    "total_trade_number",
    "frequency",
    "net_gains",
    "avg_odds",
    "profit_per_trade",
    "tsne_1",
    "tsne_2"
  ].includes(field);
}

function getValue(d, field, idx) {
  if (field === "win_rate") return d.win_rate[idx];
  if (field === "avg_trade_size") return d.avg_size[idx];
  if (field === "total_trade_volume") return d.total_volume[idx];
  if (field === "total_trade_number") return d.total_number[idx];
  if (field === "profit_per_trade") return d.profit_per_trade[idx];
  if (field === "frequency") return d.frequency[idx];
  if (field === "net_gains") return d.net_gain[idx];
  if (field === "avg_odds") return d.avg_odds[idx];
  if (field === "tsne_1") return d.tsne_1[idx];
  if (field === "tsne_2") return d.tsne_2[idx];
  return null;
}

function setStatus(msg) {
  els.status.textContent = msg;
}

function resize() {
  const target = els.plotWrap || els.viz;
  const rect = target.getBoundingClientRect();
  const width = Math.max(10, rect.width);
  const height = Math.max(10, rect.height);

  els.canvas.width = Math.floor(width * DPR);
  els.canvas.height = Math.floor(height * DPR);
  els.canvas.style.width = `${width}px`;
  els.canvas.style.height = `${height}px`;

  els.svg.setAttribute("width", width);
  els.svg.setAttribute("height", height);

  if (raw.length) {
    buildScales();
    renderAxes();
    renderPoints();
  renderLegend();
  renderClusterInfoPanel();
  }
}

function parseRow(d) {

  // d3.csv returns strings by default; normalize booleans safely.
  const asBool = (v) => v === true || v === "True" || v === "true" || v === 1 || v === "1";

  let category = "Pop Culture";
  if (asBool(d.category_Crypto)) category = "Crypto";
  else if (asBool(d.category_Economics)) category = "Economics";
  else if (asBool(d.category_Other_Misc)) category = "Other/Misc";
  else if (asBool(d.category_Politics)) category = "Politics";
  else if (asBool(d.category_Sports)) category = "Sports";
  else if (asBool(d.category_Pop_Culture)) category = "Pop Culture";

  // index 0 is value including sales, index 1 is value excluding sales
  return {
    trader: [d.trader, d.trader],
    win_rate: [+d.win_rate, +d.win_rate_ignore_sales],
    avg_size: [Math.log(+d.avg_trade_size), Math.log(+d.avg_trade_size_ignore_sales)],
    total_volume: [Math.log(+d.total_trade_volume), Math.log(+d.total_trade_volume_ignore_sales)],
    total_number: [Math.log(+d.total_trade_number), Math.log(+d.total_trade_number_ignore_sales)],
    frequency: [Math.log(+d.frequency), Math.log(+d.frequency_ignore_sales)],
  // Net gains/loss and profit-per-trade are inverted in the source export; flip signs here so
  // positive means profit and negative means loss.
  // (These fields can be negative, so keep raw values and apply a transform at render-time.)
  net_gain: [-+d.net_gains_loss, -+d.net_gains_loss_ignore_sales],
    avg_odds: [+d.avg_odds, +d.avg_odds_ignore_sales],
  profit_per_trade: [-+d.profit_per_trade, -+d.profit_per_trade_ignore_sales],
    category: [category, category],
    // Keep raw one-hot flags so filtering can be done exactly on them.
    category_flags: {
      Crypto: asBool(d.category_Crypto),
      Economics: asBool(d.category_Economics),
      "Other/Misc": asBool(d.category_Other_Misc),
      Politics: asBool(d.category_Politics),
      "Pop Culture": asBool(d.category_Pop_Culture),
      Sports: asBool(d.category_Sports),
    },
    tsne_1: [+d.tsne_1, +d.tsne_1],
    tsne_2: [+d.tsne_2, +d.tsne_2],
    kmeans: [+d.kmeans_cluster, +d.kmeans_cluster],
    dbscan: [+d.hdbscan_cluster, +d.hdbscan_cluster]
  };
}

function populateCategoryDropdown() {
  const existing = new Set([...(els.category.options || [])].map((o) => o.value));
  for (const c of categoriesAll) {
    if (existing.has(c)) continue;
    const opt = document.createElement("option");
    opt.value = c;
    opt.textContent = c;
    els.category.appendChild(opt);
  }
}

function getCategoryValue(d, idx) {
  // Current parseRow stores category as [includingSales, excludingSales] but the label is the same in both.
  if (Array.isArray(d.category)) return d.category[idx] ?? d.category[0];
  return d.category;
}

function filterAndDownsample() {
  const maxPoints = Math.max(1000, Math.min(5000, parseInt(els.maxPoints.value, 10) || 5000));
  const minTrades = Math.max(1, parseInt(els.minPositions.value, 10) || 1);
  const category = els.category.value;

  const xField = getXAxisField();
  const yField = getYAxisField();

  const includeSales = els.includeSales.checked;
  let idx = 0
  if (!includeSales) {
    idx = 1;
  }

  let df = raw;
  if (category) {
    df = df.filter((d) => d.category_flags && d.category_flags[category] === true);
  }

  // Minimum trade-count filter. NOTE: parseRow stores `total_number` as log(total_trade_number),
  // so we compare against exp(total_number).
  df = df.filter((d) => {
    const v = d.total_number?.[idx];
    if (!Number.isFinite(v)) return false;
    const n = Math.exp(v);
    return Number.isFinite(n) && n >= minTrades;
  });

  df = df.filter((d) => (Number.isFinite(getValue(d, xField, idx)) && Number.isFinite(getValue(d, yField, idx))));

  // Keep high-activity points first; if too many, sample the rest deterministically.
  df = df.slice().sort((a, b) => (b.total_number[idx] - a.total_number[idx]) || (b.total_volume[idx] - a.total_volume[idx]));
  
  if (df.length <= maxPoints) return df;

  const head = Math.floor(maxPoints * 0.7);
  const tail = maxPoints - head;
  const keep = df.slice(0, head);

  // Deterministic sample from the remainder using a seeded PRNG.
  const rest = df.slice(head);
  const sampled = sampleDeterministic(rest, tail, 42);
  return keep.concat(sampled);
}

function fieldUsesAsinh(field) {
  return field === "net_gains" || field === "profit_per_trade";
}

function sampleDeterministic(arr, k, seed) {
  // Fisher-Yates shuffle with LCG RNG, then take first k.
  const a = arr.slice();
  let s = seed >>> 0;
  function rand() {
    // LCG constants
    s = (1664525 * s + 1013904223) >>> 0;
    return s / 2 ** 32;
  }
  for (let i = a.length - 1; i > 0; i--) {
    const j = Math.floor(rand() * (i + 1));
    [a[i], a[j]] = [a[j], a[i]];
  }
  return a.slice(0, k);
}

function buildScales() {
  const target = els.plotWrap || els.viz;
  const rect = target.getBoundingClientRect();
  const width = rect.width;
  const height = rect.height;
  const innerW = Math.max(10, width - margin.left - margin.right);
  const innerH = Math.max(10, height - margin.top - margin.bottom);

  const xField = getXAxisField();
  const yField = getYAxisField();

  const includeSales = els.includeSales.checked;
  let idx = 0
  if (!includeSales) {
    idx = 1;
  }

  // Auto log-scale heuristic:
  // - only for numeric fields
  // - only when all values are > 0
  // - only when range ratio is very large (e.g., 1..2000 compared to 1..20)
  // - never for t-SNE coordinates (they include negatives and are already visually meaningful).
  const LOG_RATIO_THRESHOLD = 50;
  const shouldUseLogScale = (field, values) => {
    if (!isNumericField(field)) return false;
    if (field === "tsne_1" || field === "tsne_2") return false;
  // Avg odds is naturally bounded (0–1-ish), so log scaling isn't helpful.
  if (field === "avg_odds") return false;
  // If we're using an asinh transform (to keep negatives), don't use log.
  if (fieldUsesAsinh(field)) return false;
    const xs = values.filter((v) => Number.isFinite(v));
    if (!xs.length) return false;
    const minV = d3.min(xs);
    const maxV = d3.max(xs);
    if (!Number.isFinite(minV) || !Number.isFinite(maxV)) return false;
    if (minV <= 0) return false; // log requires positives
    if (maxV <= 0) return false;
    const ratio = maxV / minV;
    return Number.isFinite(ratio) && ratio >= LOG_RATIO_THRESHOLD;
  };

  const getAxisValues = (field, idx) => {
    const vals = raw.map((d) => getValue(d, field, idx)).filter((v) => Number.isFinite(v));
    // Apply transform only for the chosen axis fields.
    if (fieldUsesAsinh(field)) return vals.map((v) => asinh(v));
    return vals;
  };

  if (isNumericField(xField)) {
  xIsAsinh = fieldUsesAsinh(xField);
  const xs = getAxisValues(xField, idx);
    const xMin = d3.min(xs) ?? 0;
    const xMax = d3.max(xs) ?? 1;
  xIsLog = shouldUseLogScale(xField, xs);
    if (xIsLog) {
      xScale = d3
        .scaleLog()
        .domain([Math.max(xMin, 1e-12), xMax])
        .nice()
        .range([margin.left, margin.left + innerW]);
    } else {
      xScale = d3
        .scaleLinear()
        .domain([xMin, xMax])
        .nice()
        .range([margin.left, margin.left + innerW]);
    }
  } else {
    // x is categorical. Use all categories so axis doesn't jump when filtering.
    xIsLog = false;
  xIsAsinh = false;
    xScale = d3
      .scalePoint()
      .domain(categoriesAll)
      .range([margin.left, margin.left + innerW])
      .padding(0.5);
  }

  if (isNumericField(yField)) {
  yIsAsinh = fieldUsesAsinh(yField);
  const ys = getAxisValues(yField, idx);
    const yMin = d3.min(ys) ?? 0;
    const yMax = d3.max(ys) ?? 1;
    yIsLog = shouldUseLogScale(yField, ys);
    if (yIsLog) {
      yScale = d3
        .scaleLog()
        .domain([Math.max(yMin, 1e-12), yMax])
        .nice()
        .range([margin.top + innerH, margin.top]);
    } else {
      yScale = d3
        .scaleLinear()
        .domain([yMin, yMax])
        .nice()
        .range([margin.top + innerH, margin.top]);
    }
  }
}

function renderAxes() {
  const svg = d3.select(els.svg);
  svg.selectAll("*").remove();

  const xField = getXAxisField();
  const yField = getYAxisField();

  const prettyAxisLabel = (field) => {
    const map = {
      tsne_1: "t-SNE 1",
      tsne_2: "t-SNE 2",
      win_rate: "Win Rate",
      avg_trade_size: "Avg Trade Size",
      total_trade_volume: "Total Trade Volume",
      total_trade_number: "Total Trade Number",
      frequency: "Frequency",
      net_gains: "Net Gains",
      avg_odds: "Avg Odds",
      profit_per_trade: "Profit per Trade",
    };
    return map[field] || field
      .split("_")
      .map((w) => (w ? w[0].toUpperCase() + w.slice(1) : w))
      .join(" ");
  };

  const xAxis = isNumericField(xField)
    ? (xIsLog ? d3.axisBottom(xScale).ticks(10, "~s") : d3.axisBottom(xScale).ticks(10))
    : d3.axisBottom(xScale);
  const yAxis = yIsLog ? d3.axisLeft(yScale).ticks(8, "~s") : d3.axisLeft(yScale).ticks(8);

  const target = els.plotWrap || els.viz;
  const rect = target.getBoundingClientRect();
  const width = rect.width;
  const height = rect.height;

  svg
    .append("g")
    .attr("transform", `translate(0,${height - margin.bottom})`)
    .call(xAxis)
    .call((g) => {
      if (!isNumericField(xField)) {
        g.selectAll("text").attr("transform", "rotate(-20)").style("text-anchor", "end");
      }
    });

  svg
    .append("g")
    .attr("transform", `translate(${margin.left},0)`)
    .call(yAxis);

  svg
    .append("text")
    .attr("x", width / 2)
    .attr("y", height - 5)
    .attr("text-anchor", "middle")
    .attr("fill", "#cbd5e1")
  .text(`${prettyAxisLabel(xField)}${xIsAsinh ? " (asinh)" : (xIsLog ? " (log)" : "")}`);

  svg
    .append("text")
    .attr("transform", "rotate(-90)")
    .attr("x", -height / 2)
    .attr("y", 15)
    .attr("text-anchor", "middle")
    .attr("fill", "#cbd5e1")
  .text(`${prettyAxisLabel(yField)}${yIsAsinh ? " (asinh)" : (yIsLog ? " (log)" : "")}`);
}

function renderPoints() {
  const ctx = els.canvas.getContext("2d");
  const target = els.plotWrap || els.viz;
  const rect = target.getBoundingClientRect();
  const width = rect.width;
  const height = rect.height;

  const xField = getXAxisField();
  const yField = getYAxisField();

  const includeSales = els.includeSales.checked;
  let idx = 0
  if (!includeSales) {
    idx = 1;
  }

  ctx.setTransform(DPR, 0, 0, DPR, 0, 0);
  ctx.clearRect(0, 0, width, height);

  // Plot background
  ctx.fillStyle = "#0b1220";
  ctx.fillRect(0, 0, width, height);

  // Subtle horizontal grid using y-scale ticks (helps on dark background)
  ctx.save();
  ctx.strokeStyle = "rgba(148, 163, 184, 0.12)";
  ctx.lineWidth = 1;
  if (yScale && typeof yScale.ticks === "function") {
    const ticks = yScale.ticks(8);
    for (const v of ticks) {
      const y = yScale(v);
      if (!Number.isFinite(y)) continue;
      ctx.beginPath();
      ctx.moveTo(margin.left, y);
      ctx.lineTo(width - margin.right, y);
      ctx.stroke();
    }
  }
  ctx.restore();

  const alpha = Math.max(0.05, Math.min(1, parseFloat(els.pointAlpha.value) || 0.35));

  const df = filterAndDownsample();
  setStatus(`Rendering ${df.length.toLocaleString()} points…`);

  // Build screen coords for hit-testing (ONLY for points that were actually drawn).
  points = [];

  // Encode win in color via a simple blue->green gradient.
  function colorForWin(w) {
    const t = Math.max(0, Math.min(1, (w - 0.25) / 0.5));
    // interpolate between two RGB colors
    const c0 = [96, 165, 250]; // blue-400 (brighter)
    const c1 = [52, 211, 153]; // emerald-400 (brighter)
    const r = Math.round(c0[0] + (c1[0] - c0[0]) * t);
    const g = Math.round(c0[1] + (c1[1] - c0[1]) * t);
    const b = Math.round(c0[2] + (c1[2] - c0[2]) * t);
    return `rgba(${r},${g},${b},${alpha})`;
  }

  // Radius scaled by sqrt(volume) but clamped.
  const vols = df.map((d) => (Number.isFinite(Math.exp(d.total_volume[idx])) ? Math.exp(d.total_volume[idx]) : 0));
  const vMax = Math.max(1, ...vols);
  const rScale = (v) => {
    const t = Math.sqrt(Math.max(0, v) / vMax);
    return 2 + 6 * t;
  };

  const method = getClusterMethod();

  // Render
  for (let i = 0; i < df.length; i++) {
    const d = df[i];
  const xv0 = getValue(d, xField, idx);
  const yv0 = getValue(d, yField, idx);
  const xv = xIsAsinh ? asinh(xv0) : xv0;
  const yv = yIsAsinh ? asinh(yv0) : yv0;
  const x = xScale(xv);
  const y = yScale(yv);
    if (!Number.isFinite(x) || !Number.isFinite(y)) continue;

    const r = rScale(d.total_volume[idx]);
    ctx.beginPath();
    const c = getClusterLabel(d, idx);
    const col = getClusterColor(c);
    ctx.fillStyle = (method === "dbscan" && c === -1)
      ? `rgba(148, 163, 184, ${alpha})`
      : col;
    ctx.arc(x, y, r, 0, Math.PI * 2);
    ctx.fill();

    points.push({ x, y, r, d });
  }

  // Draw hover highlight last so it sits above all points.
  if (hovered && Number.isFinite(hovered.x) && Number.isFinite(hovered.y)) {
    // If the hovered point isn't in the currently-rendered points, drop it.
    // This prevents stale hover state after filtering/axis changes.
    const stillVisible = points.some((p) => p.d === hovered.d);
    if (!stillVisible) {
      clearHover();
      setStatus(`Showing ${df.length.toLocaleString()} points`);
      return;
    }

    ctx.save();
    // Outer glow
    ctx.globalCompositeOperation = "lighter";
    ctx.shadowColor = "rgba(99, 102, 241, 0.95)"; // indigo-ish
    ctx.shadowBlur = 14;
    ctx.lineWidth = 2;
    ctx.strokeStyle = "rgba(167, 139, 250, 0.95)";
    ctx.beginPath();
    ctx.arc(hovered.x, hovered.y, Math.max(4.5, hovered.r + 2.5), 0, Math.PI * 2);
    ctx.stroke();

    // Crisp inner ring
    ctx.shadowBlur = 0;
    ctx.lineWidth = 2;
    ctx.strokeStyle = "rgba(255,255,255,0.9)";
    ctx.beginPath();
    ctx.arc(hovered.x, hovered.y, Math.max(3.5, hovered.r + 1.2), 0, Math.PI * 2);
    ctx.stroke();
    ctx.restore();
  }

  setStatus(`Showing ${df.length.toLocaleString()} points`);
}

function formatNumber(n, digits = 0) {
  if (!Number.isFinite(n)) return "";
  return n.toLocaleString(undefined, {
    maximumFractionDigits: digits,
    minimumFractionDigits: digits,
  });
}

function formatMaybeExp(v, digits = 0) {
  // Some fields are stored as log(value). If v looks invalid, return blank.
  if (!Number.isFinite(v)) return "";
  const n = Math.exp(v);
  if (!Number.isFinite(n)) return "";
  return formatNumber(n, digits);
}

function tooltipHtmlForDatum(d, idx) {
  const trader = Array.isArray(d.trader) ? (d.trader[idx] ?? d.trader[0]) : d.trader;
  const category = getCategoryValue(d, idx);

  const winRate = d.win_rate?.[idx];
  const totalTrades = formatMaybeExp(d.total_number?.[idx], 0);
  const avgTradeSize = formatMaybeExp(d.avg_size?.[idx], 2);

  const netGains = d.net_gain?.[idx];
  const profitPerTrade = d.profit_per_trade?.[idx];

  return `
      <div class="mono"><b>${trader}</b></div>
      <div>Category: <b>${category || ""}</b></div>
      <div>Win rate: <b>${Number.isFinite(winRate) ? formatNumber(winRate, 4) : ""}</b></div>
      <div>Total trades: <b>${totalTrades}</b></div>
      <div>Avg trade size: <b>${avgTradeSize}</b></div>
      <div>Net gains/loss: <b>${Number.isFinite(netGains) ? formatNumber(netGains, 4) : ""}</b></div>
      <div>Profit / trade: <b>${Number.isFinite(profitPerTrade) ? formatNumber(profitPerTrade, 4) : ""}</b></div>
  `;
}

function nearestPoint(mx, my) {
  // Brute-force is fine for 50k with throttling.
  let best = null;
  let bestDist2 = Infinity;
  for (const p of points) {
    if (!p) continue;
    const dx = mx - p.x;
    const dy = my - p.y;
    const dist2 = dx * dx + dy * dy;

    // More forgiving radius so it feels consistent even for small points.
    // Important: mouse coordinates and stored points are both in CSS pixels
    // (we scale the canvas with DPR but draw in CSS px after setTransform).
    const hitR = Math.max(8, p.r + 6);

    if (dist2 <= hitR * hitR && dist2 < bestDist2) {
      bestDist2 = dist2;
      best = p;
    }
  }
  return best;
}

let hoverRaf = null;
function onMove(evt) {
  if (hoverRaf) return;
  hoverRaf = requestAnimationFrame(() => {
    hoverRaf = null;

  // Use canvas-local coordinates for the actual hit-test.
  // This keeps the selection aligned with the cursor tip (and avoids any offset
  // introduced by layout/padding/inset).
  const mx = evt.offsetX;
  const my = evt.offsetY;

  // We still need plot-local coords for tooltip positioning.
  const rect = (els.plotWrap || els.viz).getBoundingClientRect();
    const p = nearestPoint(mx, my);

    if (!p) {
  const hadHover = !!hovered;
  clearHover();
  // Only redraw if we were previously hovering a point.
  if (hadHover) renderPoints();
      return;
    }

    // If the hovered point didn't actually change, don't force a repaint.
    if (hovered?.d !== p.d) {
      hovered = p;
      renderPoints();
    }

    const includeSales = els.includeSales.checked;
    const idx = includeSales ? 0 : 1;

    const d = p.d;
  els.tooltip.innerHTML = tooltipHtmlForDatum(d, idx);

  // Position tooltip near the hovered dot. If the dot is near the right edge,
  // flip the tooltip to the *left* so it doesn't hide behind the legend panel.
  // (Tooltip is absolutely positioned inside #plotWrap.)
  const wrap = els.plotWrap || els.viz;
  const w = Math.max(1, wrap.clientWidth || wrap.getBoundingClientRect().width);
  const h = Math.max(1, wrap.clientHeight || wrap.getBoundingClientRect().height);

  // Measure after setting innerHTML so we can clamp correctly.
  els.tooltip.style.display = "block";
  const tipW = els.tooltip.offsetWidth || 280;
  const tipH = els.tooltip.offsetHeight || 120;

  const pad = 8;
  const dx = 10;
  const dy = 10;

  // Use point coordinates if available; fall back to mouse.
  const px = Number.isFinite(p.x) ? p.x : mx;
  const py = Number.isFinite(p.y) ? p.y : my;

  // If the tooltip would run off the right edge, flip to left.
  const wouldOverflowRight = px + dx + tipW + pad > w;
  let left = wouldOverflowRight ? px - dx - tipW : px + dx;
  let top = py + dy;

  // Clamp within plot wrap bounds.
  left = Math.max(pad, Math.min(w - tipW - pad, left));
  top = Math.max(pad, Math.min(h - tipH - pad, top));

  els.tooltip.style.left = `${left}px`;
  els.tooltip.style.top = `${top}px`;
  });
}

function traderProfileUrl(traderId) {
  if (!traderId) return null;
  return `https://polymarket.com/profile/${encodeURIComponent(String(traderId))}`;
}

function onClick(evt) {
  // Open the trader profile for the clicked point.
  const mx = evt.offsetX;
  const my = evt.offsetY;
  const p = nearestPoint(mx, my);
  if (!p || !p.d) return;

  const includeSales = els.includeSales.checked;
  const idx = includeSales ? 0 : 1;
  const d = p.d;
  const traderId = Array.isArray(d.trader) ? (d.trader[idx] ?? d.trader[0]) : d.trader;
  const url = traderProfileUrl(traderId);
  if (!url) return;
  window.open(url, "_blank", "noopener,noreferrer");
}

async function loadData() {
  setStatus("Loading CSV…");

  // Streaming parse can be added later; d3.csv is ok for ~5MB.
  const df = await d3.csv(DATA_URL, parseRow);

  raw = df;
  // Always offer the full set of categories (even if the current dataset slice is skewed).
  categoriesAll = CATEGORY_ORDER.slice();
  populateCategoryDropdown();

  buildScales();
  renderAxes();
  renderPoints();
  renderLegend();
  renderClusterInfoPanel();
}

els.reload.addEventListener("click", () => {
  clearHover();
  buildScales();
  renderAxes();
  renderPoints();
  renderLegend();
  renderClusterInfoPanel();
});

if (els.clusterMethod) {
  els.clusterMethod.addEventListener("change", () => {
  clearHover();
    rerender();
  renderLegend();
  renderClusterInfoPanel();
  });
}

if (els.category) {
  els.category.addEventListener("change", () => {
  clearHover();
    rerender();
  });
}

if (els.xAxis) {
  els.xAxis.addEventListener("change", () => {
  clearHover();
    buildScales();
    renderAxes();
    renderPoints();
  renderLegend();
  });
}

if (els.yAxis) {
  els.yAxis.addEventListener("change", () => {
  clearHover();
    buildScales();
    renderAxes();
    renderPoints();
    renderLegend();
  });
}

if (els.includeSales) {
  els.includeSales.addEventListener("change", () => {
    // Re-render points/scales and also refresh tooltip if we're currently hovering a dot.
    // (Otherwise the tooltip could show values for the previous includeSales mode.)
    rerender();
    renderLegend();

    if (hovered && hovered.d) {
      const idx = els.includeSales.checked ? 0 : 1;
      els.tooltip.innerHTML = tooltipHtmlForDatum(hovered.d, idx);
      els.tooltip.style.display = "block";
    } else {
      clearHover();
    }
  });
}

window.addEventListener("resize", resize);
els.canvas.addEventListener("mousemove", onMove);
els.canvas.addEventListener("click", onClick);
els.canvas.addEventListener("mouseleave", () => {
  const hadHover = !!hovered;
  clearHover();
  if (hadHover) renderPoints();
});

resize();
loadData().catch((err) => {
  console.error(err);
  setStatus(`Error: ${err.message || err}`);
});