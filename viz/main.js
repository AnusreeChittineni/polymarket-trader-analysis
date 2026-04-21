/* global d3 */

// Canvas + D3 axes for large scatter.
// Each point: trader×category.

const DATA_URL = "../samples/clustered_traders.csv";

const clusterColor = d3.scaleOrdinal()
  .domain(d3.range(20))
  .range(
    d3.range(20).map(i => `hsl(${i * 18}, 70%, 55%)`)
  );

const els = {
  canvas: document.getElementById("plot"),
  svg: document.getElementById("axes"),
  tooltip: document.getElementById("tooltip"),
  status: document.getElementById("status"),
  xAxis: document.getElementById("xAxis"),
  yAxis: document.getElementById("yAxis"),
  maxPoints: document.getElementById("maxPoints"),
  minPositions: document.getElementById("minPositions"),
  category: document.getElementById("category"),
  pointAlpha: document.getElementById("pointAlpha"),
  reload: document.getElementById("reload"),
  viz: document.getElementById("viz"),
};

const DPR = Math.max(1, Math.min(3, window.devicePixelRatio || 1));

const margin = { top: 14, right: 18, bottom: 42, left: 56 };

let raw = [];
let categoriesAll = [];
let xScale, yScale;
let points = []; // points with screen coords for hit-testing

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
    "tsne_1",
    "tsne_2"
  ].includes(field);
}

function getValue(d, field) {
  if (field === "win_rate") return d.win_rate;
  if (field === "avg_trade_size") return d.avg_size;
  if (field === "total_trade_volume") return d.total_volume;
  if (field === "total_trade_number") return d.total_number;
  if (field === "frequency") return d.frequency;
  if (field === "net_gains") return d.net_gain;
  if (field === "avg_odds") return d.avg_odds;
  if (field === "tsne_1") return d.tsne_1;
  if (field === "tsne_2") return d.tsne_2;
  return null;
}

function setStatus(msg) {
  els.status.textContent = msg;
}

function resize() {
  const rect = els.viz.getBoundingClientRect();
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
  }
}

function parseRow(d) {
  return {
    trader: d.trader,
    win_rate: +d.win_rate,
    avg_size: +d.avg_trade_size,
    total_volume: +d.total_trade_volume,
    total_number: +d.total_trade_number,
    frequency: +d.frequency,
    net_gain: +d.net_gains_loss,
    avg_odds: +d.avg_odds,
    tsne_1: +d.tsne_1,
    tsne_2: +d.tsne_2,
    kmeans: +d.kmeans_cluster,
    dbscan: +d.hdbscan_cluster,
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

function filterAndDownsample() {
  const maxPoints = Math.max(1000, Math.min(200000, parseInt(els.maxPoints.value, 10) || 50000));
  const minPos = Math.max(1, parseInt(els.minPositions.value, 10) || 1);
  const category = els.category.value;

  const xField = getXAxisField();
  const yField = getYAxisField();

  let df = raw;
  // if (category) df = df.filter((d) => d.category === category);
  df = df.filter((d) => (Number.isFinite(getValue(d, xField)) && Number.isFinite(getValue(d, yField))));

  // Keep high-activity points first; if too many, sample the rest deterministically.
  df = df.slice().sort((a, b) => (b.total_number - a.total_number) || (b.total_volume - a.total_volume));
  
  if (df.length <= maxPoints) return df;

  const head = Math.floor(maxPoints * 0.7);
  const tail = maxPoints - head;
  const keep = df.slice(0, head);

  // Deterministic sample from the remainder using a seeded PRNG.
  const rest = df.slice(head);
  const sampled = sampleDeterministic(rest, tail, 42);
  return keep.concat(sampled);
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
  const rect = els.viz.getBoundingClientRect();
  const width = rect.width;
  const height = rect.height;
  const innerW = Math.max(10, width - margin.left - margin.right);
  const innerH = Math.max(10, height - margin.top - margin.bottom);

  const xField = getXAxisField();
  const yField = getYAxisField();

  if (isNumericField(xField)) {
    const xs = raw.map((d) => getValue(d, xField)).filter((v) => Number.isFinite(v));
    const xMin = d3.min(xs) ?? 0;
    const xMax = d3.max(xs) ?? 1;
    xScale = d3
      .scaleLinear()
      .domain([xMin, xMax])
      .nice()
      .range([margin.left, margin.left + innerW]);
  } else {
    // x is categorical. Use all categories so axis doesn't jump when filtering.
    xScale = d3
      .scalePoint()
      .domain(categoriesAll)
      .range([margin.left, margin.left + innerW])
      .padding(0.5);
  }

  if (isNumericField(yField)) {
    const ys = raw.map((d) => getValue(d, yField)).filter((v) => Number.isFinite(v));
    const yMin = d3.min(ys) ?? 0;
    const yMax = d3.max(ys) ?? 1;
    yScale = d3
      .scaleLinear()
      .domain([yMin, yMax])
      .nice()
      .range([margin.top + innerH, margin.top]);
  }
}

function renderAxes() {
  const svg = d3.select(els.svg);
  svg.selectAll("*").remove();

  const xField = getXAxisField();
  const yField = getYAxisField();

  const xAxis = isNumericField(xField) ? d3.axisBottom(xScale).ticks(10) : d3.axisBottom(xScale);
  const yAxis = d3.axisLeft(yScale).ticks(8);

  const rect = els.viz.getBoundingClientRect();
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
    .text(xField);

  svg
    .append("text")
    .attr("transform", "rotate(-90)")
    .attr("x", -height / 2)
    .attr("y", 15)
    .attr("text-anchor", "middle")
    .attr("fill", "#cbd5e1")
    .text(yField);
}

function renderPoints() {
  const ctx = els.canvas.getContext("2d");
  const rect = els.viz.getBoundingClientRect();
  const width = rect.width;
  const height = rect.height;

  const xField = getXAxisField();
  const yField = getYAxisField();

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

  // Build screen coords for hit testing.
  points = new Array(df.length);

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
  const vols = df.map((d) => (Number.isFinite(d.total_volume) ? d.total_volume : 0));
  const vMax = Math.max(1, ...vols);
  const rScale = (v) => {
    const t = Math.sqrt(Math.max(0, v) / vMax);
    return 2 + 6 * t;
  };

  // Render
  for (let i = 0; i < df.length; i++) {
    const d = df[i];
    const x = xScale(getValue(d, xField));
    const y = yScale(getValue(d, yField));
    if (!Number.isFinite(x) || !Number.isFinite(y)) continue;

    const r = rScale(d.total_volume);
    ctx.beginPath();
    ctx.fillStyle = clusterColor(d.kmeans);
    ctx.arc(x, y, r, 0, Math.PI * 2);
    ctx.fill();

    points[i] = { x, y, r, d };
  }

  setStatus(`Showing ${df.length.toLocaleString()} points`);
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
    const hitR = Math.max(4, p.r + 2);
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
    const rect = els.viz.getBoundingClientRect();
    const mx = evt.clientX - rect.left;
    const my = evt.clientY - rect.top;
    const p = nearestPoint(mx, my);

    if (!p) {
      els.tooltip.style.display = "none";
      return;
    }

    const { trader, win_rate, total_number, total_volume } = p.d;
    els.tooltip.innerHTML = `
      <div class="mono"><b>${trader}</b></div>
      <div>Win Rate: <b>${win_rate.toFixed(4)}</b></div>
      <div>Total Trade Number: <b>${Number.isFinite(total_number) ? Math.round(total_number).toLocaleString() : ""}</b></div>
      <div>Total Volume: <b>${Math.round(total_volume).toLocaleString()}</b></div>
    `;
    els.tooltip.style.left = `${mx}px`;
    els.tooltip.style.top = `${my}px`;
    els.tooltip.style.display = "block";
  });
}

async function loadData() {
  setStatus("Loading CSV…");

  // Streaming parse can be added later; d3.csv is ok for ~5MB.
  const df = await d3.csv(DATA_URL, parseRow);

  raw = df;
  // categoriesAll = Array.from(new Set(raw.map((d) => d.category))).sort();
  // populateCategoryDropdown();

  buildScales();
  renderAxes();
  renderPoints();
}

els.reload.addEventListener("click", () => {
  buildScales();
  renderAxes();
  renderPoints();
});

if (els.xAxis) {
  els.xAxis.addEventListener("change", () => {
    buildScales();
    renderAxes();
    renderPoints();
  });
}

window.addEventListener("resize", resize);
els.canvas.addEventListener("mousemove", onMove);
els.canvas.addEventListener("mouseleave", () => {
  els.tooltip.style.display = "none";
});

resize();
loadData().catch((err) => {
  console.error(err);
  setStatus(`Error: ${err.message || err}`);
});