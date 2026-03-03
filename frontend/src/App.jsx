import { useState, useEffect, useRef } from "react";

// ── Colour helpers ────────────────────────────────────────────────────────────
const scoreColor = (s) => s === 100 ? "#16a34a" : s >= 70 ? "#d97706" : "#dc2626";
const statusBg   = (s) => s === "PASS" ? "#dcfce7" : s === "WARNING" ? "#fef9c3" : "#fee2e2";
const statusFg   = (s) => s === "PASS" ? "#16a34a" : s === "WARNING" ? "#b45309" : "#dc2626";
const sevColor   = (s) => s === "HIGH" ? "#dc2626" : s === "MEDIUM" ? "#d97706" : "#2563eb";
const sevBg      = (s) => s === "HIGH" ? "#fee2e2" : s === "MEDIUM" ? "#fef9c3" : "#dbeafe";

// ── Nav items ─────────────────────────────────────────────────────────────────
const NAV = [
  { id: "overview",        label: "Overview",          icon: "⊞" },
  { id: "rules",           label: "Rule Validation",   icon: "✓" },
  { id: "recommendations", label: "Recommendations",   icon: "💡" },
  { id: "eda",             label: "EDA & Charts",      icon: "📊" },
  { id: "columns",         label: "Column Explorer",   icon: "⊟" },
  { id: "parse",           label: "Parse Report",      icon: "⚙" },
];

// ═════════════════════════════════════════════════════════════════════════════
export default function App() {
  const [file,    setFile]    = useState(null);
  const [data,    setData]    = useState(null);
  const [loading, setLoading] = useState(false);
  const [error,   setError]   = useState(null);
  const [active,  setActive]  = useState("overview");
  const [dragging, setDragging] = useState(false);

  const analyze = async () => {
    if (!file) { setError("Please upload a file first."); return; }
    setLoading(true); setError(null); setData(null);
    try {
      const fd = new FormData();
      fd.append("file", file);
      const res = await fetch("http://127.0.0.1:8000/analyze", { method: "POST", body: fd });
      if (!res.ok) {
        const err = await res.json().catch(() => ({}));
        throw new Error(err.detail || "Backend error");
      }
      setData(await res.json());
      setActive("overview");
    } catch (e) { setError(e.message); }
    finally { setLoading(false); }
  };

  const onDrop = (e) => {
    e.preventDefault(); setDragging(false);
    const f = e.dataTransfer.files[0];
    if (f) setFile(f);
  };

  return (
    <div style={css.app}>
      {/* ── Sidebar ── */}
      <aside style={css.sidebar}>
        <div style={css.logo}>
          <span style={css.logoIcon}>◈</span>
          <div>
            <div style={css.logoTitle}>DQ Engine</div>
            <div style={css.logoSub}>Data Quality Platform</div>
          </div>
        </div>

        {data && (
          <nav style={{ marginTop: 8 }}>
            {NAV.map(n => (
              <div key={n.id}
                style={{ ...css.navItem, ...(active === n.id ? css.navActive : {}) }}
                onClick={() => setActive(n.id)}>
                <span style={{ marginRight: 10, fontSize: 15 }}>{n.icon}</span>
                {n.label}
              </div>
            ))}
          </nav>
        )}

        {/* File upload in sidebar */}
        <div style={{ marginTop: "auto", padding: "16px 0" }}>
          <div
            style={{ ...css.dropzone, ...(dragging ? css.dropzoneActive : {}) }}
            onDragOver={e => { e.preventDefault(); setDragging(true); }}
            onDragLeave={() => setDragging(false)}
            onDrop={onDrop}
            onClick={() => document.getElementById("fileInput").click()}
          >
            <input id="fileInput" type="file" accept=".csv,.xlsx,.xls,.json,.log,.txt"
              style={{ display: "none" }}
              onChange={e => setFile(e.target.files[0])} />
            <div style={{ fontSize: 24, marginBottom: 6 }}>📂</div>
            <div style={css.dropText}>
              {file ? file.name : "Drop file or click"}
            </div>
            <div style={css.dropSub}>CSV · Excel · JSON · Log</div>
          </div>

          {error && <div style={css.errorBox}>{error}</div>}

          <button style={{ ...css.btn, ...(loading ? css.btnDisabled : {}) }}
            onClick={analyze} disabled={loading}>
            {loading
              ? <><Spinner /> Analyzing…</>
              : "▶  Run Analysis"}
          </button>
        </div>
      </aside>

      {/* ── Main ── */}
      <main style={css.main}>
        {!data && !loading && <LandingHero />}
        {loading && <LoadingScreen />}
        {data && (
          <>
            {active === "overview"        && <Overview        data={data} />}
            {active === "rules"           && <RuleValidation  data={data} />}
            {active === "recommendations" && <Recommendations data={data} />}
            {active === "eda"             && <EDACharts       data={data} />}
            {active === "columns"         && <ColumnExplorer  data={data} />}
            {active === "parse"           && <ParseReport     data={data} />}
          </>
        )}
      </main>
    </div>
  );
}

// ═════════════════════════════════════════════════════════════════════════════
// Landing hero
// ═════════════════════════════════════════════════════════════════════════════
function LandingHero() {
  return (
    <div style={css.hero}>
      <div style={css.heroIcon}>◈</div>
      <h1 style={css.heroTitle}>Data Quality Engine</h1>
      <p style={css.heroSub}>
        Upload any dataset — CSV, Excel, JSON or Log file — and get an instant,
        comprehensive data quality report with EDA insights and actionable recommendations.
      </p>
      <div style={css.heroFeatures}>
        {["Auto format detection","Weighted rule validation","EDA & visualisations",
          "Actionable recommendations","Data freshness check","SQL database support"].map(f => (
          <div key={f} style={css.heroFeature}>✓ {f}</div>
        ))}
      </div>
    </div>
  );
}

// ═════════════════════════════════════════════════════════════════════════════
// Overview
// ═════════════════════════════════════════════════════════════════════════════
function Overview({ data }) {
  const score  = data.quality_score?.score  ?? 0;
  const status = data.quality_score?.status ?? "UNKNOWN";
  const failed = (data.failed_rules ?? []).length;
  const recs   = (data.recommendations ?? []).length;
  const high   = (data.recommendations ?? []).filter(r => r.severity === "HIGH").length;

  return (
    <div style={css.page}>
      <PageHeader title="Overview" sub="Dataset quality summary and key metrics" />

      {/* Summary banner */}
      <div style={{ ...css.banner, background: statusBg(status), borderColor: statusFg(status) }}>
        <span style={{ color: statusFg(status), fontWeight: 700, fontSize: 15 }}>
          {status === "PASS" ? "✓" : status === "WARNING" ? "⚠" : "✕"} {status}
        </span>
        <span style={{ color: "#374151", marginLeft: 16, fontSize: 14 }}>
          {data.summary}
        </span>
      </div>

      {/* Score + stat cards */}
      <div style={css.row}>
        <ScoreGauge score={score} status={status} />

        <div style={{ flex: 1, display: "grid", gridTemplateColumns: "1fr 1fr", gap: 16 }}>
          <StatCard label="Total Rows"    value={data.rows?.toLocaleString()} icon="⊟" color="#2563eb" />
          <StatCard label="Columns"       value={data.column_count}           icon="⊞" color="#7c3aed" />
          <StatCard label="Failed Rules"  value={failed}  icon="✕" color={failed  ? "#dc2626" : "#16a34a"} />
          <StatCard label="Critical Issues" value={high}  icon="⚠" color={high    ? "#dc2626" : "#16a34a"} />
        </div>
      </div>

      {/* Missing values bar chart */}
      <Card title="Missing Values by Column" sub="Columns with missing data — hover for details">
        <MissingBarChart data={data} />
      </Card>

      {/* Rule summary mini */}
      <Card title="Rule Results Summary">
        <RuleSummaryBars data={data} />
      </Card>
    </div>
  );
}

// ═════════════════════════════════════════════════════════════════════════════
// Score gauge (SVG circle)
// ═════════════════════════════════════════════════════════════════════════════
function ScoreGauge({ score, status }) {
  const r   = 70;
  const circ = 2 * Math.PI * r;
  const pct  = score / 100;
  const [anim, setAnim] = useState(0);

  useEffect(() => {
    const t = setTimeout(() => setAnim(pct), 100);
    return () => clearTimeout(t);
  }, [pct]);

  return (
    <div style={css.gaugeWrap}>
      <svg width={180} height={180} viewBox="0 0 180 180">
        <circle cx={90} cy={90} r={r} fill="none" stroke="#e5e7eb" strokeWidth={14} />
        <circle cx={90} cy={90} r={r} fill="none"
          stroke={scoreColor(score)} strokeWidth={14}
          strokeDasharray={circ}
          strokeDashoffset={circ * (1 - anim)}
          strokeLinecap="round"
          transform="rotate(-90 90 90)"
          style={{ transition: "stroke-dashoffset 1.2s ease" }} />
        <text x={90} y={84} textAnchor="middle" fontSize={28} fontWeight={700}
          fill={scoreColor(score)}>{score.toFixed(0)}</text>
        <text x={90} y={104} textAnchor="middle" fontSize={12} fill="#6b7280">/ 100</text>
        <text x={90} y={122} textAnchor="middle" fontSize={11} fontWeight={600}
          fill={statusFg(status)}>{status}</text>
      </svg>
      <div style={{ textAlign: "center", color: "#6b7280", fontSize: 13, marginTop: 4 }}>
        Quality Score
      </div>
    </div>
  );
}

// ═════════════════════════════════════════════════════════════════════════════
// Missing bar chart
// ═════════════════════════════════════════════════════════════════════════════
function MissingBarChart({ data }) {
  const cols = Object.entries(data.quality_score?.details
    ? {} : {})
  ;

  // Build from column_issues
  const issues = data.column_issues ?? [];
  const colData = Object.entries(
    (data.eda?.missing_heatmap?.missing_columns ?? []).reduce((acc, c) => {
      acc[c.column] = c.missing_pct;
      return acc;
    }, {})
  );

  if (colData.length === 0) {
    return <div style={{ color: "#16a34a", padding: "12px 0" }}>✓ No missing values detected in any column.</div>;
  }

  const maxPct = Math.max(...colData.map(([, v]) => v), 1);

  return (
    <div style={{ display: "flex", flexDirection: "column", gap: 10 }}>
      {colData.map(([col, pct]) => (
        <div key={col} style={{ display: "flex", alignItems: "center", gap: 12 }}>
          <div style={{ width: 120, fontSize: 13, color: "#374151", textAlign: "right",
            whiteSpace: "nowrap", overflow: "hidden", textOverflow: "ellipsis" }}
            title={col}>{col}</div>
          <div style={{ flex: 1, background: "#f3f4f6", borderRadius: 4, height: 22, position: "relative" }}>
            <div style={{
              width: `${(pct / maxPct) * 100}%`,
              height: "100%", borderRadius: 4,
              background: pct > 50 ? "#dc2626" : pct > 20 ? "#d97706" : "#2563eb",
              transition: "width 0.8s ease"
            }} />
          </div>
          <div style={{ width: 48, fontSize: 13, color: "#6b7280", textAlign: "right" }}>
            {pct.toFixed(1)}%
          </div>
        </div>
      ))}
    </div>
  );
}

// ═════════════════════════════════════════════════════════════════════════════
// Rule summary bars
// ═════════════════════════════════════════════════════════════════════════════
function RuleSummaryBars({ data }) {
  const details = data.quality_score?.details ?? [];
  const passed  = details.filter(r => r.passed).length;
  const failed  = details.filter(r => !r.passed).length;
  const total   = details.length;

  return (
    <div>
      <div style={{ display: "flex", gap: 24, marginBottom: 16 }}>
        <div style={{ fontSize: 28, fontWeight: 700, color: "#16a34a" }}>{passed}</div>
        <div style={{ fontSize: 28, fontWeight: 700, color: "#dc2626" }}>{failed}</div>
        <div style={{ fontSize: 28, fontWeight: 700, color: "#6b7280" }}>{total}</div>
      </div>
      <div style={{ display: "flex", gap: 8, fontSize: 12, color: "#6b7280", marginBottom: 16 }}>
        <span>✓ Passed</span><span style={{ marginLeft: 16 }}>✕ Failed</span><span style={{ marginLeft: 16 }}>Total Rules</span>
      </div>
      <div style={{ height: 12, borderRadius: 6, background: "#e5e7eb", overflow: "hidden" }}>
        <div style={{
          width: `${total ? (passed / total) * 100 : 0}%`,
          height: "100%", background: "#16a34a",
          transition: "width 1s ease"
        }} />
      </div>
    </div>
  );
}

// ═════════════════════════════════════════════════════════════════════════════
// Rule Validation
// ═════════════════════════════════════════════════════════════════════════════
function RuleValidation({ data }) {
  const details = data.quality_score?.details ?? [];
  const [filter, setFilter] = useState("all");

  const filtered = details.filter(r =>
    filter === "all" ? true : filter === "passed" ? r.passed : !r.passed
  );

  return (
    <div style={css.page}>
      <PageHeader title="Rule Validation" sub="Detailed results for all validation rules" />

      <div style={{ display: "flex", gap: 10, marginBottom: 20 }}>
        {["all", "passed", "failed"].map(f => (
          <button key={f} style={{ ...css.filterBtn, ...(filter === f ? css.filterActive : {}) }}
            onClick={() => setFilter(f)}>
            {f.charAt(0).toUpperCase() + f.slice(1)}
            <span style={css.badge}>{
              f === "all" ? details.length :
              f === "passed" ? details.filter(r => r.passed).length :
              details.filter(r => !r.passed).length
            }</span>
          </button>
        ))}
      </div>

      <Card>
        <table style={css.table}>
          <thead>
            <tr style={css.thead}>
              <th style={css.th}>Status</th>
              <th style={css.th}>Rule</th>
              <th style={css.th}>Message</th>
              <th style={css.th}>Weight</th>
            </tr>
          </thead>
          <tbody>
            {filtered.map((r, i) => (
              <tr key={i} style={{ ...css.tr, background: i % 2 === 0 ? "#fff" : "#f9fafb" }}>
                <td style={css.td}>
                  <span style={{
                    ...css.pill,
                    background: r.passed ? "#dcfce7" : "#fee2e2",
                    color: r.passed ? "#16a34a" : "#dc2626"
                  }}>
                    {r.passed ? "✓ PASS" : "✕ FAIL"}
                  </span>
                </td>
                <td style={{ ...css.td, fontFamily: "monospace", fontSize: 13 }}>{r.rule}</td>
                <td style={css.td}>{r.message}</td>
                <td style={{ ...css.td, textAlign: "center" }}>{r.weight}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </Card>
    </div>
  );
}

// ═════════════════════════════════════════════════════════════════════════════
// Recommendations
// ═════════════════════════════════════════════════════════════════════════════
function Recommendations({ data }) {
  const recs = data.recommendations ?? [];
  const [filter, setFilter] = useState("ALL");
  const sevs = ["ALL", "HIGH", "MEDIUM", "LOW"];

  const filtered = recs.filter(r => filter === "ALL" || r.severity === filter);

  return (
    <div style={css.page}>
      <PageHeader title="Recommendations"
        sub="Actionable fixes for detected data quality issues" />

      {recs.length === 0
        ? <div style={css.emptyState}>✓ No issues detected — dataset meets all quality standards.</div>
        : <>
          <div style={{ display: "flex", gap: 10, marginBottom: 20 }}>
            {sevs.map(s => (
              <button key={s}
                style={{ ...css.filterBtn, ...(filter === s ? { ...css.filterActive, background: s === "ALL" ? "#2563eb" : sevBg(s), color: s === "ALL" ? "#fff" : sevColor(s) } : {}) }}
                onClick={() => setFilter(s)}>
                {s}
                <span style={css.badge}>
                  {s === "ALL" ? recs.length : recs.filter(r => r.severity === s).length}
                </span>
              </button>
            ))}
          </div>

          <div style={{ display: "flex", flexDirection: "column", gap: 14 }}>
            {filtered.map((r, i) => (
              <div key={i} style={{ ...css.recCard, borderLeftColor: sevColor(r.severity) }}>
                <div style={css.recHeader}>
                  <span style={{ ...css.pill, background: sevBg(r.severity), color: sevColor(r.severity) }}>
                    {r.severity}
                  </span>
                  <span style={css.recCol}>{r.column}</span>
                  <span style={css.recIssue}>{r.issue}</span>
                </div>
                <div style={css.recImpact}>
                  <strong>Business Impact:</strong> {r.business_impact}
                </div>
                <div style={css.recAction}>
                  <strong>Recommendation:</strong> {r.recommendation}
                </div>
                <div style={css.recTag}>{r.action}</div>
              </div>
            ))}
          </div>
        </>
      }
    </div>
  );
}

// ═════════════════════════════════════════════════════════════════════════════
// EDA Charts
// ═════════════════════════════════════════════════════════════════════════════
function EDACharts({ data }) {
  const eda = data.eda ?? {};
  const insights = eda.insights ?? [];
  const numeric  = eda.numeric_summary ?? {};
  const outliers = eda.outlier_analysis ?? {};
  const corr     = eda.correlation_matrix ?? {};
  const dists    = eda.distributions ?? {};
  const cats     = eda.categorical_summary ?? {};

  return (
    <div style={css.page}>
      <PageHeader title="EDA & Charts" sub="Exploratory data analysis and visual insights" />

      {/* Auto insights */}
      {insights.length > 0 && (
        <Card title="Auto-Generated Insights" sub="Patterns detected automatically by the engine">
          <div style={{ display: "flex", flexDirection: "column", gap: 10 }}>
            {insights.map((ins, i) => (
              <div key={i} style={css.insightRow}>
                <span style={css.insightIcon}>
                  {ins.type === "correlation" ? "⟷" : ins.type === "distribution" ? "∿" : ins.type === "missing" ? "⚠" : "◎"}
                </span>
                <span style={{ fontSize: 14, color: "#374151" }}>{ins.insight}</span>
              </div>
            ))}
          </div>
        </Card>
      )}

      {/* Numeric summary table */}
      {Object.keys(numeric).length > 0 && (
        <Card title="Numeric Column Statistics">
          <div style={{ overflowX: "auto" }}>
            <table style={css.table}>
              <thead>
                <tr style={css.thead}>
                  {["Column","Count","Mean","Median","Std Dev","Min","Max","Skewness"].map(h => (
                    <th key={h} style={css.th}>{h}</th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {Object.entries(numeric).map(([col, s], i) => (
                  <tr key={col} style={{ background: i % 2 === 0 ? "#fff" : "#f9fafb" }}>
                    <td style={{ ...css.td, fontWeight: 600 }}>{col}</td>
                    <td style={css.td}>{s.count?.toLocaleString()}</td>
                    <td style={css.td}>{s.mean?.toFixed(2)}</td>
                    <td style={css.td}>{s.median?.toFixed(2)}</td>
                    <td style={css.td}>{s.std?.toFixed(2)}</td>
                    <td style={css.td}>{s.min?.toFixed(2)}</td>
                    <td style={css.td}>{s.max?.toFixed(2)}</td>
                    <td style={{ ...css.td, color: Math.abs(s.skewness) > 1 ? "#d97706" : "#374151" }}>
                      {s.skewness?.toFixed(2)}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </Card>
      )}

      {/* Distribution charts */}
      {Object.keys(dists).length > 0 && (
        <Card title="Value Distributions" sub="Histogram of numeric columns">
          <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fill, minmax(300px, 1fr))", gap: 20 }}>
            {Object.entries(dists).map(([col, dist]) => (
              <HistogramChart key={col} col={col} dist={dist} />
            ))}
          </div>
        </Card>
      )}

      {/* Outlier analysis */}
      {Object.keys(outliers).length > 0 && (
        <Card title="Outlier Analysis" sub="IQR-based outlier detection per numeric column">
          <div style={{ display: "flex", flexDirection: "column", gap: 12 }}>
            {Object.entries(outliers).map(([col, o]) => (
              <div key={col} style={css.outlierRow}>
                <div style={{ fontWeight: 600, fontSize: 14, width: 140 }}>{col}</div>
                <div style={css.outlierBadge}>{o.count} outliers ({o.pct}%)</div>
                <div style={{ fontSize: 13, color: "#6b7280" }}>
                  Bounds: [{o.lower_bound} → {o.upper_bound}]
                  &nbsp;· Range: [{o.min_outlier} → {o.max_outlier}]
                </div>
              </div>
            ))}
          </div>
        </Card>
      )}

      {/* Correlation matrix */}
      {corr.strong_correlations?.length > 0 && (
        <Card title="Strong Correlations" sub="Column pairs with |r| > 0.7">
          {corr.strong_correlations.map((c, i) => (
            <div key={i} style={css.corrRow}>
              <span style={css.corrCol}>{c.col1}</span>
              <span style={{ color: "#6b7280", margin: "0 8px" }}>⟷</span>
              <span style={css.corrCol}>{c.col2}</span>
              <span style={{ ...css.pill, marginLeft: "auto",
                background: c.correlation > 0 ? "#dcfce7" : "#fee2e2",
                color: c.correlation > 0 ? "#16a34a" : "#dc2626" }}>
                r = {c.correlation}
              </span>
              <span style={{ fontSize: 12, color: "#6b7280", marginLeft: 8 }}>
                {c.strength} {c.type}
              </span>
            </div>
          ))}
        </Card>
      )}

      {/* Categorical summary */}
      {Object.keys(cats).length > 0 && (
        <Card title="Categorical Columns" sub="Top values and frequency distribution">
          <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fill, minmax(280px, 1fr))", gap: 16 }}>
            {Object.entries(cats).map(([col, c]) => (
              <div key={col} style={css.catCard}>
                <div style={css.catTitle}>{col}</div>
                <div style={{ fontSize: 12, color: "#6b7280", marginBottom: 8 }}>
                  {c.unique_count} unique values
                </div>
                {c.top_values?.slice(0, 5).map((v, i) => (
                  <div key={i} style={css.catRow}>
                    <span style={{ fontSize: 13, color: "#374151", flex: 1 }}>{v.value}</span>
                    <div style={{ width: 80, height: 8, background: "#e5e7eb", borderRadius: 4 }}>
                      <div style={{ width: `${v.pct}%`, height: "100%", background: "#2563eb", borderRadius: 4 }} />
                    </div>
                    <span style={{ fontSize: 12, color: "#6b7280", width: 40, textAlign: "right" }}>{v.pct}%</span>
                  </div>
                ))}
              </div>
            ))}
          </div>
        </Card>
      )}
    </div>
  );
}

// Mini histogram
function HistogramChart({ col, dist }) {
  const max = Math.max(...dist.counts, 1);
  return (
    <div style={css.histWrap}>
      <div style={{ fontWeight: 600, fontSize: 13, marginBottom: 8 }}>{col}</div>
      <div style={{ fontSize: 11, color: "#6b7280", marginBottom: 6 }}>
        Distribution: {dist.type?.replace("_", " ")}
      </div>
      <div style={{ display: "flex", alignItems: "flex-end", gap: 2, height: 80 }}>
        {dist.counts.map((c, i) => (
          <div key={i} title={`${dist.bins[i]?.toFixed(1)}: ${c}`}
            style={{
              flex: 1, height: `${(c / max) * 100}%`,
              background: "#2563eb", borderRadius: "2px 2px 0 0", minHeight: 2,
              opacity: 0.7 + (c / max) * 0.3
            }} />
        ))}
      </div>
      <div style={{ display: "flex", justifyContent: "space-between", fontSize: 10, color: "#9ca3af", marginTop: 4 }}>
        <span>{dist.bins[0]?.toFixed(1)}</span>
        <span>{dist.bins[Math.floor(dist.bins.length / 2)]?.toFixed(1)}</span>
        <span>{dist.bins[dist.bins.length - 1]?.toFixed(1)}</span>
      </div>
    </div>
  );
}

// ═════════════════════════════════════════════════════════════════════════════
// Column Explorer
// ═════════════════════════════════════════════════════════════════════════════
function ColumnExplorer({ data }) {
  const [selected, setSelected] = useState(null);
  const cols = data.columns ?? [];
  const eda  = data.eda ?? {};

  const getColData = (col) => {
    const num = eda.numeric_summary?.[col];
    const cat = eda.categorical_summary?.[col];
    const out = eda.outlier_analysis?.[col];
    return { num, cat, out };
  };

  return (
    <div style={css.page}>
      <PageHeader title="Column Explorer" sub="Click any column to inspect its profile" />
      <div style={{ display: "flex", gap: 20 }}>
        {/* Column list */}
        <div style={css.colList}>
          {cols.map(col => {
            const hasIssue = (data.column_issues ?? []).some(i => i.column === col);
            return (
              <div key={col}
                style={{ ...css.colItem, ...(selected === col ? css.colItemActive : {}) }}
                onClick={() => setSelected(col)}>
                <span style={{ flex: 1, fontSize: 13 }}>{col}</span>
                {hasIssue && <span style={{ color: "#d97706", fontSize: 11 }}>⚠</span>}
              </div>
            );
          })}
        </div>

        {/* Column detail */}
        <div style={{ flex: 1 }}>
          {!selected
            ? <div style={css.emptyState}>← Select a column to inspect</div>
            : (() => {
              const { num, cat, out } = getColData(selected);
              const issue = (data.column_issues ?? []).find(i => i.column === selected);
              return (
                <div style={{ display: "flex", flexDirection: "column", gap: 14 }}>
                  <Card title={selected} sub="Column profile">
                    {issue && (
                      <div style={{ ...css.banner, background: "#fef9c3", borderColor: "#d97706", marginBottom: 14 }}>
                        <span style={{ color: "#b45309" }}>⚠ {issue.issue}</span>
                      </div>
                    )}
                    {num && (
                      <div style={css.statGrid}>
                        {[
                          ["Mean",   num.mean?.toFixed(4)],
                          ["Median", num.median?.toFixed(4)],
                          ["Std Dev",num.std?.toFixed(4)],
                          ["Min",    num.min?.toFixed(4)],
                          ["Max",    num.max?.toFixed(4)],
                          ["Q1",     num.q1?.toFixed(4)],
                          ["Q3",     num.q3?.toFixed(4)],
                          ["IQR",    num.iqr?.toFixed(4)],
                          ["Skewness",num.skewness?.toFixed(4)],
                          ["Kurtosis",num.kurtosis?.toFixed(4)],
                        ].map(([k, v]) => (
                          <div key={k} style={css.statCell}>
                            <div style={css.statLabel}>{k}</div>
                            <div style={css.statValue}>{v ?? "—"}</div>
                          </div>
                        ))}
                      </div>
                    )}
                    {cat && (
                      <div>
                        <div style={{ fontSize: 13, color: "#6b7280", marginBottom: 10 }}>
                          {cat.unique_count} unique values · Entropy: {cat.entropy?.toFixed(3)}
                        </div>
                        {cat.top_values?.map((v, i) => (
                          <div key={i} style={css.catRow}>
                            <span style={{ fontSize: 13, flex: 1 }}>{v.value}</span>
                            <span style={{ fontSize: 12, color: "#6b7280" }}>{v.count} ({v.pct}%)</span>
                          </div>
                        ))}
                      </div>
                    )}
                    {out && (
                      <div style={{ marginTop: 12, padding: 12, background: "#fef9c3", borderRadius: 6 }}>
                        <div style={{ fontWeight: 600, color: "#b45309", marginBottom: 4 }}>
                          ⚠ {out.count} Outliers ({out.pct}%)
                        </div>
                        <div style={{ fontSize: 13, color: "#6b7280" }}>
                          Bounds: [{out.lower_bound} → {out.upper_bound}]
                        </div>
                      </div>
                    )}
                    {!num && !cat && (
                      <div style={{ color: "#6b7280", fontSize: 14 }}>No detailed profile available for this column.</div>
                    )}
                  </Card>
                </div>
              );
            })()
          }
        </div>
      </div>
    </div>
  );
}

// ═════════════════════════════════════════════════════════════════════════════
// Parse Report
// ═════════════════════════════════════════════════════════════════════════════
function ParseReport({ data }) {
  const pr = data.parse_report ?? {};
  const freshness = pr.freshness;

  return (
    <div style={css.page}>
      <PageHeader title="Parse Report" sub="How the smart loader ingested and processed your file" />

      <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 16, marginBottom: 20 }}>
        {[
          ["File Type",      pr.file_type],
          ["Log Format",     pr.log_format ?? "N/A"],
          ["Encoding",       `${pr.encoding} (${Math.round((pr.encoding_confidence ?? 0) * 100)}% confidence)`],
          ["Delimiter",      pr.delimiter ?? "N/A"],
          ["Headers",        pr.headers_detected ? "Detected" : "Auto-generated"],
          ["Load Strategy",  pr.load_strategy?.replace("_", " ")],
          ["Rows Loaded",    pr.rows_loaded?.toLocaleString()],
          ["Sampled Rows",   pr.rows_sampled?.toLocaleString() ?? "Full load"],
          ["Encoding Errors",pr.encoding_error_rows || "None"],
          ["Parse Confidence", pr.parse_confidence ?? "N/A"],
          ["Fallback Used",  pr.fallback_used ? "Yes" : "No"],
          ["File Size",      pr.file_size_mb ? `${pr.file_size_mb} MB` : "N/A"],
        ].map(([k, v]) => (
          <div key={k} style={css.parseCell}>
            <div style={css.parseLbl}>{k}</div>
            <div style={css.parseVal}>{v ?? "—"}</div>
          </div>
        ))}
      </div>

      {freshness && (
        <Card title="Data Freshness">
          <div style={{ display: "flex", gap: 20, alignItems: "center" }}>
            <div style={{
              ...css.pill,
              background: freshness.status === "FRESH" ? "#dcfce7" : freshness.status === "STALE" ? "#fef9c3" : "#fee2e2",
              color: freshness.status === "FRESH" ? "#16a34a" : freshness.status === "STALE" ? "#b45309" : "#dc2626",
              fontSize: 14, padding: "6px 14px"
            }}>
              {freshness.status}
            </div>
            <div>
              <div style={{ fontSize: 14, color: "#374151" }}>
                Most recent record: <strong>{freshness.most_recent_record}</strong>
              </div>
              <div style={{ fontSize: 13, color: "#6b7280", marginTop: 2 }}>
                {freshness.age_days} days old · Column: {freshness.column}
              </div>
            </div>
          </div>
        </Card>
      )}

      {pr.warnings?.length > 0 && (
        <Card title="Warnings">
          {pr.warnings.map((w, i) => (
            <div key={i} style={css.warnRow}>
              <span style={{ color: "#d97706", marginRight: 8 }}>⚠</span>
              <span style={{ fontSize: 13, color: "#374151" }}>{w}</span>
            </div>
          ))}
        </Card>
      )}

      {pr.corrupted_row_indices?.length > 0 && (
        <Card title="Corrupted Row Indices (sample)">
          <div style={{ fontSize: 13, color: "#6b7280", fontFamily: "monospace" }}>
            {pr.corrupted_row_indices.join(", ")}
          </div>
        </Card>
      )}
    </div>
  );
}

// ═════════════════════════════════════════════════════════════════════════════
// Shared components
// ═════════════════════════════════════════════════════════════════════════════
function Card({ title, sub, children }) {
  return (
    <div style={css.card}>
      {title && <div style={css.cardTitle}>{title}</div>}
      {sub   && <div style={css.cardSub}>{sub}</div>}
      <div style={{ marginTop: title ? 16 : 0 }}>{children}</div>
    </div>
  );
}

function StatCard({ label, value, icon, color }) {
  return (
    <div style={css.statCard}>
      <div style={{ ...css.statIcon, background: color + "18", color }}>{icon}</div>
      <div style={css.statNum}>{value ?? "—"}</div>
      <div style={css.statLbl}>{label}</div>
    </div>
  );
}

function PageHeader({ title, sub }) {
  return (
    <div style={css.pageHeader}>
      <h2 style={css.pageTitle}>{title}</h2>
      {sub && <p style={css.pageSub}>{sub}</p>}
    </div>
  );
}

function LoadingScreen() {
  return (
    <div style={css.loading}>
      <div style={css.loadingSpinner} />
      <div style={{ fontSize: 18, fontWeight: 600, color: "#1f2937", marginTop: 20 }}>
        Analyzing your dataset…
      </div>
      <div style={{ fontSize: 14, color: "#6b7280", marginTop: 8 }}>
        Profiling columns · Evaluating rules · Running EDA
      </div>
    </div>
  );
}

function Spinner() {
  return <span style={{ display: "inline-block", width: 14, height: 14,
    border: "2px solid #fff", borderTopColor: "transparent",
    borderRadius: "50%", animation: "spin 0.8s linear infinite",
    marginRight: 8, verticalAlign: "middle" }} />;
}

// ═════════════════════════════════════════════════════════════════════════════
// Styles
// ═════════════════════════════════════════════════════════════════════════════
const css = {
  app:       { display: "flex", minHeight: "100vh", fontFamily: "'Inter', 'Segoe UI', Arial, sans-serif", background: "#f8fafc", color: "#1f2937" },
  sidebar:   { width: 240, background: "#fff", borderRight: "1px solid #e5e7eb", display: "flex", flexDirection: "column", padding: "20px 12px", position: "sticky", top: 0, height: "100vh", overflowY: "auto" },
  logo:      { display: "flex", alignItems: "center", gap: 10, padding: "0 8px 20px", borderBottom: "1px solid #e5e7eb", marginBottom: 8 },
  logoIcon:  { fontSize: 26, color: "#2563eb" },
  logoTitle: { fontWeight: 700, fontSize: 15, color: "#111827" },
  logoSub:   { fontSize: 11, color: "#6b7280" },
  navItem:   { display: "flex", alignItems: "center", padding: "9px 12px", borderRadius: 8, cursor: "pointer", fontSize: 13, color: "#374151", marginBottom: 2, transition: "all 0.15s" },
  navActive: { background: "#eff6ff", color: "#2563eb", fontWeight: 600 },
  dropzone:  { border: "2px dashed #d1d5db", borderRadius: 10, padding: "20px 12px", textAlign: "center", cursor: "pointer", transition: "all 0.2s", marginBottom: 10 },
  dropzoneActive: { borderColor: "#2563eb", background: "#eff6ff" },
  dropText:  { fontSize: 12, color: "#374151", fontWeight: 500, wordBreak: "break-all" },
  dropSub:   { fontSize: 11, color: "#9ca3af", marginTop: 4 },
  btn:       { width: "100%", padding: "10px 0", background: "#2563eb", color: "#fff", border: "none", borderRadius: 8, cursor: "pointer", fontWeight: 600, fontSize: 14, display: "flex", alignItems: "center", justifyContent: "center" },
  btnDisabled: { background: "#93c5fd", cursor: "not-allowed" },
  errorBox:  { background: "#fee2e2", color: "#dc2626", padding: "8px 12px", borderRadius: 6, fontSize: 12, marginBottom: 8 },
  main:      { flex: 1, overflowY: "auto" },
  page:      { padding: "28px 32px", maxWidth: 1100 },
  hero:      { display: "flex", flexDirection: "column", alignItems: "center", justifyContent: "center", height: "100%", minHeight: "80vh", padding: 40, textAlign: "center" },
  heroIcon:  { fontSize: 56, color: "#2563eb", marginBottom: 16 },
  heroTitle: { fontSize: 32, fontWeight: 800, color: "#111827", margin: 0 },
  heroSub:   { fontSize: 16, color: "#6b7280", maxWidth: 540, marginTop: 12, lineHeight: 1.6 },
  heroFeatures: { display: "flex", flexWrap: "wrap", gap: 10, justifyContent: "center", marginTop: 24 },
  heroFeature:  { background: "#eff6ff", color: "#2563eb", padding: "6px 14px", borderRadius: 20, fontSize: 13, fontWeight: 500 },
  card:      { background: "#fff", borderRadius: 12, padding: "20px 24px", boxShadow: "0 1px 4px rgba(0,0,0,0.07)", marginBottom: 20, border: "1px solid #e5e7eb" },
  cardTitle: { fontWeight: 700, fontSize: 15, color: "#111827" },
  cardSub:   { fontSize: 13, color: "#6b7280", marginTop: 2 },
  banner:    { padding: "12px 16px", borderRadius: 8, border: "1px solid", marginBottom: 20, display: "flex", alignItems: "flex-start", gap: 8, flexWrap: "wrap" },
  row:       { display: "flex", gap: 20, marginBottom: 20, alignItems: "flex-start" },
  gaugeWrap: { background: "#fff", borderRadius: 12, padding: 20, border: "1px solid #e5e7eb", display: "flex", flexDirection: "column", alignItems: "center" },
  statCard:  { background: "#fff", borderRadius: 12, padding: "18px 20px", border: "1px solid #e5e7eb", display: "flex", flexDirection: "column", alignItems: "flex-start" },
  statIcon:  { width: 36, height: 36, borderRadius: 8, display: "flex", alignItems: "center", justifyContent: "center", fontSize: 16, marginBottom: 10 },
  statNum:   { fontSize: 26, fontWeight: 700, color: "#111827" },
  statLbl:   { fontSize: 12, color: "#6b7280", marginTop: 2 },
  pageHeader:{ marginBottom: 24 },
  pageTitle: { fontSize: 22, fontWeight: 800, color: "#111827", margin: 0 },
  pageSub:   { fontSize: 14, color: "#6b7280", marginTop: 4 },
  table:     { width: "100%", borderCollapse: "collapse" },
  thead:     { background: "#f9fafb" },
  th:        { padding: "10px 14px", textAlign: "left", fontSize: 12, fontWeight: 600, color: "#6b7280", textTransform: "uppercase", letterSpacing: "0.05em", borderBottom: "1px solid #e5e7eb" },
  td:        { padding: "10px 14px", fontSize: 13, color: "#374151", borderBottom: "1px solid #f3f4f6" },
  tr:        { transition: "background 0.1s" },
  pill:      { display: "inline-block", padding: "3px 10px", borderRadius: 20, fontSize: 12, fontWeight: 600 },
  badge:     { display: "inline-block", background: "#e5e7eb", borderRadius: 20, padding: "1px 8px", fontSize: 11, marginLeft: 6 },
  filterBtn: { padding: "7px 14px", border: "1px solid #e5e7eb", borderRadius: 8, cursor: "pointer", fontSize: 13, background: "#fff", color: "#374151", display: "flex", alignItems: "center" },
  filterActive: { background: "#2563eb", color: "#fff", borderColor: "#2563eb" },
  recCard:   { background: "#fff", border: "1px solid #e5e7eb", borderLeft: "4px solid", borderRadius: 8, padding: 16 },
  recHeader: { display: "flex", alignItems: "center", gap: 10, marginBottom: 10 },
  recCol:    { fontWeight: 700, fontSize: 14, fontFamily: "monospace" },
  recIssue:  { fontSize: 13, color: "#6b7280" },
  recImpact: { fontSize: 13, color: "#374151", marginBottom: 8, lineHeight: 1.5 },
  recAction: { fontSize: 13, color: "#374151", lineHeight: 1.5 },
  recTag:    { display: "inline-block", marginTop: 10, background: "#f3f4f6", color: "#6b7280", padding: "2px 10px", borderRadius: 4, fontSize: 11, fontFamily: "monospace" },
  insightRow:{ display: "flex", alignItems: "flex-start", gap: 12, padding: "10px 0", borderBottom: "1px solid #f3f4f6" },
  insightIcon:{ fontSize: 18, color: "#2563eb", minWidth: 24, textAlign: "center" },
  outlierRow:{ display: "flex", alignItems: "center", gap: 16, padding: "10px 0", borderBottom: "1px solid #f3f4f6" },
  outlierBadge:{ background: "#fef9c3", color: "#b45309", padding: "3px 10px", borderRadius: 20, fontSize: 12, fontWeight: 600, whiteSpace: "nowrap" },
  corrRow:   { display: "flex", alignItems: "center", padding: "10px 0", borderBottom: "1px solid #f3f4f6", gap: 4 },
  corrCol:   { fontWeight: 600, fontSize: 13, fontFamily: "monospace" },
  catCard:   { background: "#f9fafb", borderRadius: 8, padding: 14 },
  catTitle:  { fontWeight: 700, fontSize: 14, marginBottom: 4 },
  catRow:    { display: "flex", alignItems: "center", gap: 10, padding: "4px 0" },
  histWrap:  { background: "#f9fafb", borderRadius: 8, padding: 14 },
  colList:   { width: 180, background: "#fff", border: "1px solid #e5e7eb", borderRadius: 10, padding: 8, height: "fit-content", maxHeight: "70vh", overflowY: "auto" },
  colItem:   { padding: "8px 10px", borderRadius: 6, cursor: "pointer", fontSize: 13, display: "flex", alignItems: "center", marginBottom: 2 },
  colItemActive: { background: "#eff6ff", color: "#2563eb", fontWeight: 600 },
  statGrid:  { display: "grid", gridTemplateColumns: "repeat(auto-fill, minmax(130px, 1fr))", gap: 10 },
  statCell:  { background: "#f9fafb", borderRadius: 8, padding: 10 },
  statLabel: { fontSize: 11, color: "#6b7280", marginBottom: 2 },
  statValue: { fontSize: 15, fontWeight: 700, color: "#111827" },
  parseCell: { background: "#fff", border: "1px solid #e5e7eb", borderRadius: 8, padding: "12px 16px" },
  parseLbl:  { fontSize: 11, color: "#6b7280", marginBottom: 4, textTransform: "uppercase", letterSpacing: "0.05em" },
  parseVal:  { fontSize: 14, fontWeight: 600, color: "#111827" },
  warnRow:   { display: "flex", alignItems: "flex-start", padding: "8px 0", borderBottom: "1px solid #f3f4f6" },
  emptyState:{ color: "#6b7280", fontSize: 15, padding: 40, textAlign: "center", background: "#f9fafb", borderRadius: 10, border: "1px dashed #e5e7eb" },
  loading:   { display: "flex", flexDirection: "column", alignItems: "center", justifyContent: "center", height: "80vh" },
  loadingSpinner: { width: 48, height: 48, border: "4px solid #e5e7eb", borderTopColor: "#2563eb", borderRadius: "50%", animation: "spin 0.8s linear infinite" },
};

// Inject keyframe animation
const style = document.createElement("style");
style.textContent = `@keyframes spin { to { transform: rotate(360deg); } }
  nav div:hover { background: #f9fafb; }`;
document.head.appendChild(style);