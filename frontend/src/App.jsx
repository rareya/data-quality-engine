import { useState } from "react";

export default function App() {
  const [file, setFile] = useState(null);
  const [data, setData] = useState(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState(null);

  const analyze = async () => {
    if (!file) {
      setError("Please upload a CSV file");
      return;
    }

    setLoading(true);
    setError(null);
    setData(null);

    try {
      const formData = new FormData();
      formData.append("file", file);

      const res = await fetch("http://127.0.0.1:8000/analyze", {
        method: "POST",
        body: formData,
      });

      if (!res.ok) throw new Error("Backend error");

      const json = await res.json();
      setData(json);
    } catch (err) {
      setError(err.message);
    } finally {
      setLoading(false);
    }
  };

  return (
    <div style={styles.page}>
      <div style={styles.container}>
        <h1>Data Quality Engine</h1>
        <p>Automated data quality analysis & scoring</p>

        <div style={styles.card}>
          <input
            type="file"
            accept=".csv"
            onChange={(e) => setFile(e.target.files[0])}
          />
          <br /><br />
          <button onClick={analyze} disabled={loading}>
            {loading ? "Analyzing..." : "Analyze Dataset"}
          </button>
        </div>

        {error && <p style={{ color: "red" }}>{error}</p>}

        {data && <Dashboard data={data} />}
      </div>
    </div>
  );
}

/* ---------------- Dashboard ---------------- */

function Dashboard({ data }) {
  const score = data.quality_score?.score ?? 0;
  const status = data.quality_score?.status ?? "UNKNOWN";
  const failedRules = data.failed_rules ?? [];

  return (
    <>
      <div style={styles.card}>
        <h2>Overall Quality Score</h2>
        <div style={styles.score}>{score.toFixed(2)} / 100</div>
        <p>Status: <strong>{status}</strong></p>
      </div>

      <div style={styles.card}>
        <h2>Dataset Overview</h2>
        <ul>
          <li><strong>Rows:</strong> {data.rows}</li>
          <li><strong>Columns:</strong> {data.column_count}</li>
        </ul>
      </div>

      <div style={styles.card}>
        <h2>Failed Rules</h2>

        {failedRules.length === 0 ? (
          <p style={{ color: "green" }}>No rule violations 🎉</p>
        ) : (
          <table style={styles.table}>
            <thead>
              <tr>
                <th>Rule</th>
                <th>Message</th>
              </tr>
            </thead>
            <tbody>
              {failedRules.map((r, i) => (
                <tr key={i}>
                  <td>{r.rule}</td>
                  <td style={{ color: "#d1242f" }}>{r.message}</td>
                </tr>
              ))}
            </tbody>
          </table>
        )}
      </div>
    </>
  );
}

/* ---------------- Styles ---------------- */

const styles = {
  page: {
    minHeight: "100vh",
    background: "#f4f6f8",
    padding: "2rem",
    fontFamily: "Inter, Arial, sans-serif",
  },
  container: {
    maxWidth: "1000px",
    margin: "0 auto",
  },
  card: {
    background: "#fff",
    padding: "1.5rem",
    borderRadius: "8px",
    boxShadow: "0 4px 12px rgba(0,0,0,0.08)",
    marginBottom: "1.5rem",
  },
  score: {
    fontSize: "3rem",
    fontWeight: "bold",
    color: "#1f2937",
  },
  table: {
    width: "100%",
    borderCollapse: "collapse",
  },
};
