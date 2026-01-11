import pandas as pd
import numpy as np

class DataProfiler:
    def __init__(self, df: pd.DataFrame):
        self.df = df
        self.n_rows = len(df)

    def profile(self):
        profile = {
            "row_count": self.n_rows,
            "duplicate_rows": int(self.df.duplicated().sum()),
            "columns": {},
            "log_metrics": {}
        }

        for col in self.df.columns:
            series = self.df[col]
            col_profile = {
                "missing_count": int(series.isnull().sum()),
                "missing_pct": round(series.isnull().mean() * 100, 2),
                "unique_count": int(series.nunique(dropna=True))
            }

            if pd.api.types.is_numeric_dtype(series):
                col_profile.update(self._profile_numeric(series))

            elif pd.api.types.is_object_dtype(series):
                col_profile.update(self._profile_categorical(series))

            profile["columns"][col] = col_profile

        # Log-aware metrics
        profile["log_metrics"] = self._profile_logs()

        return profile

    # ---------------- numeric ----------------

    def _profile_numeric(self, series):
        clean = series.dropna()

        if clean.empty:
            return {}

        q1 = clean.quantile(0.25)
        q3 = clean.quantile(0.75)
        iqr = q3 - q1
        outliers = clean[(clean < q1 - 1.5 * iqr) | (clean > q3 + 1.5 * iqr)]

        return {
            "min": float(clean.min()),
            "max": float(clean.max()),
            "mean": float(clean.mean()),
            "median": float(clean.median()),
            "std": float(clean.std()),
            "zero_count": int((clean == 0).sum()),
            "outlier_count": int(len(outliers))
        }

    # ---------------- categorical ----------------

    def _profile_categorical(self, series):
        clean = series.dropna()
        value_counts = clean.value_counts()

        if value_counts.empty:
            return {}

        return {
            "most_frequent": value_counts.idxmax(),
            "most_frequent_pct": round(value_counts.max() / len(clean) * 100, 2),
            "rare_value_count": int((value_counts / len(clean) < 0.05).sum())
        }

    # ---------------- log-specific ----------------

    def _profile_logs(self):
        metrics = {}

        if "status" in self.df.columns:
            status_counts = self.df["status"].value_counts(normalize=True) * 100
            metrics["status_distribution_pct"] = status_counts.round(2).to_dict()
            metrics["error_rate_pct"] = round(
                status_counts.get(500, 0) +
                status_counts.get(502, 0) +
                status_counts.get(503, 0),
                2
            )

        if "ip" in self.df.columns:
            metrics["top_ips"] = (
                self.df["ip"]
                .value_counts()
                .head(5)
                .to_dict()
            )

        if "endpoint" in self.df.columns:
            metrics["top_endpoints"] = (
                self.df["endpoint"]
                .value_counts()
                .head(5)
                .to_dict()
            )

        if "timestamp" in self.df.columns:
            parsed = pd.to_datetime(self.df["timestamp"], errors="coerce")
            metrics["time_span"] = {
                "start": str(parsed.min()),
                "end": str(parsed.max())
            }

        return metrics
