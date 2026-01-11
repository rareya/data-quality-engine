import pandas as pd
import numpy as np
import re

class SchemaDetector:
    def __init__(self, df: pd.DataFrame):
        self.df = df
        self.n_rows = len(df)

    def infer_schema(self):
        schema = {}

        for col in self.df.columns:
            series = self.df[col]

            col_schema = {
                "dtype": str(series.dtype),
                "semantic_type": self._infer_semantic_type(col, series),
                "missing_count": int(series.isnull().sum()),
                "missing_pct": round(series.isnull().mean() * 100, 2),
                "unique_count": int(series.nunique(dropna=True)),
                "is_constant": series.nunique(dropna=True) <= 1,
                "sample_values": series.dropna().head(3).tolist()
            }

            # Numeric enrichment
            if pd.api.types.is_numeric_dtype(series):
                col_schema.update({
                    "min": float(series.min()),
                    "max": float(series.max()),
                    "mean": float(series.mean()),
                    "std": float(series.std())
                })

            # Log-specific validation
            if col == "ip":
                col_schema["invalid_ip_pct"] = self._invalid_ip_pct(series)

            if col == "timestamp":
                col_schema["unparseable_timestamp_pct"] = self._invalid_timestamp_pct(series)

            if col == "status":
                col_schema["invalid_status_pct"] = self._invalid_status_pct(series)

            schema[col] = col_schema

        return schema

    # ---------------- semantic inference ----------------

    def _infer_semantic_type(self, col, series):
        if col == "ip":
            return "ip_address"

        if col == "timestamp":
            return "timestamp"

        if col == "status":
            return "http_status"

        if col == "method":
            return "http_method"

        if pd.api.types.is_bool_dtype(series):
            return "boolean"

        if pd.api.types.is_datetime64_any_dtype(series):
            return "datetime"

        if pd.api.types.is_numeric_dtype(series):
            return "numeric"

        if pd.api.types.is_object_dtype(series):
            return "categorical"

        return "unknown"

    # ---------------- validators ----------------

    def _invalid_ip_pct(self, series):
        ip_regex = re.compile(
            r"^(\d{1,3}\.){3}\d{1,3}$|^([a-fA-F0-9:]+)$"
        )
        invalid = series.dropna().apply(lambda x: not bool(ip_regex.match(str(x))))
        return round(invalid.mean() * 100, 2)

    def _invalid_timestamp_pct(self, series):
        parsed = pd.to_datetime(series, errors="coerce")
        return round(parsed.isnull().mean() * 100, 2)

    def _invalid_status_pct(self, series):
        invalid = ~series.between(100, 599)
        return round(invalid.mean() * 100, 2)
