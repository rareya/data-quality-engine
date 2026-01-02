
import pandas as pd
import numpy as np

class SchemaDetector:
    def __init__(self, df: pd.DataFrame):
        self.df = df
        self.n_rows = len(df)

    def infer_schema(self):
        schema = {}

        for col in self.df.columns:
            series = self.df[col]

            schema[col] = {
                "dtype": str(series.dtype),
                "semantic_type": self._infer_semantic_type(series),
                "missing_count": int(series.isnull().sum()),
                "missing_pct": round(series.isnull().mean() * 100, 2),
                "unique_count": int(series.nunique(dropna=True)),
                "unique_pct": round(series.nunique(dropna=True) / self.n_rows * 100, 2),
                "is_constant": series.nunique(dropna=True) <= 1,
                "sample_values": series.dropna().head(3).tolist()
            }

            if pd.api.types.is_numeric_dtype(series):
                schema[col].update({
                    "min": float(series.min()),
                    "max": float(series.max()),
                    "mean": float(series.mean()),
                    "std": float(series.std())
                })

        return schema

    def _infer_semantic_type(self, series):
        if pd.api.types.is_bool_dtype(series):
            return "boolean"

        if pd.api.types.is_datetime64_any_dtype(series):
            return "datetime"

        if pd.api.types.is_numeric_dtype(series):
            if series.nunique() / len(series) > 0.9:
                return "id-like"
            return "numeric"

        if pd.api.types.is_object_dtype(series):
            return "categorical"

        return "unknown"
