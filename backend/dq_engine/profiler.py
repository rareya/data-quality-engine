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
            "columns": {}
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

        return profile

    def _profile_numeric(self, series):
        clean = series.dropna()

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

    def _profile_categorical(self, series):
        clean = series.dropna()
        value_counts = clean.value_counts()

        most_common = value_counts.idxmax() if not value_counts.empty else None
        most_common_pct = round((value_counts.max() / len(clean)) * 100, 2) if not value_counts.empty else 0

        rare_count = int((value_counts / len(clean) < 0.05).sum())

        return {
            "most_frequent": most_common,
            "most_frequent_pct": most_common_pct,
            "rare_value_count": rare_count
        }
