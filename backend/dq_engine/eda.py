"""
eda.py
------
Exploratory Data Analysis module.
Goes beyond data quality checking into actual analysis.

This is what makes the project a proper DA project:
- Correlation analysis
- Distribution analysis  
- Trend detection
- Statistical summaries
- Outlier analysis

All results are serializable (no matplotlib objects) so they
can be sent via FastAPI and rendered in the frontend.
"""

import pandas as pd
import numpy as np


class EDAAnalyzer:
    def __init__(self, df: pd.DataFrame):
        self.df = df
        self.n_rows = len(df)
        self.numeric_cols = df.select_dtypes(include=[np.number]).columns.tolist()
        self.categorical_cols = df.select_dtypes(include=["object"]).columns.tolist()

    def analyze(self) -> dict:
        """
        Runs full EDA and returns serializable results dict.
        """
        results = {
            "numeric_summary": self._numeric_summary(),
            "correlation_matrix": self._correlation_matrix(),
            "distributions": self._distributions(),
            "categorical_summary": self._categorical_summary(),
            "outlier_analysis": self._outlier_analysis(),
            "missing_heatmap": self._missing_heatmap(),
            "insights": self._auto_insights()
        }
        return results

    # ---------------- numeric summary ----------------

    def _numeric_summary(self) -> dict:
        """
        Descriptive statistics for all numeric columns.
        """
        if not self.numeric_cols:
            return {}

        summary = {}
        for col in self.numeric_cols:
            series = self.df[col].dropna()
            if series.empty:
                continue

            summary[col] = {
                "count": int(series.count()),
                "mean": round(float(series.mean()), 4),
                "median": round(float(series.median()), 4),
                "std": round(float(series.std()), 4),
                "min": round(float(series.min()), 4),
                "max": round(float(series.max()), 4),
                "q1": round(float(series.quantile(0.25)), 4),
                "q3": round(float(series.quantile(0.75)), 4),
                "iqr": round(float(series.quantile(0.75) - series.quantile(0.25)), 4),
                "skewness": round(float(series.skew()), 4),
                "kurtosis": round(float(series.kurtosis()), 4),
                "cv": round(float(series.std() / series.mean()), 4) if series.mean() != 0 else None
            }

        return summary

    # ---------------- correlation ----------------

    def _correlation_matrix(self) -> dict:
        """
        Pearson correlation between all numeric columns.
        Returns as dict of dicts for easy frontend rendering.
        """
        if len(self.numeric_cols) < 2:
            return {}

        try:
            corr = self.df[self.numeric_cols].corr(method="pearson")
            
            # Round and convert to dict
            corr_dict = {}
            for col in corr.columns:
                corr_dict[col] = {
                    other: round(float(val), 4)
                    for other, val in corr[col].items()
                }

            # Find strong correlations (|r| > 0.7)
            strong_pairs = []
            cols = list(corr.columns)
            for i in range(len(cols)):
                for j in range(i + 1, len(cols)):
                    r = corr.iloc[i, j]
                    if abs(r) > 0.7:
                        strong_pairs.append({
                            "col1": cols[i],
                            "col2": cols[j],
                            "correlation": round(float(r), 4),
                            "type": "positive" if r > 0 else "negative",
                            "strength": "very strong" if abs(r) > 0.9 else "strong"
                        })

            return {
                "matrix": corr_dict,
                "strong_correlations": strong_pairs,
                "columns": self.numeric_cols
            }

        except Exception:
            return {}

    # ---------------- distributions ----------------

    def _distributions(self) -> dict:
        """
        Distribution data for frontend charts.
        Returns histogram bin data for each numeric column.
        """
        distributions = {}

        for col in self.numeric_cols:
            series = self.df[col].dropna()
            if series.empty or len(series) < 5:
                continue

            try:
                counts, bin_edges = np.histogram(series, bins=min(20, len(series) // 5))
                distributions[col] = {
                    "bins": [round(float(e), 4) for e in bin_edges[:-1]],
                    "counts": [int(c) for c in counts],
                    "type": self._detect_distribution_shape(series)
                }
            except Exception:
                continue

        return distributions

    def _detect_distribution_shape(self, series: pd.Series) -> str:
        """
        Roughly classifies distribution shape based on skewness.
        """
        skew = series.skew()
        if abs(skew) < 0.5:
            return "normal"
        elif skew > 1:
            return "right_skewed"
        elif skew < -1:
            return "left_skewed"
        else:
            return "slightly_skewed"

    # ---------------- categorical summary ----------------

    def _categorical_summary(self) -> dict:
        """
        Value counts and frequency analysis for categorical columns.
        """
        summary = {}

        for col in self.categorical_cols:
            series = self.df[col].dropna()
            if series.empty:
                continue

            value_counts = series.value_counts()
            total = len(series)

            summary[col] = {
                "unique_count": int(series.nunique()),
                "top_values": [
                    {
                        "value": str(val),
                        "count": int(count),
                        "pct": round(count / total * 100, 2)
                    }
                    for val, count in value_counts.head(10).items()
                ],
                "entropy": round(float(self._entropy(value_counts, total)), 4)
            }

        return summary

    def _entropy(self, value_counts: pd.Series, total: int) -> float:
        """Shannon entropy — measures diversity of categorical column."""
        probs = value_counts / total
        return -sum(p * np.log2(p) for p in probs if p > 0)

    # ---------------- outlier analysis ----------------

    def _outlier_analysis(self) -> dict:
        """
        IQR based outlier detection for all numeric columns.
        Returns outlier values and bounds.
        """
        outliers = {}

        for col in self.numeric_cols:
            series = self.df[col].dropna()
            if series.empty:
                continue

            q1 = series.quantile(0.25)
            q3 = series.quantile(0.75)
            iqr = q3 - q1

            lower = q1 - 1.5 * iqr
            upper = q3 + 1.5 * iqr

            outlier_values = series[(series < lower) | (series > upper)]

            if len(outlier_values) > 0:
                outliers[col] = {
                    "count": int(len(outlier_values)),
                    "pct": round(len(outlier_values) / len(series) * 100, 2),
                    "lower_bound": round(float(lower), 4),
                    "upper_bound": round(float(upper), 4),
                    "min_outlier": round(float(outlier_values.min()), 4),
                    "max_outlier": round(float(outlier_values.max()), 4),
                    "sample_outliers": [
                        round(float(v), 4)
                        for v in outlier_values.head(5).tolist()
                    ]
                }

        return outliers

    # ---------------- missing heatmap ----------------

    def _missing_heatmap(self) -> dict:
        """
        Missing value pattern analysis.
        Shows which columns are missing together (co-missing).
        """
        missing_cols = [
            col for col in self.df.columns
            if self.df[col].isnull().any()
        ]

        if not missing_cols:
            return {"missing_columns": [], "co_missing_pairs": []}

        missing_pcts = {
            col: round(self.df[col].isnull().mean() * 100, 2)
            for col in missing_cols
        }

        # Find columns that tend to be missing together
        co_missing = []
        for i in range(len(missing_cols)):
            for j in range(i + 1, len(missing_cols)):
                col1, col2 = missing_cols[i], missing_cols[j]
                both_missing = (
                    self.df[col1].isnull() & self.df[col2].isnull()
                ).sum()
                if both_missing > 0:
                    co_missing.append({
                        "col1": col1,
                        "col2": col2,
                        "co_missing_count": int(both_missing)
                    })

        return {
            "missing_columns": [
                {"column": col, "missing_pct": pct}
                for col, pct in sorted(
                    missing_pcts.items(),
                    key=lambda x: x[1],
                    reverse=True
                )
            ],
            "co_missing_pairs": co_missing[:10]
        }

    # ---------------- auto insights ----------------

    def _auto_insights(self) -> list:
        """
        Automatically generates human readable insights from the data.
        These are the kind of observations a DA would make manually.
        """
        insights = []

        # Insight 1: Most missing column
        missing_pcts = {
            col: self.df[col].isnull().mean() * 100
            for col in self.df.columns
        }
        worst_col = max(missing_pcts, key=missing_pcts.get)
        if missing_pcts[worst_col] > 0:
            insights.append({
                "type": "missing",
                "insight": f"'{worst_col}' has the highest missing rate "
                           f"({round(missing_pcts[worst_col], 1)}%)."
            })

        # Insight 2: Strong correlations
        if len(self.numeric_cols) >= 2:
            try:
                corr = self.df[self.numeric_cols].corr()
                for i in range(len(self.numeric_cols)):
                    for j in range(i + 1, len(self.numeric_cols)):
                        r = corr.iloc[i, j]
                        if abs(r) > 0.8:
                            insights.append({
                                "type": "correlation",
                                "insight": (
                                    f"Strong {'positive' if r > 0 else 'negative'} correlation "
                                    f"({round(r, 2)}) between "
                                    f"'{self.numeric_cols[i]}' and '{self.numeric_cols[j]}'. "
                                    f"These features may be redundant in ML models."
                                )
                            })
            except Exception:
                pass

        # Insight 3: Skewed distributions
        for col in self.numeric_cols:
            series = self.df[col].dropna()
            if len(series) > 10:
                skew = series.skew()
                if abs(skew) > 2:
                    insights.append({
                        "type": "distribution",
                        "insight": (
                            f"'{col}' is heavily {'right' if skew > 0 else 'left'}-skewed "
                            f"(skewness={round(skew, 2)}). "
                            f"Consider log transformation before analysis."
                        )
                    })

        # Insight 4: Low variance columns
        SKIP_VARIANCE_COLS = {"status", "http_status", "method", "http_method"}
        for col in self.numeric_cols:
            if col.lower() in SKIP_VARIANCE_COLS:
                continue
            series = self.df[col].dropna()
            if len(series) > 0 and series.mean() != 0:
                cv = series.std() / abs(series.mean())
                if cv < 0.01:
                    insights.append({
                        "type": "variance",
                        "insight": (
                            f"'{col}' has very low variance (CV={round(cv, 4)}). "
                            f"This column may not be useful for predictive modelling."
                        )
                    })

        return insights[:10]