"""
recommendations.py
------------------
Generates actionable, business-friendly recommendations
based on profiling results and failed rules.

This is what separates a data quality CHECKER from a data quality ENGINE.
Instead of just saying "40% missing values", we tell the user WHAT TO DO.
"""


class RecommendationEngine:
    def __init__(self, profile: dict, schema: dict, rule_results: list):
        self.profile = profile
        self.schema = schema
        self.rule_results = rule_results

    def generate(self) -> list:
        recommendations = []

        for col, col_profile in self.profile["columns"].items():
            semantic = (
                self.schema.get(col, {})
                .get("semantic_type", "unknown")
                .strip()
                .lower()
            )

            # Missing values
            missing_pct = col_profile.get("missing_pct", 0)
            if missing_pct > 0:
                recommendations.append(
                    self._missing_recommendation(col, missing_pct, semantic, col_profile)
                )

            # Outliers in numeric columns
            outlier_count = col_profile.get("outlier_count", 0)
            if outlier_count > 0:
                recommendations.append(
                    self._outlier_recommendation(col, outlier_count, col_profile)
                )

            # Constant columns — skip for semantics where constant is expected
            CONSTANT_OK_SEMANTICS = {"http_status", "http_method", "boolean", "status"}
            unique_count = col_profile.get("unique_count", 0)
            col_semantic = self.schema.get(col, {}).get("semantic_type", "unknown").lower()

            if unique_count <= 1 and col_semantic not in CONSTANT_OK_SEMANTICS:
                recommendations.append({
                    "column": col,
                    "issue": "Constant Column",
                    "severity": "MEDIUM",
                    "business_impact": f"Column '{col}' has only one unique value and adds no analytical value.",
                    "recommendation": f"Consider dropping column '{col}' from your dataset. "
                                      f"Constant columns waste storage and can confuse ML models.",
                    "action": "DROP_COLUMN"
                })

            # High cardinality categorical
            if semantic == "categorical":
                total_rows = self.profile["row_count"]
                if unique_count > 0.9 * total_rows:
                    recommendations.append({
                        "column": col,
                        "issue": "High Cardinality",
                        "severity": "LOW",
                        "business_impact": f"Column '{col}' has nearly unique values in every row — "
                                           f"likely a free-text or ID field that cannot be aggregated.",
                        "recommendation": f"If '{col}' is an identifier, exclude it from analysis. "
                                          f"If it's free text, consider NLP techniques or bucketing.",
                        "action": "REVIEW_COLUMN"
                    })

            # Zero values in numeric
            zero_count = col_profile.get("zero_count", 0)
            if zero_count > 0 and semantic == "numeric":
                zero_pct = round((zero_count / self.profile["row_count"]) * 100, 2)
                if zero_pct > 10:
                    recommendations.append({
                        "column": col,
                        "issue": "High Zero Count",
                        "severity": "LOW",
                        "business_impact": f"{zero_pct}% of values in '{col}' are zero. "
                                           f"This may indicate missing data encoded as zero, "
                                           f"which would skew averages and aggregations.",
                        "recommendation": f"Verify whether zeros in '{col}' represent actual zero values "
                                          f"or missing data. If missing, replace with NaN and impute.",
                        "action": "VERIFY_ZEROS"
                    })

        # Dataset level recommendations
        duplicate_rows = self.profile.get("duplicate_rows", 0)
        if duplicate_rows > 0:
            dup_pct = round((duplicate_rows / self.profile["row_count"]) * 100, 2)
            recommendations.append({
                "column": "DATASET",
                "issue": "Duplicate Rows",
                "severity": "HIGH" if dup_pct > 5 else "MEDIUM",
                "business_impact": f"{duplicate_rows} duplicate rows ({dup_pct}%) detected. "
                                   f"Duplicates cause double counting in reports, "
                                   f"inflate metrics and corrupt aggregations.",
                "recommendation": "Remove duplicate rows using pandas df.drop_duplicates(). "
                                  "Investigate the root cause — duplicates often indicate "
                                  "pipeline or ingestion bugs upstream.",
                "action": "DROP_DUPLICATES"
            })

        # Log specific recommendations
        log_metrics = self.profile.get("log_metrics", {})
        error_rate = log_metrics.get("error_rate_pct")
        if error_rate and error_rate > 2:
            recommendations.append({
                "column": "LOG_METRICS",
                "issue": "High Error Rate",
                "severity": "HIGH",
                "business_impact": f"Server error rate is {error_rate}%. "
                                   f"This indicates system instability that may affect "
                                   f"data completeness and reliability.",
                "recommendation": "Investigate server logs for 500/502/503 patterns. "
                                  "High error rates often indicate infrastructure issues "
                                  "or upstream service failures.",
                "action": "INVESTIGATE_ERRORS"
            })

        return recommendations

    # ---------------- helpers ----------------

    def _missing_recommendation(self, col, missing_pct, semantic, col_profile):
        if missing_pct > 50:
            severity = "HIGH"
            business_impact = (
                f"Column '{col}' has {missing_pct}% missing values — "
                f"more than half the data is absent. "
                f"Any analysis using this column will be unreliable."
            )
            recommendation = (
                f"Consider dropping column '{col}' if missing rate exceeds 70%. "
                f"If the column is critical, investigate the data pipeline "
                f"for ingestion failures upstream."
            )
            action = "DROP_OR_INVESTIGATE"

        elif missing_pct > 20:
            severity = "MEDIUM"
            business_impact = (
                f"Column '{col}' has {missing_pct}% missing values. "
                f"This will introduce bias in any analysis or model using this column."
            )
            if semantic == "numeric":
                recommendation = (
                    f"Impute missing values in '{col}' using median "
                    f"(robust to outliers) or mean imputation. "
                    f"For time series data, consider forward-fill."
                )
            else:
                recommendation = (
                    f"Impute missing values in '{col}' using mode (most frequent value) "
                    f"or add an explicit 'Unknown' category."
                )
            action = "IMPUTE"

        else:
            severity = "LOW"
            business_impact = (
                f"Column '{col}' has {missing_pct}% missing values — "
                f"low but worth addressing before production use."
            )
            recommendation = (
                f"Safe to impute '{col}' with "
                f"{'median' if semantic == 'numeric' else 'mode'}. "
                f"Impact on analysis will be minimal."
            )
            action = "IMPUTE"

        return {
            "column": col,
            "issue": "Missing Values",
            "severity": severity,
            "business_impact": business_impact,
            "recommendation": recommendation,
            "action": action
        }

    def _outlier_recommendation(self, col, outlier_count, col_profile):
        outlier_pct = round(
            (outlier_count / self.profile["row_count"]) * 100, 2
        )
        mean = col_profile.get("mean", "N/A")
        median = col_profile.get("median", "N/A")

        return {
            "column": col,
            "issue": "Outliers Detected",
            "severity": "MEDIUM" if outlier_pct > 5 else "LOW",
            "business_impact": (
                f"{outlier_count} outliers ({outlier_pct}%) detected in '{col}'. "
                f"Mean={round(mean, 2) if isinstance(mean, float) else mean}, "
                f"Median={round(median, 2) if isinstance(median, float) else median}. "
                f"Outliers can skew averages, corrupt ML model training "
                f"and lead to misleading business insights."
            ),
            "recommendation": (
                f"Investigate outliers in '{col}' — are they data entry errors or genuine values? "
                f"If errors: cap using IQR method. "
                f"If genuine: use median instead of mean for reporting, "
                f"and consider robust scaling for ML."
            ),
            "action": "INVESTIGATE_OUTLIERS"
        }


def generate_natural_language_summary(profile: dict, score_report: dict, recommendations: list) -> str:
    """
    Generates a plain English executive summary of the data quality report.
    This is what a business user reads — not a technical person.
    """
    score = score_report.get("score", 0)
    status = score_report.get("status", "UNKNOWN")
    row_count = profile.get("row_count", 0)
    col_count = len(profile.get("columns", {}))
    duplicate_rows = profile.get("duplicate_rows", 0)

    high_severity = [r for r in recommendations if r.get("severity") == "HIGH"]
    medium_severity = [r for r in recommendations if r.get("severity") == "MEDIUM"]
    low_severity = [r for r in recommendations if r.get("severity") == "LOW"]

    if status == "PASS":
        overall = "Your dataset is in excellent condition and ready for analysis."
    elif status == "WARNING":
        overall = "Your dataset has some quality issues that should be addressed before analysis."
    else:
        overall = "Your dataset has critical quality issues. Analysis based on this data may be unreliable."

    summary = (
        f"Dataset contains {row_count:,} rows and {col_count} columns. "
        f"Overall Data Quality Score: {score}/100 — {status}. "
        f"{overall} "
    )

    if duplicate_rows > 0:
        summary += f"Found {duplicate_rows} duplicate rows that should be removed. "

    if high_severity:
        summary += f"{len(high_severity)} critical issue(s) require immediate attention. "

    if medium_severity:
        summary += f"{len(medium_severity)} moderate issue(s) should be reviewed. "

    if low_severity:
        summary += f"{len(low_severity)} minor issue(s) flagged for awareness. "

    if not recommendations:
        summary += "No issues detected — data meets all quality standards."

    return summary.strip()