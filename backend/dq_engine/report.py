class QualityReport:
    def __init__(self, profile, rule_results, score_report):
        self.profile = profile
        self.rule_results = rule_results
        self.score_report = score_report

    def generate(self):
        return {
            "rows": self.profile["row_count"],
            "columns": list(self.profile["columns"].keys()),
            "column_count": len(self.profile["columns"]),
            "quality_score": self.score_report,
            "failed_rules": self._failed_rules(),
            "column_issues": self._column_issues()
        }

    def _failed_rules(self):
        return [
            {
                "rule": r.rule_name,
                "message": r.message
            }
            for r in self.rule_results
            if not r.passed
        ]

    def _column_issues(self):
        issues = []

        for col, meta in self.profile["columns"].items():
            missing_pct = meta.get("missing_pct", 0)
            if missing_pct > 0:
                issues.append({
                    "column": col,
                    "issue": f"{missing_pct}% missing values"
                })

        return issues
