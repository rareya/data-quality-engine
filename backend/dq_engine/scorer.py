class QualityScorer:
    def __init__(self, rule_results):
        self.rule_results = rule_results

    def evaluate(self):
        total_weight = sum(r.weight for r in self.rule_results)
        earned_weight = sum(r.weight for r in self.rule_results if r.passed)

        score = round((earned_weight / total_weight) * 100, 2) if total_weight > 0 else 0.0

        if score >= 90:
            status = "EXCELLENT"
        elif score >= 70:
            status = "GOOD"
        elif score >= 50:
            status = "WARNING"
        else:
            status = "POOR"

        return {
            "score": score,
            "status": status,
            "total_weight": total_weight,
            "earned_weight": earned_weight,
            "details": [
                {
                    "rule": r.rule_name,
                    "passed": r.passed,
                    "weight": r.weight,
                    "message": r.message
                }
                for r in self.rule_results
            ]
        }
