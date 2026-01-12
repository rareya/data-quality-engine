class QualityScorer:
    def __init__(self, rule_results):
        self.rule_results = rule_results

    def evaluate(self):
        if not self.rule_results:
            return {
                "score": 0.0,
                "status": "NO_RULES",
                "total_weight": 0,
                "passed_weight": 0,
                "details": []
            }

        total_weight = sum(r.weight for r in self.rule_results)
        passed_weight = sum(r.weight for r in self.rule_results if r.passed)

        score = round((passed_weight / total_weight) * 100, 2)

        if score == 100:
            status = "PASS"
        elif score >= 70:
            status = "WARNING"
        else:
            status = "FAIL"

        return {
            "score": score,
            "status": status,
            "total_weight": total_weight,
            "passed_weight": passed_weight,
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
