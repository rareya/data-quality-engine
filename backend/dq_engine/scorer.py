class QualityScorer:
    def __init__(self, rule_results):
        self.rule_results = rule_results

    def evaluate(self):
        total = len(self.rule_results)
        passed = sum(1 for r in self.rule_results if r.passed)
        failed = total - passed

        score = round((passed / total) * 100, 2) if total > 0 else 0.0

        status = (
            "PASS" if score == 100
            else "WARNING" if score >= 70
            else "FAIL"
        )

        return {
            "total_rules": total,
            "passed": passed,
            "failed": failed,
            "score": score,
            "status": status,
            "details": [
                {
                    "rule": r.rule_name,
                    "passed": r.passed,
                    "message": r.message
                }
                for r in self.rule_results
            ]
        }
