class QualityScorer:
    def __init__(self, rule_results):
        self.rule_results = rule_results

    def compute_score(self):
        total = len(self.rule_results)
        passed = sum(1 for r in self.rule_results if r.passed)
        failed = total - passed

        score = round((passed / total) * 100, 2) if total > 0 else 0.0

        if score == 100:
            status = "PASS"
        elif score >= 70:
            status = "WARNING"
        else:
            status = "FAIL"

        return {
            "total_rules": total,
            "passed": passed,
            "failed": failed,
            "score": score,
            "status": status
        }
