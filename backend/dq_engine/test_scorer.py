from backend.dq_engine.scorer import QualityScorer
from backend.dq_engine.rules import RuleResult


def main():
    print("Testing Quality Scorer\n")

    results = [
        RuleResult(rule_name="MissingValueRule", passed=False, message="Age missing too much", weight=20),
        RuleResult(rule_name="MissingValueRule", passed=True,  message="Marks OK",             weight=20),
        RuleResult(rule_name="ConstantColumnRule", passed=True, message="City OK",             weight=10),
        RuleResult(rule_name="DuplicateRowRule",  passed=False, message="10% duplicate rows",  weight=15),
    ]

    scorer = QualityScorer(results)
    summary = scorer.evaluate()

    print("Quality Summary")
    for k, v in summary.items():
        if k != "details":
            print(f"  {k}: {v}")

    print("\nRule Details:")
    for d in summary["details"]:
        status = "PASS" if d["passed"] else "FAIL"
        print(f"  [{status}] {d['rule']} — {d['message']} (weight: {d['weight']})")


if __name__ == "__main__":
    main()