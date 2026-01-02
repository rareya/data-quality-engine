from backend.dq_engine.scorer import QualityScorer
from backend.dq_engine.rules import RuleResult

def main():
    print("🧪 Testing Quality Scorer")

    results = [
        RuleResult(False, "Age missing too much"),
        RuleResult(True, "Marks OK"),
        RuleResult(True, "City OK")
    ]

    scorer = QualityScorer(results)
    summary = scorer.compute_score()

    print("\n📊 Quality Summary")
    for k, v in summary.items():
        print(f"{k}: {v}")

if __name__ == "__main__":
    main()
