from backend.dq_engine.pipeline import DataQualityPipeline

pipeline = DataQualityPipeline("data/ongc_access.csv")
report = pipeline.run()

print("="*50)
print("SCORE:", report["quality_score"]["score"])
print("STATUS:", report["quality_score"]["status"])
print()
print("SUMMARY:", report["summary"])
print()
print("FAILED RULES:")
for r in report["failed_rules"]:
    print(f"  - {r['rule']}: {r['message']}")
print()
print("RECOMMENDATIONS:")
for r in report["recommendations"]:
    print(f"  [{r['severity']}] {r['column']} — {r['issue']}")
    print(f"  → {r['recommendation'][:100]}")
    print()
print("EDA INSIGHTS:")
for i in report["eda"]["insights"]:
    print(f"  - {i['insight']}")