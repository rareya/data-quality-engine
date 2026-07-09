"""
real_data_benchmark.py
------------------------
Runs the pipeline against a real, industry-standard, publicly-cited dataset
(not synthetic) and reports results. Complements accuracy_benchmark.py:
that one proves correctness against known ground truth you construct;
this one proves usefulness against data you didn't design yourself.

Dataset: UCI Online Retail (541,909 rows)
  - Real transactions from a UK-based online retailer, Dec 2010 - Dec 2011
  - Cited in: Chen, D., Sain, S.L., Guo, K. (2012), Journal of Database
    Marketing and Customer Strategy Management, Vol 19
  - Publicly documented known defects: ~135,080 missing CustomerID (~24.9%),
    ~1,454 missing Description (~0.27%), ~5,268 duplicate rows (~1%),
    negative Quantity values representing order cancellations

Download into data/ first:
    Invoke-WebRequest -Uri "https://raw.githubusercontent.com/databricks/Spark-The-Definitive-Guide/master/data/retail-data/all/online-retail-dataset.csv" -OutFile "data\online_retail.csv"

Usage:
    python benchmark/real_data_benchmark.py
"""
from backend.dq_engine.pipeline import DataQualityPipeline

DATASETS = [
    "data/online_retail.csv",
]

# Publicly documented known stats for online_retail.csv, for sanity-checking
# what your engine reports against what's independently verifiable.
KNOWN_STATS = {
    "data/online_retail.csv": {
        "total_rows": 541909,
        "missing_customerid_pct": 24.93,
        "missing_description_pct": 0.27,
        "duplicate_rows_approx": 5268,
    }
}


def summarize(path: str):
    pipeline = DataQualityPipeline(path)
    report = pipeline.run()

    top_issues = sorted(
        report["column_issues"],
        key=lambda x: x["issue"],
        reverse=True
    )[:5]

    return {
        "dataset": path,
        "rows": report["rows"],
        "columns": report["column_count"],
        "quality_score": report["quality_score"]["score"],
        "status": report["quality_score"]["status"],
        "failed_rules_count": len(report["failed_rules"]),
        "top_issues": [f"{i['column']}: {i['issue']}" for i in top_issues],
    }


def main():
    for path in DATASETS:
        print("\n" + "=" * 70)
        print(f"REAL-DATASET BENCHMARK: {path}")
        print("=" * 70)
        try:
            r = summarize(path)
        except Exception as e:
            print(f"  ERROR: {e}")
            continue

        print(f"  Rows: {r['rows']}, Columns: {r['columns']}")
        print(f"  Quality Score: {r['quality_score']} ({r['status']})")
        print(f"  Failed rules: {r['failed_rules_count']}")
        print("  Top issues found:")
        for issue in r["top_issues"]:
            print(f"    - {issue}")

        known = KNOWN_STATS.get(path)
        if known:
            print("\n  Sanity check against publicly documented stats:")
            print(f"    Known total rows: {known['total_rows']} (yours: {r['rows']})")
            print(f"    Known missing CustomerID: ~{known['missing_customerid_pct']}%")
            print(f"    Known missing Description: ~{known['missing_description_pct']}%")
            print(f"    Known duplicate rows: ~{known['duplicate_rows_approx']}")
            print("    -> Manually compare these to what's printed in 'Top issues found' above.")


if __name__ == "__main__":
    main()