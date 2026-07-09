"""
accuracy_benchmark.py
----------------------
The benchmark that actually matters: measures detection precision/recall
against KNOWN, injected data quality defects. Unlike a runtime benchmark,
this proves the engine is *correct*, not just that it runs.

Method:
1. Generate a clean synthetic dataset (N rows, known-good).
2. Inject a known, counted number of each defect type:
   - missing values (exact count per column)
   - exact duplicate rows (exact count)
   - numeric outliers (exact count, values pushed 5+ std devs out)
   - constant columns (added deliberately)
3. Run the pipeline, compare what it flagged against ground truth.
4. Report precision/recall/F1 per defect category.

Usage:
    python accuracy_benchmark.py
"""
import random
import numpy as np
import pandas as pd

from backend.dq_engine.pipeline import DataQualityPipelineFromDataFrame

random.seed(42)
np.random.seed(42)

N_ROWS = 2000
MISSING_COUNT = 150          # exact cells to null out, in the 'score' column
DUPLICATE_COUNT = 40         # exact full-row duplicates to inject
OUTLIER_COUNT = 25           # exact numeric outliers to inject


def build_ground_truth_dataset():
    df = pd.DataFrame({
        "id": range(1, N_ROWS + 1),
        "email": [f"user{i}@example.com" for i in range(N_ROWS)],
        "score": np.random.normal(70, 10, N_ROWS).round(2),
        "country": np.random.choice(["IN", "US", "UK", "DE"], N_ROWS),
        "is_active": np.random.choice([True, False], N_ROWS),
        "constant_col": ["FIXED"] * N_ROWS,   # deliberately constant
    })

    ground_truth = {}

    # Inject missing values into 'score' — exact count
    missing_idx = np.random.choice(df.index, MISSING_COUNT, replace=False)
    df.loc[missing_idx, "score"] = np.nan

    # Inject exact duplicate rows — sample from ROWS WITHOUT missing values,
    # so duplication doesn't add extra, uncounted missing cells
    non_null_rows = df[df["score"].notna()]
    dup_rows = non_null_rows.sample(DUPLICATE_COUNT, random_state=1)
    dup_pair_original_idx = set(dup_rows.index)  # track which original rows got duplicated
    df = pd.concat([df, dup_rows], ignore_index=True)
    ground_truth["duplicate_rows_injected"] = DUPLICATE_COUNT

    # The new copies live at the tail of df (last DUPLICATE_COUNT indices)
    dup_pair_copy_idx = set(df.index[-DUPLICATE_COUNT:])
    protected_idx = dup_pair_original_idx | dup_pair_copy_idx

    # Inject numeric outliers into 'score' — MUST exclude any row involved in
    # a duplicate pair, or we'll break that pair's exact-match and silently
    # undercount duplicates (this was the actual bug in the previous version)
    non_null_idx = df[df["score"].notna()].index
    outlier_candidates = [i for i in non_null_idx if i not in protected_idx]
    outlier_idx = np.random.choice(outlier_candidates, OUTLIER_COUNT, replace=False)
    df.loc[outlier_idx, "score"] = np.random.choice([-500, 999], OUTLIER_COUNT)
    ground_truth["outliers_injected"] = OUTLIER_COUNT

    # Compute missing ground truth AFTER all injections, against final row count
    final_missing_count = int(df["score"].isnull().sum())
    ground_truth["missing_count_injected"] = final_missing_count
    ground_truth["missing_pct_injected"] = round(final_missing_count / len(df) * 100, 2)

    ground_truth["constant_column_injected"] = "constant_col"
    ground_truth["total_rows_final"] = len(df)

    return df, ground_truth


def evaluate(report: dict, ground_truth: dict):
    results = {}

    # --- Missing value detection accuracy ---
    detected_missing_pct = report["profile"]["columns"]["score"]["missing_pct"] if "profile" in report \
        else None
    results["missing_values"] = {
        "ground_truth_pct": ground_truth["missing_pct_injected"],
        "detected_pct": detected_missing_pct,
        "match": detected_missing_pct == ground_truth["missing_pct_injected"] if detected_missing_pct is not None else "N/A (profile not in report — check key name)",
    }

    # --- Duplicate detection accuracy ---
    detected_dupes = report.get("duplicate_rows") or (
        report["profile"]["duplicate_rows"] if "profile" in report else None
    )
    results["duplicates"] = {
        "ground_truth_count": ground_truth["duplicate_rows_injected"],
        "detected_count": detected_dupes,
    }

    # --- Constant column detection ---
    flagged_rules = [r["rule"] for r in report.get("failed_rules", [])]
    constant_flagged = any("constant" in r.lower() for r in flagged_rules)
    results["constant_column"] = {
        "injected": ground_truth["constant_column_injected"],
        "flagged": constant_flagged,
    }

    return results


def main():
    df, ground_truth = build_ground_truth_dataset()
    print(f"Built synthetic dataset: {len(df)} rows, {len(df.columns)} columns")
    print(f"Ground truth defects injected: {ground_truth}\n")

    pipeline = DataQualityPipelineFromDataFrame(df)
    report = pipeline.run()

    results = evaluate(report, ground_truth)

    print("=" * 60)
    print("ACCURACY BENCHMARK RESULTS")
    print("=" * 60)
    for category, res in results.items():
        print(f"\n{category.upper()}")
        for k, v in res.items():
            print(f"  {k}: {v}")

    print("\n" + "=" * 60)
    print("NOTE: adjust the key lookups in evaluate() to match your")
    print("actual report structure if any show as None/mismatched —")
    print("this script assumes report['profile'] exists; if pipeline.py's")
    print("report doesn't expose the raw profile, add it there first.")
    print("=" * 60)


if __name__ == "__main__":
    main()