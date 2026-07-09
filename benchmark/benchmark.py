"""
benchmark.py
------------
Compares Data Quality Engine against Great Expectations on the same dataset:
runtime, setup lines-of-code, and rule/check overlap.

Written for great_expectations 1.x (the "Fluent" API — from_pandas() was
removed in this version; GE now requires an explicit context, data source,
and expectation suite). If you're on an older 0.x GE version, this will
fail differently — tell me the exact error and I'll adjust.

Usage:
    python benchmark/benchmark.py --data data/online_retail.csv
"""
import argparse
import time
import pandas as pd

from backend.dq_engine.pipeline import DataQualityPipeline

try:
    import great_expectations as gx
    GE_AVAILABLE = True
except ImportError:
    GE_AVAILABLE = False


def benchmark_dqe(data_path: str):
    start = time.perf_counter()
    pipeline = DataQualityPipeline(data_path)
    report = pipeline.run()
    elapsed = time.perf_counter() - start

    quality_score = report["quality_score"]
    checks_run = len(quality_score.get("details", []))

    return {
        "tool": "Data Quality Engine",
        "runtime_sec": round(elapsed, 4),
        "checks_run": checks_run,
        "quality_score": quality_score.get("score"),
        "quality_status": quality_score.get("status"),
        "setup_lines_of_code": 2,  # DataQualityPipeline(path).run()
    }


def benchmark_great_expectations(data_path: str):
    if not GE_AVAILABLE:
        return {
            "tool": "Great Expectations",
            "runtime_sec": None,
            "checks_run": None,
            "quality_score": None,
            "setup_lines_of_code": None,
            "note": "not installed — pip install great_expectations",
        }

    df = pd.read_csv(data_path)
    start = time.perf_counter()

    setup_loc = 0  # count "real" setup lines as we go, for an honest comparison

    context = gx.get_context(mode="ephemeral")
    setup_loc += 1

    data_source = context.data_sources.add_pandas("pandas_datasource")
    setup_loc += 1
    data_asset = data_source.add_dataframe_asset(name="dataframe_asset")
    setup_loc += 1
    batch_definition = data_asset.add_batch_definition_whole_dataframe("batch_definition")
    setup_loc += 1
    batch = batch_definition.get_batch(batch_parameters={"dataframe": df})
    setup_loc += 1

    suite = gx.ExpectationSuite(name="dq_benchmark_suite")
    context.suites.add(suite)
    setup_loc += 2

    checks = 0
    for col in df.columns:
        suite.add_expectation(
            gx.expectations.ExpectColumnValuesToNotBeNull(column=col)
        )
        checks += 1
        setup_loc += 1
        if pd.api.types.is_numeric_dtype(df[col]):
            suite.add_expectation(
                gx.expectations.ExpectColumnValuesToBeBetween(
                    column=col,
                    min_value=float(df[col].min()),
                    max_value=float(df[col].max()),
                )
            )
            checks += 1
            setup_loc += 1

    result = batch.validate(suite)
    elapsed = time.perf_counter() - start

    passed = sum(1 for r in result["results"] if r["success"])
    score = round((passed / checks) * 100, 2) if checks else 0

    return {
        "tool": "Great Expectations",
        "runtime_sec": round(elapsed, 4),
        "checks_run": checks,
        "quality_score": score,
        "setup_lines_of_code": setup_loc,
    }


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--data", default="data/online_retail.csv")
    args = parser.parse_args()

    print(f"Benchmarking against: {args.data}\n")

    dqe_result = benchmark_dqe(args.data)

    try:
        ge_result = benchmark_great_expectations(args.data)
    except Exception as e:
        print(f"Great Expectations run failed: {e}")
        print("Paste this full error back — GE's API varies by version, "
              "easier to fix with the real traceback than guess further.\n")
        ge_result = {}

    print(f"{'Metric':<22}{'Data Quality Engine':<25}{'Great Expectations':<25}")
    print("-" * 72)
    for key in ["runtime_sec", "checks_run", "quality_score", "setup_lines_of_code"]:
        print(f"{key:<22}{str(dqe_result.get(key)):<25}{str(ge_result.get(key)):<25}")

    if "quality_status" in dqe_result:
        print(f"\nData Quality Engine status: {dqe_result['quality_status']}")

    if not GE_AVAILABLE:
        print("\n(Install great_expectations for a real side-by-side)")


if __name__ == "__main__":
    main()