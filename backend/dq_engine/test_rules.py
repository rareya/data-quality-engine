from backend.dq_engine.loader import DataLoader
from backend.dq_engine.profiler import DataProfiler
from backend.dq_engine.rules import MissingValueRule


def main():
    print(" Testing Rules Engine\n")

    loader = DataLoader("data/students.csv")
    df = loader.load()

    profiler = DataProfiler(df)
    profile = profiler.profile()

    rule = MissingValueRule(column="age", max_missing_pct=10)
    result = rule.evaluate(profile)

    print(f"Rule Passed: {result.passed}")
    print(f"Message: {result.message}")


if __name__ == "__main__":
    main()
