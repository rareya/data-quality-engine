from backend.dq_engine.loader import DataLoader
from backend.dq_engine.profiler import DataProfiler


def main():
    print("📊 Starting Data Profiler Test...\n")

    # Step 1: Load sample data
    file_path = "data/students.csv"
    loader = DataLoader(file_path)
    df = loader.load()

    print("✅ Data Loaded Successfully")
    print(df.head(), "\n")

    # Step 2: Run profiler
    profiler = DataProfiler(df)
    profile = profiler.profile()

    # Step 3: Print profiling results
    print("📘 Profiling Results:\n")

    print(f"Total Rows: {profile['row_count']}")
    print(f"Duplicate Rows: {profile['duplicate_rows']}\n")

    for col, stats in profile["columns"].items():
        print(f"Column: {col}")
        for key, value in stats.items():
            print(f"  {key}: {value}")
        print("-" * 40)


if __name__ == "__main__":
    main()
