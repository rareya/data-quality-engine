from .loader import DataLoader
from .profiler import DataProfiler
import os

def main():
    base_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../"))
    data_path = os.path.join(base_dir, "data", "students.csv")

    loader = DataLoader(data_path)
    df = loader.load()

    profiler = DataProfiler(df)
    profile = profiler.profile()

    for key, value in profile.items():
        print(key, ":", value)

if __name__ == "__main__":
    main()
