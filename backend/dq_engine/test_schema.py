print(" test_schema.py file started")


from backend.dq_engine.schema import SchemaDetector
from backend.dq_engine.loader import DataLoader


def main():
    print(" Starting Schema Detection Test...\n")

    # Step 1: Load data
    file_path = "data/students.csv"
    loader = DataLoader(file_path)
    df = loader.load()

    print(" Data Loaded Successfully")
    print(df.head(), "\n")

    # Step 2: Run schema detector
    detector = SchemaDetector(df)
    schema = detector.infer_schema()

    # Step 3: Print schema nicely
    print(" Inferred Schema:\n")

    for column, details in schema.items():
        print(f"Column: {column}")
        for key, value in details.items():
            print(f"  {key}: {value}")
        print("-" * 40)


if __name__ == "__main__":
    main()
