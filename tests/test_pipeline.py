from backend.dq_engine.pipeline import DataQualityPipeline


def main():
    pipeline = DataQualityPipeline("data/ongc_access.csv")
    report = pipeline.run()

    print("\n📊 DATA QUALITY PIPELINE REPORT\n")
    print("Rows:", report["rows"])
    print("Columns:", report["columns"])
    print("Quality Score:", report["quality_score"])


if __name__ == "__main__":
    main()
