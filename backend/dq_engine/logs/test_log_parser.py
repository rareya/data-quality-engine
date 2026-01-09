from backend.dq_engine.logs.log_parser import LogParser

def main():
    parser = LogParser("data/ongc_access.csv")
    df = parser.parse()

    print("Parsed rows:", len(df))
    print("Columns:", list(df.columns))
    print(df.head())

if __name__ == "__main__":
    main()
