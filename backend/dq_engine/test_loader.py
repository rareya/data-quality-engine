from .loader import DataLoader

def main():
    loader = DataLoader("data/students.csv")
    df = loader.load()
    print(df.head())

if __name__ == "__main__":
    main()