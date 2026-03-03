"""
sql_loader.py
-------------
Loads data directly from SQL databases (SQLite or PostgreSQL).
This makes the project a proper DA tool — not just a file uploader.

Supports:
- SQLite (local file based database — great for demos)
- PostgreSQL (production databases)
"""

import pandas as pd


class SQLLoader:
    def __init__(self, connection_string: str):
        """
        connection_string examples:
        - SQLite:     "sqlite:///path/to/database.db"
        - PostgreSQL: "postgresql://user:password@host:port/dbname"
        """
        self.connection_string = connection_string
        self.engine = None

    def connect(self):
        try:
            from sqlalchemy import create_engine
            self.engine = create_engine(self.connection_string)
            return True
        except Exception as e:
            raise ConnectionError(f"Failed to connect to database: {e}")

    def list_tables(self) -> list:
        """Returns all table names in the database."""
        try:
            from sqlalchemy import inspect
            inspector = inspect(self.engine)
            return inspector.get_table_names()
        except Exception as e:
            raise RuntimeError(f"Failed to list tables: {e}")

    def load_table(self, table_name: str) -> pd.DataFrame:
        """Loads an entire table into a DataFrame."""
        try:
            df = pd.read_sql_table(table_name, self.engine)
            return df
        except Exception as e:
            raise RuntimeError(f"Failed to load table '{table_name}': {e}")

    def load_query(self, query: str) -> pd.DataFrame:
        """
        Executes a custom SQL query and returns results as DataFrame.
        
        Example:
            loader.load_query("SELECT * FROM sales WHERE year = 2023")
        """
        try:
            df = pd.read_sql_query(query, self.engine)
            return df
        except Exception as e:
            raise RuntimeError(f"Failed to execute query: {e}")

    def get_table_info(self, table_name: str) -> dict:
        """
        Returns metadata about a table:
        - Column names and types
        - Row count
        - Sample data
        """
        try:
            from sqlalchemy import inspect, text

            inspector = inspect(self.engine)
            columns = inspector.get_columns(table_name)

            with self.engine.connect() as conn:
                row_count = conn.execute(
                    text(f"SELECT COUNT(*) FROM {table_name}")
                ).scalar()

            sample_df = pd.read_sql_query(
                f"SELECT * FROM {table_name} LIMIT 5",
                self.engine
            )

            return {
                "table_name": table_name,
                "row_count": row_count,
                "columns": [
                    {
                        "name": col["name"],
                        "type": str(col["type"])
                    }
                    for col in columns
                ],
                "sample": sample_df.to_dict(orient="records")
            }

        except Exception as e:
            raise RuntimeError(f"Failed to get table info: {e}")


class SQLiteLoader(SQLLoader):
    """
    Simplified loader specifically for SQLite databases.
    Great for demos and local development.
    
    Usage:
        loader = SQLiteLoader("path/to/database.db")
        loader.connect()
        df = loader.load_table("customers")
    """

    def __init__(self, db_path: str):
        super().__init__(f"sqlite:///{db_path}")
        self.db_path = db_path

    @classmethod
    def create_demo_database(cls, db_path: str = "demo.db") -> "SQLiteLoader":
        """
        Creates a demo SQLite database with sample data
        for testing and demonstrations.
        """
        import sqlite3
        import numpy as np

        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()

        # Create sample sales table with intentional quality issues
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS sales (
                id INTEGER PRIMARY KEY,
                customer_name TEXT,
                product TEXT,
                quantity INTEGER,
                unit_price REAL,
                sale_date TEXT,
                region TEXT,
                sales_rep TEXT
            )
        """)

        # Insert sample data with quality issues
        import random
        from datetime import datetime, timedelta

        random.seed(42)
        regions = ["North", "South", "East", "West", None]
        products = ["Product A", "Product B", "Product C", "Product D"]
        reps = ["Alice", "Bob", "Charlie", "Diana", "Eve"]

        rows = []
        base_date = datetime(2023, 1, 1)

        for i in range(200):
            sale_date = base_date + timedelta(days=random.randint(0, 364))
            region = random.choice(regions)
            quantity = random.randint(1, 100) if random.random() > 0.05 else None
            price = round(random.uniform(10, 500), 2) if random.random() > 0.03 else None

            # Inject duplicates
            if i in [50, 51, 100, 101, 150, 151]:
                rows.append(rows[-1] if rows else (
                    i, "Duplicate Customer", "Product A",
                    10, 99.99, "2023-01-01", "North", "Alice"
                ))
            else:
                rows.append((
                    i,
                    f"Customer_{random.randint(1, 50)}",
                    random.choice(products),
                    quantity,
                    price,
                    sale_date.strftime("%Y-%m-%d"),
                    region,
                    random.choice(reps)
                ))

        cursor.executemany("""
            INSERT OR IGNORE INTO sales 
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, rows)

        conn.commit()
        conn.close()

        print(f"Demo database created at: {db_path}")
        return cls(db_path)