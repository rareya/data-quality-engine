import pandas as pd

class DataProfiler:
    def __init__(self, dataframe: pd.DataFrame):
        self.df = dataframe

    def profile(self):
        profile = {
            "num_rows": len(self.df),
            "num_columns": len(self.df.columns),
            "columns": {}
        }

        for col in self.df.columns:
            profile["columns"][col] = {
                "dtype": str(self.df[col].dtype),
                "missing_count": int(self.df[col].isna().sum()),
                "unique_count": int(self.df[col].nunique()),
            }

            if pd.api.types.is_numeric_dtype(self.df[col]):
                profile["columns"][col].update({
                    "min": float(self.df[col].min()),
                    "max": float(self.df[col].max()),
                    "mean": float(self.df[col].mean())
                })

        return profile
