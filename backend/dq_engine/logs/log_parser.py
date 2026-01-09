import re
import pandas as pd
import os

LOG_PATTERN = re.compile(
    r'(?P<ip>[^,]+),[^,]*,[^,]*,'
    r'\[(?P<timestamp>[^\]]+)\],'
    r'"""(?P<method>\w+)",(?P<endpoint>[^,]+),"(?P<protocol>[^"]+)"""'
    r',(?P<status>\d{3}),(?P<bytes>[\d-]+),'
    r'""".*?""","""(?P<user_agent>.*?)"""'
)

class LogParser:
    def __init__(self, file_path):
        self.file_path = os.path.abspath(file_path)

    def parse(self):
        if not os.path.exists(self.file_path):
            raise FileNotFoundError(self.file_path)

        rows = []

        with open(self.file_path, "r", encoding="utf-8") as f:
            for line in f:
                match = LOG_PATTERN.search(line)
                if match:
                    row = match.groupdict()
                    row["bytes"] = None if row["bytes"] == "-" else int(row["bytes"])
                    rows.append(row)

        df = pd.DataFrame(rows)
        print(f"Parsed rows: {len(df)}")
        return df
