"""
smart_loader.py
---------------
Intelligent file loader that automatically detects and handles:

1. File format detection (CSV, TSV, PSV, Log files, JSON logs)
2. Delimiter sniffing using csv.Sniffer
3. Encoding detection using chardet
4. Mid-file encoding error handling (keep rows, replace with NaN, flag in report)
5. Large file handling with intelligent sampling (first/mid/last 10%)
6. Log format differentiation (Apache, Nginx, JSON, Syslog, Custom)
7. Parse confidence scoring
8. Fallback chain
9. File validation before parsing
10. Data freshness check

Industry standard: Never silently drop data. Always flag, never hide.
"""

import os
import re
import csv
import json
import chardet
import pandas as pd
import numpy as np
from datetime import datetime, timezone


# ── Constants ─────────────────────────────────────────────────────────────────

MAX_FULL_LOAD_BYTES   = 50  * 1024 * 1024   # 50MB  → full load
MAX_SAMPLE_BYTES      = 500 * 1024 * 1024   # 500MB → intelligent sampling
SAMPLE_LINES          = 20                   # lines used for sniffing
MIN_CONFIDENCE        = 0.60                 # below this → warn user
MAX_COLUMNS           = 500                  # safety limit
MAX_FILE_SIZE_BYTES   = 2 * 1024 * 1024 * 1024  # 2GB hard limit

#_______________________________________-
JUNK_LINE_PATTERNS = [
    re.compile(r'^[=\-\*\.]{3,}'),
    re.compile(r'==>[^,]*,.*,<=='),
    re.compile(r'^#+\s'),
    re.compile(r'^\[.*\]$'),
    re.compile(r'^log\s+file', re.IGNORECASE),
]

def is_junk_line(line: str) -> bool:
    clean = line.strip().strip('"').strip()
    print(f"JUNK CHECK: {repr(clean[:150])}")
    if not clean:
        return True
    for pattern in JUNK_LINE_PATTERNS:
        if pattern.search(clean):
            return True
    return False

def sanitize_line(line: str) -> str:
    """
    Strips outer Excel-added quotes and normalizes internal quoting.
    """
    stripped = line.strip()
    
    # Keep unwrapping outer quotes until stable
    prev = None
    while prev != stripped:
        prev = stripped
        if stripped.startswith('"') and stripped.endswith('"'):
            inner = stripped[1:-1]
            if any(x in inner for x in [',-,-,', 'HTTP', 'GET', 'POST', 'HEAD', 'STATUS']):
                stripped = inner
            else:
                break
    
    # Normalize ALL quote runs down to single quote
    # Then our regex can match cleanly
    stripped = re.sub(r'"{2,}', '"', stripped)
    
    return stripped



# ── Log format patterns ───────────────────────────────────────────────────────

LOG_FORMATS = {
    "apache_combined": {
        "pattern": re.compile(
            r'(?P<ip>\S+)\s+\S+\s+\S+\s+'
            r'\[(?P<timestamp>[^\]]+)\]\s+'
            r'"(?P<method>\S+)\s+(?P<endpoint>\S+)\s+\S+"\s+'
            r'(?P<status>\d{3})\s+'
            r'(?P<size>\S+)'
            r'(?:\s+"(?P<referrer>[^"]*)"\s+"(?P<user_agent>[^"]*)")?'
        ),
        "columns": ["ip", "timestamp", "method", "endpoint", "status", "size", "referrer", "user_agent"],
        "description": "Apache Combined Log Format"
    },

    "nginx_access": {
        "pattern": re.compile(
            r'(?P<ip>\S+)\s+-\s+-\s+'
            r'\[(?P<timestamp>[^\]]+)\]\s+'
            r'"(?P<method>\S+)\s+(?P<endpoint>\S+)\s+\S+"\s+'
            r'(?P<status>\d{3})\s+'
            r'(?P<size>\d+)\s+'
            r'"(?P<referrer>[^"]*)"\s+'
            r'"(?P<user_agent>[^"]*)"'
        ),
        "columns": ["ip", "timestamp", "method", "endpoint", "status", "size", "referrer", "user_agent"],
        "description": "Nginx Access Log Format"
    },

    "apache_csv": {
    "pattern": re.compile(
        r'(?P<ip>[a-fA-F0-9\.\:]+)'
        r',-,-,'
        r'\[(?P<timestamp>[^\]]+)\],'
        r'"(?P<method>\w+)",'
        r'(?P<endpoint>[^,]+),'
        r'[^,]+,'
        r'(?P<status>\d{3}),'
        r'(?P<size>[\d\-]+),'
        r'[^,]+,'
        r'"(?P<agent>[^"]+)'
    ),
    "columns": ["ip", "timestamp", "method", "endpoint", "status", "size", "agent"],
    "description": "Apache CSV Log Format (comma-separated with quoted fields)"
},

    "syslog": {
        "pattern": re.compile(
            r'(?P<timestamp>\w+\s+\d+\s+\d+:\d+:\d+)\s+'
            r'(?P<hostname>\S+)\s+'
            r'(?P<service>\S+?)(?:\[(?P<pid>\d+)\])?:\s+'
            r'(?P<message>.+)'
        ),
        "columns": ["timestamp", "hostname", "service", "pid", "message"],
        "description": "Syslog Format"
    },

    "json_log": {
        "pattern": None,   # handled separately
        "columns": [],
        "description": "JSON Log Format (one JSON object per line)"
    },

    "custom": {
        "pattern": None,   # best effort
        "columns": [],
        "description": "Custom/Unknown Log Format"
    }
}


# ── Main Smart Loader ─────────────────────────────────────────────────────────

class SmartLoader:
    """
    Intelligent file loader.
    
    Usage:
        loader = SmartLoader("path/to/file.csv")
        df, parse_report = loader.load()
    """

    def __init__(self, file_path: str):
        self.file_path = os.path.abspath(file_path)
        self.parse_report = {
            "file_path": self.file_path,
            "file_size_mb": 0,
            "file_type": None,
            "encoding": None,
            "encoding_confidence": None,
            "delimiter": None,
            "headers_detected": None,
            "log_format": None,
            "parse_confidence": None,
            "load_strategy": None,
            "rows_loaded": 0,
            "rows_sampled": None,
            "encoding_error_rows": 0,
            "corrupted_row_indices": [],
            "warnings": [],
            "freshness": None,
            "fallback_used": False
        }

    def load(self) -> tuple[pd.DataFrame, dict]:
        """
        Main entry point.
        Returns (DataFrame, parse_report)
        """

        # ── Step 1: Validate file ─────────────────────────────────────────
        self._validate_file()

        # ── Step 2: Detect encoding ───────────────────────────────────────
        encoding = self._detect_encoding()

        # ── Step 3: Read raw sample lines ────────────────────────────────
        raw_lines = self._read_raw_lines(encoding)

        # ── Step 4: Classify file type ────────────────────────────────────
        file_type = self._classify_file(raw_lines)
        self.parse_report["file_type"] = file_type

        # ── Step 5: Parse based on type ───────────────────────────────────
        if file_type == "log":
            df = self._parse_log(raw_lines, encoding)
        else:
            df = self._parse_tabular(raw_lines, encoding)

        # ── Step 6: Large file handling ───────────────────────────────────
        file_size = os.path.getsize(self.file_path)
        self.parse_report["file_size_mb"] = round(file_size / 1024 / 1024, 2)

        if file_size > MAX_FULL_LOAD_BYTES and file_type != "log":
            df = self._intelligent_sample(encoding)
            self.parse_report["load_strategy"] = "intelligent_sampling"
        else:
            self.parse_report["load_strategy"] = "full_load"

        # ── Step 7: Data freshness check ──────────────────────────────────
        self._check_freshness(df)

        # ── Step 8: Final validation ──────────────────────────────────────
        self._validate_parse(df)

        self.parse_report["rows_loaded"] = len(df)

        return df, self.parse_report


    # ── File validation ───────────────────────────────────────────────────────

    def _validate_file(self):
        if not os.path.exists(self.file_path):
            raise FileNotFoundError(f"File not found: {self.file_path}")

        file_size = os.path.getsize(self.file_path)

        if file_size == 0:
            raise ValueError("File is empty.")

        if file_size > MAX_FILE_SIZE_BYTES:
            raise ValueError(
                f"File too large ({round(file_size/1024/1024/1024, 2)}GB). "
                f"Maximum supported size is 2GB."
            )

        ext = os.path.splitext(self.file_path)[1].lower()
        allowed = {".csv", ".tsv", ".txt", ".log", ".json", ".xlsx", ".xls"}
        if ext not in allowed:
            raise ValueError(
                f"Unsupported file extension '{ext}'. "
                f"Allowed: {', '.join(allowed)}"
            )


    # ── Encoding detection ────────────────────────────────────────────────────

    def _detect_encoding(self) -> str:
        """
        Uses chardet to detect file encoding.
        Falls back to utf-8 if detection fails.
        """
        try:
            with open(self.file_path, "rb") as f:
                raw = f.read(50000)   # read first 50KB for detection

            result = chardet.detect(raw)
            encoding = result.get("encoding") or "utf-8"
            confidence = result.get("confidence", 0)

            self.parse_report["encoding"] = encoding
            self.parse_report["encoding_confidence"] = round(confidence, 2)

            if confidence < 0.7:
                self.parse_report["warnings"].append(
                    f"Encoding detection confidence is low ({round(confidence*100)}%). "
                    f"Detected as {encoding}. Consider specifying encoding manually."
                )

            return encoding

        except Exception:
            self.parse_report["encoding"] = "utf-8"
            self.parse_report["warnings"].append(
                "Encoding detection failed. Defaulting to UTF-8."
            )
            return "utf-8"


    # ── Read raw lines ────────────────────────────────────────────────────────

    def _read_raw_lines(self, encoding: str) -> list[str]:
        """
        Reads first SAMPLE_LINES lines for sniffing.
        Handles mid-file encoding errors gracefully.
        """
        lines = []
        try:
            with open(self.file_path, "r", encoding=encoding, errors="replace") as f:
                for i, line in enumerate(f):
                    if i >= SAMPLE_LINES:
                        break
                    raw = line.rstrip("\r\n")
                    if is_junk_line(raw):
                        continue
                    clean = sanitize_line(raw)
                    if clean:
                        lines.append(clean)
                    if len(lines) >= SAMPLE_LINES:
                        break
        except Exception as e:
            self.parse_report["warnings"].append(
                f"Error reading raw lines: {e}. Falling back to UTF-8."
            )
            with open(self.file_path, "r", encoding="utf-8", errors="replace") as f:
                for i, line in enumerate(f):
                    if i >= SAMPLE_LINES:
                        break
                    raw = line.rstrip("\r\n")
                    if is_junk_line(raw):
                        continue
                    clean = sanitize_line(raw)
                    if clean:
                        lines.append(clean)
                    if len(lines) >= SAMPLE_LINES:
                        break

        return lines


    # ── File type classification ──────────────────────────────────────────────

    def _classify_file(self, raw_lines: list[str]) -> str:
        """
        Classifies file as:
        - "tabular"  → CSV, TSV, PSV etc
        - "log"      → any log format
        - "json"     → JSON lines
        """
        ext = os.path.splitext(self.file_path)[1].lower()

        # Excel files
        if ext in {".xlsx", ".xls"}:
            return "excel"

        # Check for JSON lines
        json_score = sum(
            1 for line in raw_lines[:10]
            if line.strip().startswith("{") and line.strip().endswith("}")
        )
        if json_score >= len(raw_lines[:10]) * 0.7:
            return "json_log"

        # Check for log patterns
        log_score = self._score_log_likelihood(raw_lines)
        if log_score > 0.6:
            return "log"

        return "tabular"


    def _score_log_likelihood(self, lines: list[str]) -> float:
        """
        Scores how likely the file is a log file (0-1).
        Looks for common log indicators.
        """
        indicators = [
            re.compile(r'\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}'),  # IP address
            re.compile(r'\[\d{2}/\w+/\d{4}'),                     # Apache timestamp
            re.compile(r'"(GET|POST|PUT|DELETE|HEAD|OPTIONS)'),    # HTTP method
            re.compile(r'\b(ERROR|WARN|INFO|DEBUG|CRITICAL)\b'),   # Log levels
            re.compile(r'\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}'),  # ISO timestamp
            re.compile(r'\b(GET|POST|PUT|DELETE)\s+/\S+'),
            re.compile(r',-,-,'),        # Apache CSV separator
            re.compile(r',\d{3},'),      # HTTP status in CSV log
            re.compile(r'[a-f0-9]{4}:[a-f0-9]{4}:'),  # IPv6        # HTTP method + path
        ]

        scores = []
        for line in lines[:10]:
            matches = sum(1 for ind in indicators if ind.search(line))
            scores.append(min(matches / 3, 1.0))

        return sum(scores) / len(scores) if scores else 0.0


    # ── Tabular parser ────────────────────────────────────────────────────────

    def _parse_tabular(self, raw_lines: list[str], encoding: str) -> pd.DataFrame:
        """
        Parses CSV/TSV/PSV files with intelligent delimiter detection.
        Handles encoding errors row by row.
        """
        ext = os.path.splitext(self.file_path)[1].lower()

        # Excel files
        if ext in {".xlsx", ".xls"}:
            return pd.read_excel(self.file_path)

        # Detect delimiter
        delimiter = self._detect_delimiter(raw_lines)
        self.parse_report["delimiter"] = repr(delimiter)

        # Detect headers
        has_headers = self._detect_headers(raw_lines, delimiter)
        self.parse_report["headers_detected"] = has_headers

        # Read with encoding error handling
        try:
            df = pd.read_csv(
                self.file_path,
                sep=delimiter,
                header=0 if has_headers else None,
                encoding=encoding,
                encoding_errors="replace",
                on_bad_lines="warn",
                low_memory=False
            )

            # Auto generate column names if no headers
            if not has_headers:
                df.columns = [f"col_{i+1}" for i in range(len(df.columns))]
                self.parse_report["warnings"].append(
                    "No headers detected — column names auto-generated as col_1, col_2, ..."
                )

            # Check column count
            if len(df.columns) > MAX_COLUMNS:
                raise ValueError(
                    f"File has {len(df.columns)} columns which exceeds "
                    f"the maximum limit of {MAX_COLUMNS}."
                )

            # Detect and report encoding errors
            self._detect_encoding_errors(df)

            self.parse_report["parse_confidence"] = "high"
            return df

        except Exception as e:
            # Fallback chain
            return self._tabular_fallback(encoding, e)


    def _detect_delimiter(self, raw_lines: list[str]) -> str:
        """
        Uses csv.Sniffer first then falls back to manual detection.
        """
        sample = "\n".join(raw_lines[:10])

        # Try csv.Sniffer first (industry standard)
        try:
            dialect = csv.Sniffer().sniff(sample, delimiters=",\t|;")
            return dialect.delimiter
        except Exception:
            pass

        # Manual fallback — count occurrences of common delimiters
        delimiters = {
            ",": sample.count(","),
            "\t": sample.count("\t"),
            "|": sample.count("|"),
            ";": sample.count(";"),
        }
        best = max(delimiters, key=delimiters.get)

        # If no clear winner — default to comma
        if delimiters[best] == 0:
            self.parse_report["warnings"].append(
                "Could not detect delimiter clearly. Defaulting to comma."
            )
            return ","

        return best


    def _detect_headers(self, raw_lines: list[str], delimiter: str) -> bool:
        """
        Detects if first row is a header row.
        Logic: if first row is mostly strings and second row has numbers → headers exist.
        """
        if len(raw_lines) < 2:
            return True

        try:
            first_row = raw_lines[0].split(delimiter)
            second_row = raw_lines[1].split(delimiter)

            # Count numeric values in each row
            def numeric_count(row):
                return sum(1 for val in row if val.strip().replace(".", "").replace("-", "").isdigit())

            first_numeric = numeric_count(first_row)
            second_numeric = numeric_count(second_row)

            # If second row has more numbers than first → first is header
            if second_numeric > first_numeric:
                return True

            # If first row values look like column names (no spaces, short)
            header_like = sum(
                1 for val in first_row
                if val.strip() and len(val.strip()) < 30
                and not val.strip().replace(".", "").replace("-", "").isdigit()
            )
            if header_like > len(first_row) * 0.6:
                return True

            return False

        except Exception:
            return True


    # ── Log parser ────────────────────────────────────────────────────────────

    def _parse_log(self, raw_lines: list[str], encoding: str) -> pd.DataFrame:
        """
        Detects which log format and parses accordingly.
        """

        # Detect log format
        log_format = self._detect_log_format(raw_lines)
        self.parse_report["log_format"] = log_format

        if log_format == "json_log":
            return self._parse_json_log(encoding)

        if log_format in LOG_FORMATS and LOG_FORMATS[log_format]["pattern"]:
            return self._parse_regex_log(
                LOG_FORMATS[log_format]["pattern"],
                LOG_FORMATS[log_format]["columns"],
                encoding
            )

        # Custom/unknown — best effort
        return self._parse_custom_log(raw_lines, encoding)


    def _detect_log_format(self, raw_lines: list[str]) -> str:
        """
        Tries each log format and returns the best match.
        """
        scores = {}

        for fmt_name, fmt in LOG_FORMATS.items():
            if fmt_name == "json_log":
                score = sum(
                    1 for line in raw_lines[:10]
                    if line.strip().startswith("{")
                )
                scores[fmt_name] = score / max(len(raw_lines[:10]), 1)
                continue

            if fmt["pattern"] is None:
                scores[fmt_name] = 0
                continue

            matches = sum(
                1 for line in raw_lines[:10]
                if fmt["pattern"].search(line)
            )
            scores[fmt_name] = matches / max(len(raw_lines[:10]), 1)

        best_format = max(scores, key=scores.get)
        best_score = scores[best_format]

        if best_score < MIN_CONFIDENCE:
            self.parse_report["warnings"].append(
                f"Log format detection confidence is low ({round(best_score*100)}%). "
                f"Falling back to custom log parser."
            )
            return "custom"

        self.parse_report["parse_confidence"] = f"{round(best_score*100)}%"
        return best_format


    def _parse_regex_log(self, pattern, columns: list, encoding: str) -> pd.DataFrame:
        """
        Parses log file using a regex pattern.
        Handles encoding errors line by line.
        """
        records = []
        error_rows = []

        with open(self.file_path, "r", encoding=encoding, errors="replace") as f:
            for i, line in enumerate(f):
                if is_junk_line(line):
                    continue
                line = sanitize_line(line)
                if not line:
                    continue

                match = pattern.search(line)
                if match:
                    records.append(match.groupdict())
                else:
                    # Line doesn't match — flag it
                    error_rows.append(i)
                    # Try to extract whatever we can
                    records.append({col: None for col in columns})

        if error_rows:
            self.parse_report["warnings"].append(
                f"{len(error_rows)} log lines did not match the detected format "
                f"and were recorded as NaN."
            )
            self.parse_report["corrupted_row_indices"] = error_rows[:20]

        df = pd.DataFrame(records)

        # Convert status to numeric if present
        if "status" in df.columns:
            df["status"] = pd.to_numeric(df["status"], errors="coerce")

        # Convert size to numeric if present
        if "size" in df.columns:
            df["size"] = pd.to_numeric(df["size"], errors="coerce")

        return df


    def _parse_json_log(self, encoding: str) -> pd.DataFrame:
        """
        Parses JSON log files (one JSON object per line).
        """
        records = []
        error_rows = []

        with open(self.file_path, "r", encoding=encoding, errors="replace") as f:
            for i, line in enumerate(f):
                line = line.strip()
                if not line:
                    continue
                try:
                    records.append(json.loads(line))
                except json.JSONDecodeError:
                    error_rows.append(i)

        if error_rows:
            self.parse_report["warnings"].append(
                f"{len(error_rows)} JSON lines could not be parsed and were skipped."
            )

        return pd.DataFrame(records)


    def _parse_custom_log(self, raw_lines: list[str], encoding: str) -> pd.DataFrame:
        """
        Best effort parser for unknown log formats.
        Tries space/tab splitting and assigns generic column names.
        """
        self.parse_report["warnings"].append(
            "Unknown log format detected. Using best-effort parser. "
            "Results may need manual review."
        )
        self.parse_report["parse_confidence"] = "low"
        self.parse_report["fallback_used"] = True

        records = []
        with open(self.file_path, "r", encoding=encoding, errors="replace") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                parts = line.split()
                records.append(parts)

        if not records:
            raise ValueError("Could not parse any data from the file.")

        max_cols = max(len(r) for r in records)
        df = pd.DataFrame(
            records,
            columns=[f"field_{i+1}" for i in range(max_cols)]
        )

        return df


    # ── Intelligent sampling ──────────────────────────────────────────────────

    def _intelligent_sample(self, encoding: str) -> pd.DataFrame:
        """
        For large files — samples first 10%, middle 10%, last 10%.
        Much better than random sampling for quality detection.
        """
        self.parse_report["warnings"].append(
            f"File is large ({self.parse_report['file_size_mb']}MB). "
            f"Using intelligent sampling (first/middle/last 10% of rows)."
        )

        # Count total rows first
        with open(self.file_path, "r", encoding=encoding, errors="replace") as f:
            total_rows = sum(1 for _ in f) - 1  # subtract header

        sample_size = max(int(total_rows * 0.10), 1000)

        # First 10%
        df_first = pd.read_csv(
            self.file_path,
            encoding=encoding,
            encoding_errors="replace",
            nrows=sample_size
        )

        # Middle 10%
        mid_start = max(total_rows // 2 - sample_size // 2, sample_size)
        df_mid = pd.read_csv(
            self.file_path,
            encoding=encoding,
            encoding_errors="replace",
            skiprows=range(1, mid_start),
            nrows=sample_size
        )
        df_mid.columns = df_first.columns

        # Last 10%
        last_start = max(total_rows - sample_size, mid_start + sample_size)
        df_last = pd.read_csv(
            self.file_path,
            encoding=encoding,
            encoding_errors="replace",
            skiprows=range(1, last_start),
            nrows=sample_size
        )
        df_last.columns = df_first.columns

        df = pd.concat([df_first, df_mid, df_last], ignore_index=True)
        df = df.drop_duplicates()

        self.parse_report["rows_sampled"] = len(df)
        self.parse_report["warnings"].append(
            f"Analysis based on {len(df):,} sampled rows out of "
            f"{total_rows:,} total rows ({round(len(df)/total_rows*100, 1)}% sample)."
        )

        return df


    # ── Encoding error detection ──────────────────────────────────────────────

    def _detect_encoding_errors(self, df: pd.DataFrame):
        """
        Detects cells that contain replacement characters (?)
        which indicate encoding errors.
        Industry standard: flag but never drop.
        """
        error_count = 0
        error_indices = []

        for col in df.select_dtypes(include=["object"]).columns:
            mask = df[col].astype(str).str.contains("\ufffd", na=False)
            count = mask.sum()
            if count > 0:
                error_count += count
                error_indices.extend(df.index[mask].tolist()[:5])
                # Replace with NaN — industry standard
                df.loc[mask, col] = np.nan

        if error_count > 0:
            self.parse_report["encoding_error_rows"] = error_count
            self.parse_report["corrupted_row_indices"] = list(set(error_indices))[:20]
            self.parse_report["warnings"].append(
                f"{error_count} cells had encoding errors. "
                f"Values replaced with NaN (industry standard — data preserved, errors flagged)."
            )


    # ── Data freshness check ──────────────────────────────────────────────────

    def _check_freshness(self, df: pd.DataFrame):
        """
        Checks how recent the data is using any timestamp columns.
        Flags stale data as a quality concern.
        """
        timestamp_cols = [
            col for col in df.columns
            if any(keyword in col.lower() for keyword in
                   ["timestamp", "date", "time", "created", "updated", "modified"])
        ]

        for col in timestamp_cols:
            try:
                import warnings
                with warnings.catch_warnings():
                    warnings.simplefilter("ignore")
                    parsed = pd.to_datetime(df[col], errors="coerce")
                valid = parsed.dropna()

                if valid.empty:
                    continue

                # Make timezone aware for comparison
                most_recent = valid.max()
                if most_recent.tzinfo is None:
                    most_recent = most_recent.replace(tzinfo=timezone.utc)

                now = datetime.now(timezone.utc)
                age_days = (now - most_recent).days

                freshness = {
                    "column": col,
                    "most_recent_record": str(most_recent.date()),
                    "age_days": age_days,
                    "status": (
                        "FRESH" if age_days <= 7
                        else "STALE" if age_days <= 30
                        else "VERY_STALE"
                    )
                }

                self.parse_report["freshness"] = freshness

                if age_days > 30:
                    self.parse_report["warnings"].append(
                        f"Data freshness warning: Most recent record in '{col}' "
                        f"is {age_days} days old. Data may be stale."
                    )

                break  # Only check first valid timestamp column

            except Exception:
                continue


    # ── Parse validation ──────────────────────────────────────────────────────

    def _validate_parse(self, df: pd.DataFrame):
        """
        Final sanity check on parsed DataFrame.
        """
        if df is None or df.empty:
            raise ValueError("Parsing produced an empty dataset.")

        if len(df.columns) < 2:
            self.parse_report["warnings"].append(
                "Only 1 column detected — file may not have parsed correctly. "
                "Check delimiter or file format."
            )
            self.parse_report["parse_confidence"] = "low"

        if len(df) < 5:
            self.parse_report["warnings"].append(
                f"Only {len(df)} rows loaded — dataset is very small."
            )


    # ── Tabular fallback chain ────────────────────────────────────────────────

    def _tabular_fallback(self, encoding: str, original_error: Exception) -> pd.DataFrame:
        """
        Fallback chain when primary parsing fails.
        Tries different approaches one by one.
        """
        self.parse_report["fallback_used"] = True
        self.parse_report["parse_confidence"] = "medium"

        fallback_configs = [
            {"sep": ",",  "encoding": "utf-8"},
            {"sep": "\t", "encoding": "utf-8"},
            {"sep": ";",  "encoding": "utf-8"},
            {"sep": ",",  "encoding": "latin-1"},
            {"sep": ",",  "encoding": "cp1252"},
        ]

        for config in fallback_configs:
            try:
                df = pd.read_csv(
                    self.file_path,
                    sep=config["sep"],
                    encoding=config["encoding"],
                    encoding_errors="replace",
                    on_bad_lines="skip"
                )
                self.parse_report["warnings"].append(
                    f"Primary parsing failed. "
                    f"Successfully parsed using fallback: "
                    f"delimiter='{config['sep']}', encoding='{config['encoding']}'."
                )
                self.parse_report["delimiter"] = repr(config["sep"])
                self.parse_report["encoding"] = config["encoding"]
                return df

            except Exception:
                continue

        # All fallbacks failed
        raise RuntimeError(
            f"All parsing attempts failed. Original error: {original_error}. "
            f"Please check the file format and try again."
        )