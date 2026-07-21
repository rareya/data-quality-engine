# Data Quality Engine

A data quality profiling and validation tool for teams who need something between "eyeball the CSV in Excel" and a full enterprise platform like Informatica or Talend. Point it at a file, get back a quality score, a list of specific problems, and plain-English recommendations for fixing them — without writing a validation suite by hand.

Built with Python, FastAPI, and React.

## What it actually does

Most of the "checks" a data quality tool runs are individually simple — count nulls, find duplicates, flag outliers. Anyone can write that in a few lines of pandas. What this project does instead is automate the *decision* of which checks a given dataset needs, and package the results into something a non-technical stakeholder can read.

- **Smart Loader** — detects file format, encoding, and delimiter automatically. Handles CSV, TSV, JSON (both array and line-delimited), Excel, and several log formats (Apache combined, Apache CSV, Nginx, syslog). Falls back gracefully through a chain of parsing strategies rather than failing outright on a malformed file, and never silently drops rows — anything it can't parse cleanly gets flagged in the parse report, not discarded.
- **Schema inference** — works out semantic type per column (numeric, categorical, boolean, IP address, HTTP status, etc.), not just pandas dtype, and runs type-specific validation (e.g. HTTP status codes are checked against the valid 100–599 range).
- **Dynamic rule generation** — instead of a fixed checklist, rules are generated per dataset based on its actual schema and profile. A constant column gets flagged differently than a column with 40% missing values.
- **EDA module** — Pearson correlation, Shannon entropy, IQR-based outlier detection, skew/kurtosis, co-missing pattern analysis.
- **Natural-language summary** — auto-generated plain-English recommendations, so the output is usable by someone who isn't going to read a JSON blob.

## Getting started

```bash
git clone https://github.com/rareya/data-quality-engine.git
cd data-quality-engine
python -m venv venv
source venv/bin/activate   # venv\Scripts\activate on Windows
pip install -r requirements.txt
```

Run the pipeline directly:

```python
from backend.dq_engine.pipeline import DataQualityPipeline

report = DataQualityPipeline("your_data.csv").run()
print(report["quality_score"])
print(report["summary"])
```

Or run the API + frontend:

```bash
uvicorn backend.main:app --reload
cd frontend && npm install && npm run dev
```

## Testing

```bash
pytest tests/ -v
```

55 tests covering rules, scoring, schema detection, profiling, the smart loader, and full end-to-end pipeline runs. Runs on every push via GitHub Actions.

![Tests](https://github.com/rareya/data-quality-engine/actions/workflows/tests.yml/badge.svg)

## Benchmarks

Two separate validations, because "it runs without crashing" and "it gives correct answers" are different claims.

**Accuracy against known ground truth.** A synthetic dataset (2,040 rows) with deliberately injected, exactly-counted defects — 150 missing values, 40 duplicate rows, 1 constant column. The engine detected all three at 100% accuracy against the known counts.

**Validation against a real, independently-documented dataset.** The [UCI Online Retail dataset](https://archive.ics.uci.edu/ml/datasets/online+retail) (541,909 real transactions from a UK online retailer, cited in Chen, Sain & Guo, 2012, *Journal of Database Marketing and Customer Strategy Management*). This dataset's defects are already publicly documented elsewhere — ~24.9% missing `CustomerID`, ~0.27% missing `Description`, ~5,268 duplicate rows. The engine's output matched these independently-published figures.

**Comparison against Great Expectations**, on the same 541K-row dataset. This isn't a clean apples-to-apples speed test — the two tools aren't doing the same amount of work, and the numbers reflect that honestly rather than being cherry-picked:

| Metric | Data Quality Engine | Great Expectations |
|---|---|---|
| Setup code required | 2 lines | 18 lines |
| Checks run | 18 | 11 |
| Runtime | 10.7s | 1.6s |

Great Expectations was faster because it only ran 11 basic null/range checks in this comparison. In the same run, this engine also computed a full EDA report (correlation matrix, entropy, outlier detection, skew/kurtosis) and generated natural-language recommendations — none of which Great Expectations does out of the box. The honest takeaway isn't "faster," it's "less setup for more output." A stripped-down, EDA-skipping mode would likely close most of the runtime gap, but that's not what's shipped today.

## Known limitations

- Log format detection uses fairly strict regex patterns (particularly for the Apache CSV format) — a log line with unexpected leading whitespace or an unusual field order will fall through to the generic custom-log parser rather than the specific one.
- Header detection on tabular files uses a heuristic (numeric content in row 2 vs row 1) that can misfire on edge cases like ID columns that look numeric in both rows.
- Large-file intelligent sampling (first/middle/last 10%) is a reasonable heuristic for spotting quality drift across a file, but it's still a sample — it won't catch a rare defect confined to a small slice of the untouched 70%.
- Semantic type detection for log-specific fields (`ip`, `status`, `timestamp`) is currently based on exact column name matches rather than content inference — a column called `client_ip` won't get the same validation as one called `ip`.

## Project structure

```
backend/
  dq_engine/
    smart_loader.py      # file loading, format detection, parsing
    schema.py             # semantic type inference
    profiler.py            # statistical profiling
    rule_factory.py       # dynamic rule generation
    scorer.py              # weighted quality scoring
    eda.py                    # correlation, entropy, outliers
    recommendations.py  # natural language output
    report.py               # final report assembly
    pipeline.py            # orchestration
  main.py                    # FastAPI app
frontend/                    # React dashboard
tests/                          # pytest suite
benchmark/                # accuracy + real-data + GE comparison scripts
```

## License

MIT