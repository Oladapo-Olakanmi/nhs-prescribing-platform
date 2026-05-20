# nhs-prescribing-platform

A UK Housing Data Platform — built in stages, each one a tagged release. A portfolio project for transitioning from DBA to Data Platform Engineer.

> **About the name.** The project started out targeting NHS prescription data, but the dataset turned out to be too large (6 GB+ per month) for a comfortable learning experience. We switched to the UK House Price Index, which gives us the same engineering shape (monthly partitions, dimensional modelling, lakehouse architecture) but in a 3 MB file. The repo name kept its original branding to save re-doing the GitHub setup.

## What it does today (Stage 1)

Downloads the UK House Price Index data from HM Land Registry, parses the monthly rows, and lands a partitioned Parquet file for each month under `data/raw/hpi/year=YYYY/month=MM/`.

Three Python files. Roughly 150 lines of code total. You can read every line and understand what it does.

## Quick start

```bash
# Set up a virtual environment
python3.12 -m venv .venv
source .venv/bin/activate

# Install the package and its dev tools
pip install -e ".[dev]"

# Run with the bundled sample data (no internet needed)
rx ingest --sample

# Or fetch the live HPI data from HM Land Registry
rx ingest

# Look at what was produced
ls data/raw/hpi/
```

## How to read this codebase

There are only three source files. Read them in this order:

1. **`src/rx_platform/ingest.py`** — does the actual work. Has three functions: `download_csv` (saves a URL to disk), `read_and_partition` (splits a CSV into monthly Parquet files), and `ingest_hpi` (runs the whole pipeline).

2. **`src/rx_platform/cli.py`** — wraps the ingest function in a `rx` command-line tool.

3. **`tests/test_ingest.py`** — three small tests that show what each function expects.

That's the whole project. No frameworks, no abstractions, no magic. The complexity gets added gradually in later stages.

## The build plan

| Stage | What we add | Tag |
|------:|------|-----|
| 1 | Local Python pipeline: download + partition | `v0.1-foundations` |
| 2 | AWS S3 storage, provisioned with Terraform | `v0.2-aws-foundations` |
| 3 | PySpark transformation jobs, Glue Catalog, Athena | `v0.3-spark-lakehouse` |
| 4 | dbt models, dimensional schema, GitHub Pages docs | `v0.4-dbt-marts` |
| 5 | Airflow orchestration, scheduled monthly runs | `v0.5-orchestration` |
| 6 | Observability, cost dashboards, polish | `v1.0-platform` |

Each stage is small and additive. By Stage 6 this is a real data platform — but the shape was already there in Stage 1.

## Data source

UK House Price Index, published monthly by HM Land Registry. CSV format, about 3 MB per month covering all UK regions. Free to use under the Open Government Licence v3.0.

Source page: https://www.gov.uk/government/statistical-data-sets/uk-house-price-index-data-downloads

## License

MIT.
