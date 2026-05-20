# Stage 1 — Foundations

A small Python pipeline that downloads UK House Price Index data and saves it as Parquet files, partitioned by year and month. Everything runs locally on your laptop. No AWS yet.

## The three source files

**`src/rx_platform/ingest.py`** is where all the actual work happens. About 100 lines. Three functions:

- `download_csv(url, destination)` — streams a URL to disk in 1 MB chunks.
- `read_and_partition(csv_path, raw_zone)` — reads a CSV with a Date column, groups the rows by year/month, writes one Parquet file per group.
- `ingest_hpi(url, raw_zone, use_sample)` — runs the whole pipeline. Has a `use_sample` flag so you can test it offline.

**`src/rx_platform/cli.py`** is a tiny wrapper that exposes one command-line action. About 30 lines. It's what makes `rx ingest` work.

**`tests/test_ingest.py`** uses the bundled `SAMPLE_CSV` to test the pipeline with no internet access. Three tests, no mocking.

## What to do first

After you've set up the venv and installed the package, run the sample mode:

```bash
rx ingest --sample
```

You should see output like:

```
Using bundled sample data (no download needed).
Reading data/raw/_tmp/uk-hpi.csv ...
Read 6 rows covering 2024-2024
Wrote 3 monthly partitions under data/raw/hpi/

Finished. 3 monthly partitions are in data/raw/hpi/
```

Then look at the files:

```bash
ls -R data/raw/hpi/
```

You'll see three folders (year=2024/month=01, /02, /03), each with one Parquet file. Open one in Python:

```python
import pandas as pd
df = pd.read_parquet("data/raw/hpi/year=2024/month=01/part-0000.parquet")
print(df)
```

Two rows. Real DataFrame. That is your data platform's storage layer in miniature.

## Then try the real data

When you're confident the sample mode works:

```bash
rx ingest
```

This downloads the real HM Land Registry HPI CSV (~3 MB). If the default URL has expired (it's pinned to a specific month), you'll get a 404. To fix:

1. Visit https://www.gov.uk/government/statistical-data-sets/uk-house-price-index-data-downloads
2. Click the most recent "UK House Price Index" CSV link.
3. Right-click → Copy link.
4. Pass it via `--url`:

```bash
rx ingest --url "https://publicdata.landregistry.gov.uk/.../UK-HPI-full-file-2025-03.csv"
```

When it succeeds you'll see dozens or hundreds of monthly partitions appear under `data/raw/hpi/` — every month from 1968 to whenever the file was published.

## Definition of done

- [ ] `pip install -e ".[dev]"` succeeds without errors.
- [ ] `rx --help` shows the `ingest` command.
- [ ] `rx ingest --sample` writes three partition files.
- [ ] `pytest` says "3 passed".
- [ ] `ruff check .` and `ruff format --check .` both pass.
- [ ] Code pushed to GitHub, CI is green on main.
- [ ] Tagged release `v0.1-foundations`.
