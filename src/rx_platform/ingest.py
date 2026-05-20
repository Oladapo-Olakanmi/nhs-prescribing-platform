"""UK House Price Index ingestion — the heart of Stage 1.

What this file does, top to bottom:
  1. Defines the default URL where HM Land Registry publishes the HPI CSV.
  2. Provides a small bundled "sample" CSV for offline learning and tests.
  3. download_csv()        — fetches a URL and saves it as a file.
  4. read_and_partition()  — reads a CSV and writes one Parquet file per
                             year/month, in the Hive-style directory
                             layout that Spark, Athena and Glue all expect.
  5. ingest_hpi()          — the end-to-end pipeline: download + partition.

Everything is plain Python with the standard data libraries. No magic.
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd
import requests

# ---------------------------------------------------------------------------
# Where the data comes from
# ---------------------------------------------------------------------------
#
# HM Land Registry publishes the UK House Price Index as a monthly CSV file.
# The "full-file" version contains the entire history (1968 to today), about
# 3 MB. The URL contains the publication month, which changes each time a
# new release comes out — if you get a 404, visit the URL on gov.uk and
# pick a more recent month, then pass it via:  rx ingest --url <new-url>
#
# Source page:
#   https://www.gov.uk/government/statistical-data-sets/uk-house-price-index-data-downloads
DEFAULT_URL = (
    "https://publicdata.landregistry.gov.uk/market-trend-data/"
    "house-price-index-data/UK-HPI-full-file-2024-04.csv"
)


# ---------------------------------------------------------------------------
# A tiny sample dataset bundled inside the code.
#
# This lets you run `rx ingest --sample` without any internet connection.
# It's the same shape as the real HPI CSV but with only six rows, so you
# can inspect it easily while you learn what each function does.
# ---------------------------------------------------------------------------
SAMPLE_CSV = """Date,RegionName,AreaCode,AveragePrice,Index,AnnualChange
01/01/2024,United Kingdom,K02000001,285000,148.5,1.2
01/01/2024,England,E92000001,295000,150.1,1.0
01/02/2024,United Kingdom,K02000001,287000,149.2,1.5
01/02/2024,England,E92000001,297000,151.0,1.2
01/03/2024,United Kingdom,K02000001,289000,150.0,1.7
01/03/2024,England,E92000001,300000,152.1,1.4
"""


# ---------------------------------------------------------------------------
# Step 1: download a CSV
# ---------------------------------------------------------------------------
def download_csv(url: str, destination: Path) -> Path:
    """Download a CSV file from `url` and save it to `destination`.

    We stream the response so that very large files don't all sit in
    memory at once. Each iteration writes one chunk (1 MB) to disk.
    """
    print(f"Downloading {url} ...")
    # Make sure the folder we're saving into exists.
    destination.parent.mkdir(parents=True, exist_ok=True)

    # `stream=True` tells requests not to download the whole body at once.
    # `timeout=60` gives up if the server doesn't respond in 60 seconds.
    response = requests.get(url, stream=True, timeout=60)
    response.raise_for_status()  # turn HTTP errors (404, 500, ...) into exceptions

    bytes_written = 0
    with destination.open("wb") as f:
        # iter_content yields the body in chunks. 1 MB chunks are a sensible default.
        for chunk in response.iter_content(chunk_size=1024 * 1024):
            f.write(chunk)
            bytes_written += len(chunk)

    print(f"Downloaded {bytes_written:,} bytes to {destination}")
    return destination


# ---------------------------------------------------------------------------
# Step 2: read the CSV and split it into monthly Parquet files
# ---------------------------------------------------------------------------
def read_and_partition(csv_path: Path, raw_zone: Path) -> list[Path]:
    """Read an HPI CSV and write one Parquet file per (year, month).

    The output layout is "Hive-style":
        raw_zone/hpi/year=YYYY/month=MM/part-0000.parquet

    This layout matters because Spark, Athena and AWS Glue all understand
    it natively — they can read the partition values straight out of the
    folder names without needing to scan the data.
    """
    print(f"Reading {csv_path} ...")

    # Read the whole CSV into memory. UK HPI is only ~3 MB, so this is fine.
    # For larger files we'd use the `chunksize=` argument and process in chunks.
    df = pd.read_csv(csv_path)

    # The Date column is in UK format: DD/MM/YYYY. Convert it to a real
    # datetime so we can extract year and month from it.
    df["Date"] = pd.to_datetime(df["Date"], format="%d/%m/%Y")
    df["year"] = df["Date"].dt.year
    df["month"] = df["Date"].dt.month

    print(f"Read {len(df):,} rows covering {df['year'].min()}-{df['year'].max()}")

    # Group the DataFrame by (year, month) and write each group to its own
    # Parquet file under the partition directory.
    written: list[Path] = []
    for (year, month), group in df.groupby(["year", "month"]):
        partition_dir = raw_zone / "hpi" / f"year={year:04d}" / f"month={month:02d}"
        partition_dir.mkdir(parents=True, exist_ok=True)
        out_path = partition_dir / "part-0000.parquet"

        # Drop the helper "year" and "month" columns before writing —
        # they're encoded in the folder names already, so storing them
        # in the file too would just waste space.
        group.drop(columns=["year", "month"]).to_parquet(out_path, index=False)
        written.append(out_path)

    print(f"Wrote {len(written)} monthly partitions under {raw_zone}/hpi/")
    return written


# ---------------------------------------------------------------------------
# Step 3: the whole pipeline in one function
# ---------------------------------------------------------------------------
def ingest_hpi(
    url: str = DEFAULT_URL,
    raw_zone: Path = Path("data/raw"),
    *,
    use_sample: bool = False,
) -> list[Path]:
    """Run the full pipeline: download (or load sample) then partition.

    Parameters
    ----------
    url:
        Where to download the HPI CSV from. Ignored if `use_sample` is True.
    raw_zone:
        Local folder to write the Parquet partitions under.
    use_sample:
        If True, skip the download and use the bundled SAMPLE_CSV instead.
        Handy for offline learning and for the test suite.
    """
    # Use a hidden _tmp folder for the intermediate CSV.
    workdir = raw_zone / "_tmp"
    workdir.mkdir(parents=True, exist_ok=True)
    csv_path = workdir / "uk-hpi.csv"

    if use_sample:
        print("Using bundled sample data (no download needed).")
        csv_path.write_text(SAMPLE_CSV)
    else:
        download_csv(url, csv_path)

    partitions = read_and_partition(csv_path, raw_zone)

    # Tidy up: remove the intermediate CSV now that we have the Parquet files.
    csv_path.unlink(missing_ok=True)

    return partitions
