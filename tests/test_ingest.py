"""Tests for rx_platform.ingest.

These tests use the bundled sample CSV (SAMPLE_CSV) so they run
without any internet access. Run them with:    pytest

What each test checks:
  - test_read_and_partition_creates_files: the right partition folders appear.
  - test_partition_contains_expected_rows:  the data inside is correct.
  - test_ingest_hpi_with_sample_runs_clean: the whole pipeline works end-to-end.
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd

from rx_platform.ingest import SAMPLE_CSV, ingest_hpi, read_and_partition


def test_read_and_partition_creates_files(tmp_path: Path) -> None:
    """After running read_and_partition we should get one file per month."""
    # Arrange: write the sample CSV into a temp folder.
    csv = tmp_path / "sample.csv"
    csv.write_text(SAMPLE_CSV)
    raw_zone = tmp_path / "raw"

    # Act: run the function we're testing.
    paths = read_and_partition(csv, raw_zone)

    # Assert: the sample has three months (Jan, Feb, Mar 2024).
    assert len(paths) == 3

    # The Parquet files should be at the Hive-style partition paths.
    for month in ("01", "02", "03"):
        expected = raw_zone / "hpi" / "year=2024" / f"month={month}" / "part-0000.parquet"
        assert expected.exists(), f"Missing partition file {expected}"


def test_partition_contains_expected_rows(tmp_path: Path) -> None:
    """The Parquet file for January should have the two January rows."""
    csv = tmp_path / "sample.csv"
    csv.write_text(SAMPLE_CSV)
    raw_zone = tmp_path / "raw"

    read_and_partition(csv, raw_zone)

    january = pd.read_parquet(raw_zone / "hpi" / "year=2024" / "month=01" / "part-0000.parquet")

    # Two rows in January: United Kingdom and England.
    assert len(january) == 2
    assert "United Kingdom" in january["RegionName"].values
    assert "England" in january["RegionName"].values

    # The "year" and "month" helper columns should NOT be in the output —
    # we drop them because they're already encoded in the folder names.
    assert "year" not in january.columns
    assert "month" not in january.columns


def test_ingest_hpi_with_sample_runs_clean(tmp_path: Path) -> None:
    """The full pipeline works end-to-end using the bundled sample data."""
    raw_zone = tmp_path / "raw"

    paths = ingest_hpi(raw_zone=raw_zone, use_sample=True)

    # Three months of sample data => three partition files.
    assert len(paths) == 3

    # The temporary CSV should have been cleaned up.
    assert not (raw_zone / "_tmp" / "uk-hpi.csv").exists()
