"""The `rx` command-line tool.

When you ran `pip install -e .` the `rx` command got wired up to the
`app` object at the bottom of this file (see pyproject.toml).

Typer is a small library that turns a Python function into a CLI.
Each function decorated with @app.command() becomes a subcommand.
"""

from __future__ import annotations

from pathlib import Path

import typer

from rx_platform.ingest import DEFAULT_URL, ingest_hpi

app = typer.Typer(
    help="UK Housing Data Platform - ingestion tools.",
    no_args_is_help=True,
)


# Adding a callback forces Typer to always expect a command name,
# so `rx ingest` works even though there's only one command today.
@app.callback()
def main() -> None:
    """UK Housing Data Platform - ingestion tools."""


@app.command()
def ingest(
    sample: bool = typer.Option(
        False,
        "--sample",
        help="Use the bundled sample data instead of downloading from the internet.",
    ),
    url: str = typer.Option(
        DEFAULT_URL,
        "--url",
        help="URL of the HPI CSV to download. Ignored when --sample is set.",
    ),
    output: Path = typer.Option(
        Path("data/raw"),
        "--output",
        "-o",
        help="Folder to write Parquet partitions under.",
    ),
) -> None:
    """Download the UK HPI data and write it as monthly Parquet partitions."""
    paths = ingest_hpi(url=url, raw_zone=output, use_sample=sample)
    typer.echo(f"\nFinished. {len(paths)} monthly partitions are in {output}/hpi/")
