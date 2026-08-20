"""Shared pytest fixtures/helpers for the test suite.

All tests run on the HOST inside the scraper's Python 3.13 venv (no Docker
required): `uv run pytest tests/`.
"""

from __future__ import annotations

import io
import os

import pandas as pd


def decode_ndbc_text(text: str) -> pd.DataFrame:
    """Mirror the scraper's mechanical decode exactly (whitespace-delimited
    pandas read, no dtype overrides) so these fixtures reproduce real bronze
    parquet content -- including pandas' automatic dtype inference, which is
    the root cause of the leading-zero edge case documented in
    tests/test_transform.py::TestKnownEdgeCases.
    """
    return pd.read_csv(io.StringIO(text.strip() + "\n"), sep=r"\s+", low_memory=False)


def write_bronze_observations(bronze_root, buoy_id: str, year: str, text: str) -> None:
    """Decode `text` (an NDBC-shaped snippet) and write it as one bronze
    observations parquet file at the exact Hive-partitioned path
    `transform.read_bronze` expects: `observations/buoy_id=<id>/year=<year>/`.
    """
    df = decode_ndbc_text(text)
    partition_dir = os.path.join(
        str(bronze_root), "observations", f"buoy_id={buoy_id}", f"year={year}"
    )
    os.makedirs(partition_dir, exist_ok=True)
    df.to_parquet(os.path.join(partition_dir, "data.parquet"), index=False, engine="pyarrow")
