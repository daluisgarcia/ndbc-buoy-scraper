"""Report the NDBC source files that were served corrupt.

    uv run python -m ndbc_buoy_scraper.malformed          # one row per file
    FULL=1 uv run python -m ndbc_buoy_scraper.malformed   # plus the bad lines

The bronze decode drops lines NDBC serves malformed, because one fused record
must not take down its whole buoy-year (see the spider's `_get_data_from_url`).
That trade is only defensible if the dropped data is recoverable BY HAND, which
is what this reads back: the URL to re-fetch, the exact line numbers, and the
raw text of every line that was thrown away.

Exits 0 whether or not anything is quarantined. Upstream corruption is a fact
about NOAA's archive, not a failure of this pipeline -- `make health` is the
target that alerts. This one is for the human doing the re-extraction.
"""

from __future__ import annotations

import json
import os
import sys
from pathlib import Path

from ndbc_buoy_scraper.pipelines import QUARANTINE_DIR


def load_records(bronze_root: str) -> list[dict]:
    """Read every quarantine record under `bronze_root`, newest name first.

    Deliberately plain `json.load` rather than DuckDB's `read_json_auto`: the
    records nest a variable-length `bad_lines` array, which DuckDB would have
    to unify into one struct schema across every file before it could show any
    of them -- so a single odd record would break the whole listing. At the
    scale this ever reaches (a handful of files), reading them directly is
    both simpler and strictly more robust.
    """
    quarantine_dir = Path(bronze_root) / QUARANTINE_DIR
    if not quarantine_dir.is_dir():
        return []

    records = []
    for path in sorted(quarantine_dir.glob('*.json')):
        try:
            records.append(json.loads(path.read_text(encoding='utf-8')))
        except (OSError, json.JSONDecodeError) as exc:
            # Reported, never fatal: one unreadable record must not hide the
            # rest of the quarantine, which is the only copy of what was lost.
            print(f'  ! unreadable quarantine record {path.name}: {exc}')
    return records


def format_report(records: list[dict], full: bool) -> str:
    if not records:
        return 'No malformed source files recorded. Every scraped file parsed cleanly.'

    total_dropped = sum(r.get('dropped_lines', 0) for r in records)
    lines = [
        f'{len(records)} source file(s) served corrupt by NDBC, '
        f'{total_dropped} observation line(s) dropped.',
        '',
    ]

    for record in records:
        lines.append(
            f"{record.get('source_stem')}  "
            f"buoy={record.get('buoy_id')} year={record.get('year')}  "
            f"dropped={record.get('dropped_lines')} "
            f"kept={record.get('rows_parsed')}"
        )
        lines.append(f"  url: {record.get('url')}")

        if record.get('lines_truncated'):
            lines.append(
                '  NOTE: too many bad lines to record; only the first are listed'
            )
        located = record.get('located_lines', 0)
        if located != record.get('dropped_lines'):
            lines.append(
                f'  NOTE: only {located} of {record.get("dropped_lines")} '
                f'dropped line(s) could be located in the response'
            )

        bad_lines = record.get('bad_lines') or []
        if full:
            expected = record.get('expected_fields')
            for bad in bad_lines:
                lines.append(
                    f"  line {bad.get('line_number')}: "
                    f"{bad.get('field_count')} fields, expected {expected}"
                )
                lines.append(f"    {bad.get('text')}")
        elif bad_lines:
            numbers = ', '.join(str(b.get('line_number')) for b in bad_lines)
            lines.append(f'  lines: {numbers}   (FULL=1 to print them)')
        lines.append('')

    return '\n'.join(lines)


def main() -> int:
    bronze_root = os.environ.get('BRONZE_ROOT', 'data/bronze')
    full = os.environ.get('FULL', '') in {'1', 'true', 'yes'}
    print(format_report(load_records(bronze_root), full))
    return 0


if __name__ == '__main__':
    sys.exit(main())
