"""Pipeline health check -- the thing you actually alert on.

    uv run python -m ndbc_buoy_scraper.healthcheck

Exits 0 when healthy, 1 when not, so cron/systemd/an uptime probe can consume
it without parsing anything.

The check that matters most here is FRESHNESS, not "did the job exit 0". A
crawl can finish green and still load nothing new: NDBC changes its page markup
and the XPath stops matching, or a station page reshuffles and the coordinate
scrape silently yields NULLs. Exit status cannot see any of that; the age of
the newest observation can. The other three checks exist to explain a
freshness failure once it fires.
"""

from __future__ import annotations

import os
import sys

import duckdb

from ndbc_buoy_scraper import pg

# NDBC publishes continuously and the timer runs daily, so anything past ~36h
# means at least one full cycle produced nothing.
DEFAULT_MAX_AGE_HOURS = 36
# A run still marked 'running' long after any plausible crawl means the process
# died without unwinding -- OOM kill, power loss, SIGKILL. Nothing else reports
# this: the row simply sits there.
DEFAULT_STALE_RUN_HOURS = 12


def _scalar(con, sql: str):
    rows = pg.query(con, sql)
    return rows[0][0] if rows else None


def check(con: duckdb.DuckDBPyConnection) -> tuple[bool, list[str]]:
    max_age = float(os.environ.get("HEALTH_MAX_AGE_HOURS", DEFAULT_MAX_AGE_HOURS))
    stale_after = float(
        os.environ.get("HEALTH_STALE_RUN_HOURS", DEFAULT_STALE_RUN_HOURS)
    )
    problems: list[str] = []

    age_hours = _scalar(
        con,
        "SELECT EXTRACT(EPOCH FROM (now() - max(datetime) AT TIME ZONE 'UTC')) / 3600 "
        "FROM buoy_observations",
    )
    if age_hours is None:
        problems.append("FRESHNESS: buoy_observations is empty")
    elif float(age_hours) > max_age:
        problems.append(
            f"FRESHNESS: newest observation is {float(age_hours):.1f}h old "
            f"(threshold {max_age:.0f}h)"
        )

    for stage in ("scrape", "silver"):
        # `started_at::text` rather than the raw TIMESTAMPTZ: DuckDB
        # materializes a Postgres timestamptz through `pytz`, which is not a
        # dependency of this project. It is only ever printed, so rendering it
        # server-side costs nothing and keeps the dependency out.
        row = pg.query(
            con,
            "SELECT status, started_at::text FROM pipeline_runs "
            f"WHERE stage = {pg.literal(stage)} ORDER BY started_at DESC LIMIT 1",
        )
        if not row:
            problems.append(f"RUNS: no {stage} run has ever been recorded")
            continue
        status, started_at = row[0]
        if status == "failed":
            problems.append(f"RUNS: last {stage} run failed at {started_at}")

    stale = _scalar(
        con,
        "SELECT count(*) FROM pipeline_runs WHERE status = 'running' "
        f"AND started_at < now() - interval '{stale_after} hours'",
    )
    if stale:
        problems.append(
            f"RUNS: {stale} run(s) stuck in 'running' for over {stale_after:.0f}h "
            f"-- a process died without unwinding"
        )

    partition_count = _scalar(
        con,
        "SELECT count(*) FROM pg_inherits WHERE inhparent = "
        "'buoy_observations'::regclass",
    )
    if not partition_count:
        problems.append("SCHEMA: buoy_observations has no partitions attached")

    return not problems, problems


def main() -> int:
    con = duckdb.connect()
    try:
        pg.attach(con, pg.PostgresSettings.from_env())
    except Exception as exc:  # noqa: BLE001 -- unreachable DB is itself the finding
        print(f"UNHEALTHY: cannot reach Postgres: {exc}")
        return 1

    try:
        healthy, problems = check(con)
    finally:
        pg.detach(con)

    if healthy:
        print("HEALTHY")
        return 0
    for problem in problems:
        print(f"UNHEALTHY: {problem}")
    return 1


if __name__ == "__main__":
    sys.exit(main())
