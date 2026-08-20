"""Postgres connection settings, SQL passthrough, and pipeline-run tracking.

Everything here talks to Postgres THROUGH DuckDB's `postgres` extension rather
than a dedicated driver. That is a deliberate dependency choice: DuckDB is
already required for the silver transform, and its `postgres` extension can
both bulk-load relations and run arbitrary SQL, so the project needs no
`psycopg`/`asyncpg` dependency for a portfolio-scale deployment.

Two quirks of that extension shape this module and are worth knowing before
editing it:

  * `postgres_query()` wraps its argument in `COPY (...) TO STDOUT`, which
    rejects DML. So `INSERT ... RETURNING id` is impossible and a writer cannot
    learn its own row id. `RunTracker` works around this with a client-side
    UUID correlation token -- see db/init.sql's `pipeline_runs.run_token`.
  * Both `postgres_execute()` and `postgres_query()` accept DuckDB bound
    parameters for the SQL text. Always pass SQL that way (as this module
    does) instead of interpolating it into the DuckDB statement: it removes an
    entire layer of quote escaping, leaving only Postgres-level quoting to get
    right.
"""

from __future__ import annotations

import json
import os
import uuid
from dataclasses import dataclass
from typing import Any

import duckdb

DEFAULT_ALIAS = "pg"


def _dsn_value(value: str) -> str:
    """Quote one libpq keyword/value DSN value.

    Passwords legitimately contain spaces, quotes and backslashes; unquoted
    interpolation would silently truncate the DSN at the first space and
    produce a baffling authentication failure.
    """
    escaped = value.replace("\\", "\\\\").replace("'", "\\'")
    return f"'{escaped}'"


def literal(value: Any) -> str:
    """Render a Python value as a Postgres SQL literal.

    Used only for values this module itself composes into statements (run
    metadata, year bounds). Modern Postgres defaults to
    `standard_conforming_strings = on`, so backslashes are literal and doubling
    single quotes is the complete escape.
    """
    if value is None:
        return "NULL"
    if isinstance(value, bool):
        return "TRUE" if value else "FALSE"
    if isinstance(value, (int, float)):
        return repr(value)
    # NUL bytes are the one byte Postgres text can never hold; scraped error
    # strings occasionally carry them.
    text = str(value).replace("\x00", "")
    return "'" + text.replace("'", "''") + "'"


@dataclass(frozen=True)
class PostgresSettings:
    host: str = "localhost"
    port: str = "5432"
    user: str = "ndbc"
    password: str = "ndbc"
    dbname: str = "ndbc"

    @classmethod
    def from_env(cls) -> PostgresSettings:
        return cls(
            host=os.environ.get("PG_HOST", "localhost"),
            port=os.environ.get("PG_PORT", "5432"),
            user=os.environ.get("PG_USER", "ndbc"),
            password=os.environ.get("PG_PASSWORD", "ndbc"),
            dbname=os.environ.get("PG_DB", "ndbc"),
        )

    @property
    def dsn(self) -> str:
        parts = {
            "host": self.host,
            "port": self.port,
            "user": self.user,
            "password": self.password,
            "dbname": self.dbname,
        }
        return " ".join(f"{k}={_dsn_value(v)}" for k, v in parts.items())


def attach(
    con: duckdb.DuckDBPyConnection,
    settings: PostgresSettings,
    alias: str = DEFAULT_ALIAS,
) -> None:
    con.execute("INSTALL postgres")
    con.execute("LOAD postgres")
    # The DSN is doubly quoted: libpq quoting inside (so values may contain
    # spaces), then DuckDB string-literal escaping outside. `ATTACH` takes no
    # bound parameters, so the escaping cannot be delegated as it is in
    # `execute()`/`query()`. Missing the outer layer makes libpq's own quotes
    # terminate DuckDB's literal early.
    dsn = settings.dsn.replace("'", "''")
    con.execute(f"ATTACH '{dsn}' AS {alias} (TYPE postgres)")


def detach(con: duckdb.DuckDBPyConnection, alias: str = DEFAULT_ALIAS) -> None:
    con.execute(f"DETACH {alias}")


def execute(
    con: duckdb.DuckDBPyConnection, sql: str, alias: str = DEFAULT_ALIAS
) -> None:
    """Run arbitrary SQL (DDL/DML) directly on Postgres."""
    con.execute("CALL postgres_execute(?, ?)", [alias, sql])


def query(
    con: duckdb.DuckDBPyConnection, sql: str, alias: str = DEFAULT_ALIAS
) -> list[tuple]:
    """Run a SELECT directly on Postgres and materialize the rows.

    SELECT only -- see the module docstring on the `COPY` wrapping.
    """
    return con.execute(
        "SELECT * FROM postgres_query(?, ?)", [alias, sql]
    ).fetchall()


def refresh_catalog(con: duckdb.DuckDBPyConnection) -> None:
    """Invalidate DuckDB's cached view of the attached Postgres catalog.

    REQUIRED after creating a table via `execute()`: DuckDB caches the remote
    catalog at ATTACH time, so a table created by SQL passthrough is invisible
    to `relation.insert_into('pg.<table>')` until the cache is dropped.
    """
    con.execute("CALL pg_clear_cache()")


class RunTracker:
    """Records one pipeline stage execution into `pipeline_runs`.

    A row is written at START with status 'running', then updated on finish.
    Writing up front is the point: a run that hangs or is OOM-killed leaves a
    stale 'running' row, which is exactly the failure a
    write-only-on-success design cannot detect.

    Never raises. Observability that can break the pipeline it observes is
    worse than no observability -- a failed metrics write must not turn a
    successful load into a failed run.
    """

    def __init__(
        self, con: duckdb.DuckDBPyConnection, stage: str, alias: str = DEFAULT_ALIAS
    ):
        self.con = con
        self.stage = stage
        self.alias = alias
        self.token = str(uuid.uuid4())
        self.enabled = True

    def start(self) -> None:
        try:
            execute(
                self.con,
                "INSERT INTO pipeline_runs (run_token, stage, status) "
                f"VALUES ({literal(self.token)}::uuid, {literal(self.stage)}, 'running')",
                self.alias,
            )
        except Exception as exc:  # noqa: BLE001 -- see class docstring
            self.enabled = False
            print(f"[run-tracker] disabled, could not open run record: {exc}")

    def finish(
        self,
        status: str,
        *,
        rows_loaded: int | None = None,
        years_loaded: int | None = None,
        rows_rejected: int | None = None,
        scrapy_stats: dict | None = None,
        error: str | None = None,
    ) -> None:
        if not self.enabled:
            return
        stats_sql = (
            f"{literal(json.dumps(scrapy_stats, default=str))}::jsonb"
            if scrapy_stats is not None
            else "NULL"
        )
        assignments = [
            "finished_at = now()",
            f"status = {literal(status)}",
            f"rows_loaded = {literal(rows_loaded)}",
            f"years_loaded = {literal(years_loaded)}",
            f"rows_rejected = {literal(rows_rejected)}",
            f"scrapy_stats = {stats_sql}",
            # Postgres TEXT has no length cap, but an unbounded traceback in a
            # monitoring table helps nobody; the head carries the diagnosis.
            f"error = {literal(error[:4000] if error else None)}",
        ]
        try:
            execute(
                self.con,
                f"UPDATE pipeline_runs SET {', '.join(assignments)} "
                f"WHERE run_token = {literal(self.token)}::uuid",
                self.alias,
            )
        except Exception as exc:  # noqa: BLE001 -- see class docstring
            print(f"[run-tracker] could not close run record: {exc}")
