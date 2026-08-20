"""Tests for ndbc_buoy_scraper/pg.py -- SQL/DSN quoting and run tracking.

Quoting here spans two layers (libpq inside, DuckDB string literal outside),
and getting the outer one wrong is not a subtle failure: the DSN truncates at
the first quote and ATTACH dies with a parser error that says nothing about
credentials. These tests pin both layers.
"""

from __future__ import annotations

from ndbc_buoy_scraper import pg


class TestLiteral:
    def test_none_becomes_null(self):
        assert pg.literal(None) == "NULL"

    def test_numbers_are_unquoted(self):
        assert pg.literal(42) == "42"
        assert pg.literal(1.5) == "1.5"

    def test_booleans_use_sql_keywords(self):
        assert pg.literal(True) == "TRUE"
        assert pg.literal(False) == "FALSE"

    def test_single_quotes_are_doubled(self):
        assert pg.literal("it's") == "'it''s'"

    def test_injection_attempt_stays_inside_the_literal(self):
        # Error text reaching `RunTracker.finish` is arbitrary and includes
        # tracebacks with quotes in them.
        assert pg.literal("'; DROP TABLE pipeline_runs; --") == (
            "'''; DROP TABLE pipeline_runs; --'"
        )

    def test_nul_bytes_are_stripped(self):
        """Postgres TEXT cannot hold a NUL byte; scraped strings sometimes do."""
        assert pg.literal("a\x00b") == "'ab'"

    def test_backslashes_are_left_alone(self):
        # standard_conforming_strings defaults to on, so a backslash is a
        # literal backslash and escaping it would corrupt Windows-style paths.
        assert pg.literal("C:\\data") == "'C:\\data'"


class TestDsn:
    def test_every_value_is_quoted(self):
        dsn = pg.PostgresSettings(
            host="db.internal", port="5432", user="ndbc", password="pw", dbname="ndbc"
        ).dsn
        assert "host='db.internal'" in dsn
        assert "password='pw'" in dsn

    def test_password_with_spaces_survives(self):
        """Unquoted, libpq would truncate the DSN at the first space and fail
        authentication with no hint as to why."""
        dsn = pg.PostgresSettings(password="correct horse battery").dsn
        assert "password='correct horse battery'" in dsn

    def test_password_with_quotes_is_escaped_for_libpq(self):
        dsn = pg.PostgresSettings(password="it's").dsn
        assert "password='it\\'s'" in dsn

    def test_from_env_reads_the_documented_variables(self, monkeypatch):
        monkeypatch.setenv("PG_HOST", "example.internal")
        monkeypatch.setenv("PG_PASSWORD", "s3cret")
        settings = pg.PostgresSettings.from_env()
        assert settings.host == "example.internal"
        assert settings.password == "s3cret"

    def test_from_env_falls_back_to_development_defaults(self, monkeypatch):
        for name in ("PG_HOST", "PG_PORT", "PG_USER", "PG_PASSWORD", "PG_DB"):
            monkeypatch.delenv(name, raising=False)
        assert pg.PostgresSettings.from_env() == pg.PostgresSettings()


class TestRunTrackerResilience:
    """Monitoring must never be able to fail the pipeline it observes."""

    class _BrokenConnection:
        def execute(self, *args, **kwargs):
            raise RuntimeError("postgres is down")

    def test_start_failure_disables_tracking_instead_of_raising(self, capsys):
        tracker = pg.RunTracker(self._BrokenConnection(), "silver")
        tracker.start()

        assert tracker.enabled is False
        assert "disabled" in capsys.readouterr().out

    def test_finish_is_a_noop_once_disabled(self):
        tracker = pg.RunTracker(self._BrokenConnection(), "silver")
        tracker.start()
        tracker.finish("ok", rows_loaded=1)  # must not raise

    def test_finish_failure_is_swallowed(self, capsys):
        tracker = pg.RunTracker(self._BrokenConnection(), "silver")
        tracker.enabled = True  # pretend start succeeded
        tracker.finish("ok", rows_loaded=1)

        assert "could not close run record" in capsys.readouterr().out

    def test_each_tracker_gets_a_distinct_correlation_token(self):
        # The token replaces INSERT ... RETURNING id, which DuckDB's
        # postgres_query cannot express; a collision would make two runs
        # update each other's row.
        first = pg.RunTracker(self._BrokenConnection(), "silver")
        second = pg.RunTracker(self._BrokenConnection(), "silver")
        assert first.token != second.token
