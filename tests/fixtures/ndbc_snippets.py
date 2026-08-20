"""Synthetic NDBC stdmet snippets used by ``tests/test_silver.py`` (formerly
also by ``tests/test_transform.py``, retired now that Spark's ETL role has
moved to ``ndbc_buoy_scraper/silver.py``).

Each constant is whitespace-delimited text shaped exactly like a real NDBC
historical-file response: a header row, a units row (kept as an ordinary
data row, mirroring bronze — see design ADR-1 / section 2), and one or more
data rows. They are decoded the same way the scraper's mechanical decode
does (``pd.read_csv(..., sep=r"\\s+")``) via
``tests.conftest.write_bronze_observations``, so these fixtures exercise the
transform against genuinely bronze-shaped input rather than pre-built Spark
rows.

Column order in the header is irrelevant to the transform (pandas/Spark both
select by name), but every row below is kept aligned with its header for
readability.
"""

# Header variant: "#YY" (current NDBC convention). Two rows:
#   - row 1 (10:30): WDIR=099 is a LEGITIMATE 99-degree reading (the
#     wind_direction sentinel is 999.0, not 99.0) and MUST be preserved;
#     WSPD=99.0 IS the wind_speed sentinel and MUST become NULL. This single
#     row proves sentinel nulling is per-column, not a blanket 99.0 replace
#     (spec scenario "Sentinel normalization").
#   - row 2 (11:00): every sentinel-bearing column holds its documented
#     sentinel value (see silver.py::SENTINEL_VALUES) and must all null out.
HASH_YY_WITH_SENTINELS = """
#YY MM DD hh mm WDIR WSPD GST WVHT DPD APD MWD PRES ATMP WTMP DEWP VIS TIDE
#yr mo dy hr mn degT m/s m/s m sec sec degT hPa degC degC degC nmi ft
2020 01 15 10 30 099 99.0 5.0 1.2 6.0 5.0 100 1015.0 20.0 19.0 18.0 10.0 1.5
2020 01 15 11 00 999 8.0 9.0 1.3 6.5 5.5 999 9999.0 999.0 999.0 999.0 99.0 99.00
"""

# Header variant: "YYYY" (mid-2000s convention) — proves the coalesce in
# normalize_columns resolves this variant to the same year4 field.
YYYY_HEADER = """
YYYY MM DD hh mm WDIR WSPD GST WVHT DPD APD MWD PRES ATMP WTMP DEWP VIS TIDE
yr mo dy hr mn degT m/s m/s m sec sec degT hPa degC degC degC nmi ft
2015 06 01 00 00 100 5.0 6.0 1.0 5.0 4.0 110 1010.0 15.0 14.0 13.0 9.0 1.0
"""

# Header variant: bare 2-digit "YY" (older historical convention). Both
# values are >= 10 so pandas' automatic int64 inference does not strip a
# leading zero (see the dedicated leading-zero edge case fixture below for
# that discovered gap) — this fixture isolates the pivot-at-50 rule itself:
# "79" (>= 50) -> 1979, "23" (< 50) -> 2023.
YY_TWO_DIGIT_HEADER = """
YY MM DD hh mm WDIR WSPD GST WVHT DPD APD MWD PRES ATMP WTMP DEWP VIS TIDE
yr mo dy hr mn degT m/s m/s m sec sec degT hPa degC degC degC nmi ft
79 06 01 00 00 100 5.0 6.0 1.0 5.0 4.0 110 1010.0 15.0 14.0 13.0 9.0 1.0
23 06 01 00 00 100 5.0 6.0 1.0 5.0 4.0 110 1010.0 15.0 14.0 13.0 9.0 1.0
"""

# Edge case fixture: 2-digit year value < 10 ("05"). pandas' mechanical
# decode infers this column as int64, silently dropping the leading zero
# ("05" -> 5) before it ever reaches the transform. Currently unused: the
# Spark-side pin for this edge case lived in the now-retired
# tests/test_transform.py::TestKnownEdgeCases; kept here in case a DuckDB-side
# equivalent test is added to tests/test_silver.py.
YY_TWO_DIGIT_LEADING_ZERO_LOSS = """
YY MM DD hh mm WDIR WSPD GST WVHT DPD APD MWD PRES ATMP WTMP DEWP VIS TIDE
yr mo dy hr mn degT m/s m/s m sec sec degT hPa degC degC degC nmi ft
05 06 01 00 00 100 5.0 6.0 1.0 5.0 4.0 110 1010.0 15.0 14.0 13.0 9.0 1.0
"""

# No "mm" (minute) column at all -- normalize_columns must default
# minute_raw to "0" rather than failing (spec scenario "Minute present vs
# absent").
MINUTE_ABSENT = """
#YY MM DD hh WDIR WSPD GST WVHT DPD APD MWD PRES ATMP WTMP DEWP VIS TIDE
#yr mo dy hr degT m/s m/s m sec sec degT hPa degC degC degC nmi ft
2020 03 10 05 120 6.0 7.0 1.1 5.5 4.5 130 1012.0 16.0 15.0 14.0 8.0 1.2
"""

# Includes the newer "PTDY" column (pressure_tendency) that most historical
# files never carry — paired with MINUTE_ABSENT (which lacks it) to exercise
# mergeSchema tolerance (spec scenario "Newer columns present vs absent").
NEWER_COLUMN_PRESENT = """
#YY MM DD hh mm WDIR WSPD GST WVHT DPD APD MWD PRES ATMP WTMP DEWP VIS PTDY TIDE
#yr mo dy hr mn degT m/s m/s m sec sec degT hPa degC degC degC nmi hPa ft
2021 07 04 12 00 150 4.0 5.0 0.9 4.0 3.5 140 1005.0 22.0 21.0 20.0 11.0 0.5 1.8
"""

# Two rows sharing the exact same (year, month, day, hour, minute) --
# overlapping NDBC files can repeat a (buoy_id, datetime) pair, which is
# exactly what transform.dedup() must collapse before the Postgres
# UNIQUE(buoy_id, datetime) write.
DUPLICATE_DATETIME = """
#YY MM DD hh mm WDIR WSPD GST WVHT DPD APD MWD PRES ATMP WTMP DEWP VIS TIDE
#yr mo dy hr mn degT m/s m/s m sec sec degT hPa degC degC degC nmi ft
2022 02 02 02 02 200 3.0 4.0 0.8 3.5 3.0 150 1000.0 10.0 9.0 8.0 7.0 0.3
2022 02 02 02 02 210 3.5 4.5 0.9 3.6 3.1 155 1001.0 10.5 9.5 8.5 7.5 0.4
"""
