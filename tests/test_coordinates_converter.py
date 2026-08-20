"""Unit tests for ndbc_buoy_scraper/coordinates_converter.py.

Pure functions, no pyspark/pandas dependency -- runs anywhere, including the
scraper's Python 3.13 venv:

    uv run --with pytest pytest tests/test_coordinates_converter.py
"""

from __future__ import annotations

import pytest

from ndbc_buoy_scraper.coordinates_converter import (
    convert_coordinates,
    is_valid_coordinates,
)


class TestConvertCoordinates:
    def test_north_east_hemisphere(self):
        text = "48.126 N 163.355 E (48°7'34\" N 163°21'18\" E)"
        lat, lon = convert_coordinates(text)
        assert lat == pytest.approx(48.126)
        assert lon == pytest.approx(163.355)

    def test_south_hemisphere_negates_latitude(self):
        text = "33.735 S 151.289 E"
        lat, lon = convert_coordinates(text)
        assert lat == pytest.approx(-33.735)
        assert lon == pytest.approx(151.289)

    def test_west_hemisphere_negates_longitude(self):
        text = "37.227 N 76.479 W"
        lat, lon = convert_coordinates(text)
        assert lat == pytest.approx(37.227)
        assert lon == pytest.approx(-76.479)

    def test_south_and_west_both_negated(self):
        text = "12.345 S 98.765 W"
        lat, lon = convert_coordinates(text)
        assert lat == pytest.approx(-12.345)
        assert lon == pytest.approx(-98.765)

    def test_raises_on_single_coordinate(self):
        with pytest.raises(ValueError):
            convert_coordinates("48.126 N")

    def test_raises_on_no_coordinates(self):
        with pytest.raises(ValueError):
            convert_coordinates("station data unavailable")

    def test_raises_on_integer_only_values(self):
        # The pattern requires a decimal point (\d+\.\d+); bare integers
        # ("48 N 163 E") do not match and must raise, not silently truncate.
        with pytest.raises(ValueError):
            convert_coordinates("48 N 163 E")

    def test_uses_only_first_two_matches(self):
        # A trailing third coordinate-shaped token must not affect the
        # first two (lat/lon) results.
        text = "10.000 N 20.000 E 30.000 N"
        lat, lon = convert_coordinates(text)
        assert lat == pytest.approx(10.000)
        assert lon == pytest.approx(20.000)


class TestIsValidCoordinates:
    def test_none_is_invalid(self):
        assert is_valid_coordinates(None) is False

    def test_empty_string_is_invalid(self):
        assert is_valid_coordinates("") is False

    def test_well_formed_text_is_valid(self):
        assert is_valid_coordinates("37.227 N 76.479 W (37°13'36\" N 76°28'43\" W)") is True

    def test_single_coordinate_is_invalid(self):
        assert is_valid_coordinates("37.227 N") is False

    def test_integer_only_values_are_invalid(self):
        assert is_valid_coordinates("48 N 163 E") is False

    def test_unrelated_text_is_invalid(self):
        assert is_valid_coordinates("no coordinate data") is False
