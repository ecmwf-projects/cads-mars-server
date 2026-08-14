"""Unit tests for cads_mars_server.tools."""

import pytest

from cads_mars_server.tools import area_fraction, bytes, scaled_max_retrieve_size

BASE = 161061273600  # 150 GiB
FLOOR = 1073741824  # 1 GiB


class TestAreaFraction:
    def test_no_area(self):
        assert area_fraction({"class": "od", "type": "an"}) == 1.0

    def test_full_globe(self):
        assert area_fraction({"area": "90/-180/-90/180"}) == 1.0

    def test_half_globe_latitude(self):
        assert area_fraction({"area": "90/-180/0/180"}) == pytest.approx(0.5)

    def test_area_as_list(self):
        assert area_fraction({"area": [90, -180, 0, 180]}) == pytest.approx(0.5)

    def test_area_as_string_floats(self):
        # 10° lat × 12° lon = 120 / 64800
        assert area_fraction({"area": "60.0/-10.0/50.0/2.0"}) == pytest.approx(
            120.0 / 64800.0
        )

    def test_key_case_insensitive(self):
        assert area_fraction({"AREA": "90/-180/0/180"}) == pytest.approx(0.5)

    def test_dateline_crossing(self):
        # 350E → 10E crosses 0/360: longitude extent is 20°
        assert area_fraction({"area": "10/350/0/10"}) == pytest.approx(
            (10.0 * 20.0) / 64800.0
        )

    def test_point_selection_is_zero(self):
        assert area_fraction({"area": "50/10/50/10"}) == 0.0

    def test_invalid_area_is_full(self):
        assert area_fraction({"area": "not/an/area"}) == 1.0
        assert area_fraction({"area": "1/2/3"}) == 1.0
        assert area_fraction({"area": None}) == 1.0
        assert area_fraction({"area": 42}) == 1.0

    def test_multiple_requests_largest_wins(self):
        requests = [
            {"area": "90/-180/0/180"},  # 0.5
            {"area": "60/-10/50/2"},  # ~0.00185
        ]
        assert area_fraction(requests) == pytest.approx(0.5)

    def test_one_request_without_area_is_full(self):
        requests = [{"area": "60/-10/50/2"}, {"class": "od"}]
        assert area_fraction(requests) == 1.0


class TestScaledMaxRetrieveSize:
    def test_no_area_keeps_base(self):
        assert scaled_max_retrieve_size({"class": "od"}, BASE, FLOOR) == BASE

    def test_half_globe_halves(self):
        size = scaled_max_retrieve_size({"area": "90/-180/0/180"}, BASE, FLOOR)
        assert size == BASE // 2

    def test_small_area_scales_down(self):
        size = scaled_max_retrieve_size({"area": "60/-10/50/2"}, BASE, FLOOR)
        assert size == max(int(BASE * 120.0 / 64800.0), FLOOR)

    def test_point_selection_hits_floor(self):
        assert scaled_max_retrieve_size({"area": "50/10/50/10"}, BASE, FLOOR) == FLOOR

    def test_never_exceeds_base(self):
        assert (
            scaled_max_retrieve_size({"area": "90/-180/-90/180"}, BASE, FLOOR) == BASE
        )

    def test_floor_clamped_to_base(self):
        # A floor larger than the base must not raise the limit above the base
        assert scaled_max_retrieve_size({"area": "50/10/50/10"}, 100, FLOOR) == 100

    def test_defaults_from_config(self):
        # tests run against the empty test config → built-in defaults
        assert scaled_max_retrieve_size({"class": "od"}) == BASE


class TestBytes:
    def test_zero(self):
        assert bytes(0) == "0"

    def test_small_value(self):
        assert bytes(100) == "100"

    def test_exact_kib(self):
        assert bytes(1024) == "1 KiB"

    def test_mib(self):
        result = bytes(1024 * 1024)
        assert "MiB" in result

    def test_gib(self):
        result = bytes(1024**3)
        assert "GiB" in result

    def test_tib(self):
        result = bytes(1024**4)
        assert "TiB" in result

    def test_fractional_kib(self):
        result = bytes(1536)  # 1.5 KiB
        assert "KiB" in result
        assert "1.5" in result

    def test_just_below_kib(self):
        result = bytes(1023)
        assert "KiB" not in result
