"""Unit tests for cads_mars_server.tools."""

from cads_mars_server.tools import bytes


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
