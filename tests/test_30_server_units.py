"""Unit tests for server-level functions: tidy, validate_uuid, _resolve_datadir, _decode_exit."""

import os
import signal

import pytest

from cads_mars_server.server import tidy, validate_uuid
from cads_mars_server.server_cache_and_stream import (
    Handler as StreamHandler,
    _resolve_datadir,
    validate_uuid as stream_validate_uuid,
)


# ---------------------------------------------------------------------------
# validate_uuid
# ---------------------------------------------------------------------------


class TestValidateUuid:
    def test_valid(self):
        assert validate_uuid("550e8400-e29b-41d4-a716-446655440000")

    def test_invalid_short(self):
        assert not validate_uuid("550e8400")

    def test_invalid_uppercase(self):
        assert not validate_uuid("550E8400-E29B-41D4-A716-446655440000")

    def test_path_traversal(self):
        assert not validate_uuid("../../etc/passwd__________")

    def test_empty(self):
        assert not validate_uuid("")

    def test_consistent_across_modules(self):
        uid = "550e8400-e29b-41d4-a716-446655440000"
        assert bool(validate_uuid(uid)) == bool(stream_validate_uuid(uid))


# ---------------------------------------------------------------------------
# tidy
# ---------------------------------------------------------------------------


class TestTidy:
    def test_simple_ident(self):
        assert tidy("temperature") == "temperature"

    def test_integer(self):
        assert tidy(100) == "100"

    def test_float_string(self):
        assert tidy("-10.5") == "-10.5"

    def test_slash_separated_string(self):
        assert tidy("00/12") == "00/12"

    def test_list_values(self):
        assert tidy(["00", "12"]) == "00/12"

    def test_single_quoted(self):
        assert tidy("'value'") == "'value'"

    def test_double_quoted(self):
        assert tidy('"value"') == '"value"'

    def test_string_with_spaces(self):
        # "some value" matches IDENT regex (allows spaces in the middle)
        result = tidy("some value")
        assert result == "some value"

    def test_whitespace_stripped(self):
        assert tidy("  temperature  ") == "temperature"

    def test_absolute_path_not_split(self):
        # Absolute paths are not split by / but get double-quoted
        result = tidy("/path/to/file")
        assert result == '"/path/to/file"'

    def test_scientific_notation(self):
        assert tidy("1.5E-3") == "1.5E-3"


# ---------------------------------------------------------------------------
# _resolve_datadir
# ---------------------------------------------------------------------------


class TestResolveDatadir:
    def test_no_shares_falls_back(self, tmp_path):
        result = _resolve_datadir("uid-1", str(tmp_path), [], "cache")
        assert os.path.isdir(result)

    def test_valid_share(self, tmp_path):
        (tmp_path / "s0").mkdir()
        result = _resolve_datadir("uid-1", str(tmp_path), ["s0"], "cache")
        assert result == str(tmp_path / "s0" / "cache")
        assert os.path.isdir(result)

    def test_deterministic_for_same_uid(self, tmp_path):
        for name in ("s0", "s1", "s2"):
            (tmp_path / name).mkdir()
        uid = "test-uid-abc"
        shares = ["s0", "s1", "s2"]
        r1 = _resolve_datadir(uid, str(tmp_path), shares, "c")
        r2 = _resolve_datadir(uid, str(tmp_path), shares, "c")
        assert r1 == r2

    def test_skips_missing_share(self, tmp_path):
        (tmp_path / "s1").mkdir()
        result = _resolve_datadir("uid-1", str(tmp_path), ["missing", "s1"], "cache")
        assert "s1" in result

    def test_all_shares_missing(self, tmp_path):
        result = _resolve_datadir("uid-1", str(tmp_path), ["a", "b"], "cache")
        assert os.path.isdir(result)


# ---------------------------------------------------------------------------
# Handler._decode_exit
# ---------------------------------------------------------------------------


class TestDecodeExit:
    @pytest.fixture(autouse=True)
    def handler(self):
        self.h = StreamHandler.__new__(StreamHandler)

    def test_success(self):
        assert self.h._decode_exit(0) == {"error": False}

    def test_normal_exit_code_1(self):
        status = 1 << 8  # WEXITSTATUS = 1
        info = self.h._decode_exit(status)
        assert info["error"] is True
        assert info["code"] == 400
        assert info["message"] == "exited"
        assert info["value"] == 1

    def test_killed_by_signal(self):
        status = signal.SIGKILL  # WIFSIGNALED
        info = self.h._decode_exit(status)
        assert info["error"] is True
        assert info["message"] == "killed"

    def test_high_exit_code_treated_as_signal(self):
        status = 137 << 8  # 128 + 9 = SIGKILL
        info = self.h._decode_exit(status)
        assert info["error"] is True
        assert info["message"] == "killed"
        assert info["value"] == 9

    def test_retry_flags_on_sigterm(self):
        status = signal.SIGTERM  # WIFSIGNALED, retryable
        info = self.h._decode_exit(status)
        assert info["error"] is True
        assert info["retry_next_host"] is True
        assert info["retry_same_host"] is False
