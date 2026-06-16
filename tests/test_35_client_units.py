"""Unit tests for cads_mars_server.client (no live server needed)."""

from cads_mars_server.client import ClientError, Result


class TestResult:
    def test_defaults(self):
        r = Result()
        assert r.error is None
        assert r.message is None
        assert r.retry_same_host is False
        assert r.retry_next_host is False

    def test_with_error(self):
        r = Result(error=RuntimeError("fail"), retry_next_host=True)
        assert r.error is not None
        assert r.retry_next_host is True

    def test_repr(self):
        r = Result(error=None, message="hello world")
        text = repr(r)
        assert "Result" in text
        assert "hello worl" in text  # truncated to 10 chars

    def test_repr_no_message(self):
        r = Result()
        text = repr(r)
        assert "None" in text


class TestClientError:
    def test_exited(self):
        e = ClientError({"exited": 1})
        assert "exit code 1" in str(e)
        assert e.retry_same_host is False
        assert e.retry_next_host is False

    def test_killed(self):
        e = ClientError({"killed": 9})
        assert "signal 9" in str(e)

    def test_other(self):
        e = ClientError({"some": "value"})
        assert "MARS client error" in str(e)

    def test_retry_flags(self):
        e = ClientError({"exited": 1, "retry_same_host": True, "retry_next_host": True})
        assert e.retry_same_host is True
        assert e.retry_next_host is True


class TestRemoteMarsClientSession:
    def test_transfer_protocol_markers(self, tmp_path):
        """Test that _transfer correctly handles ENDR/EROR/RWND markers."""
        from unittest.mock import MagicMock

        from cads_mars_server.client import RemoteMarsClientSession

        target = str(tmp_path / "out.grib")
        session = RemoteMarsClientSession(
            url="http://localhost:9999",
            request={"class": "od"},
            environ={},
            target=target,
        )

        # Simulate a chunked response with data + ENDR
        mock_response = MagicMock()
        chunks = [b"GRIB" + b"\x00" * 96 + b"7777", b"ENDR"]
        mock_response.raw.read_chunked.return_value = iter(chunks)

        session._transfer(mock_response)

        assert session.endr_recieved is True
        with open(target, "rb") as f:
            data = f.read()
        assert data == b"GRIB" + b"\x00" * 96 + b"7777"

    def test_transfer_rwnd_truncates(self, tmp_path):
        """RWND marker should truncate the file back to position."""
        from unittest.mock import MagicMock

        from cads_mars_server.client import RemoteMarsClientSession

        target = str(tmp_path / "out.grib")
        session = RemoteMarsClientSession(
            url="http://localhost:9999",
            request={"class": "od"},
            environ={},
            target=target,
        )

        # Write some data, then RWND (rewind), then new data, then ENDR
        chunks = [b"bad_data_XXXX", b"RWND", b"GRIB_GOOD", b"ENDR"]
        mock_response = MagicMock()
        mock_response.raw.read_chunked.return_value = iter(chunks)

        session._transfer(mock_response)

        with open(target, "rb") as f:
            data = f.read()
        assert data == b"GRIB_GOOD"

    def test_transfer_missing_endr_raises(self, tmp_path):
        """If ENDR is never received, _transfer should raise."""
        import pytest
        from unittest.mock import MagicMock

        from cads_mars_server.client import RemoteMarsClientSession

        target = str(tmp_path / "out.grib")
        session = RemoteMarsClientSession(
            url="http://localhost:9999",
            request={"class": "od"},
            environ={},
            target=target,
        )

        chunks = [b"some_data"]
        mock_response = MagicMock()
        mock_response.raw.read_chunked.return_value = iter(chunks)

        with pytest.raises(ValueError, match="ENDR not received"):
            session._transfer(mock_response)

    def test_transfer_eror_raises_client_error(self, tmp_path):
        """EROR marker followed by JSON should raise ClientError."""
        import pytest
        from unittest.mock import MagicMock

        from cads_mars_server.client import ClientError, RemoteMarsClientSession

        target = str(tmp_path / "out.grib")
        session = RemoteMarsClientSession(
            url="http://localhost:9999",
            request={"class": "od"},
            environ={},
            target=target,
        )

        import json
        error_msg = json.dumps({"exited": 1}).encode()
        chunks = [b"EROR", error_msg]
        mock_response = MagicMock()
        mock_response.raw.read_chunked.return_value = iter(chunks)

        with pytest.raises(ClientError):
            session._transfer(mock_response)
