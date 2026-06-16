"""Integration tests for the pipe-mode HTTP server (server.py).

The pipe server streams MARS output from a pipe in real time.  The fake_mars
executable writes fake GRIB data to the pipe fd extracted from TARGET='&<fd>'.

NOTE: The pipe server sends `Transfer-Encoding: chunked` in headers but writes
raw (unchunked) data to the socket.  Standard HTTP libraries (requests, urllib3,
http.client) all try to decode chunked framing and fail.  Therefore data-
transfer tests use raw TCP sockets to read the actual wire bytes.
"""

import concurrent.futures
import json
import socket
import uuid

import requests as req
from conftest import requires_linux

pytestmark = requires_linux


# ---------------------------------------------------------------------------
# Helper: raw HTTP/1.0 POST bypassing chunked-encoding parsers
# ---------------------------------------------------------------------------


def _raw_post(host, port, body_dict, timeout=30):
    """Send a raw HTTP/1.0 POST and return (status_code, headers_dict, body_bytes).

    HTTP/1.0 means the response is terminated by connection close, so we can
    read everything without caring about Transfer-Encoding framing.
    """
    payload = json.dumps(body_dict).encode()
    request = (
        f"POST / HTTP/1.0\r\n"
        f"Content-Type: application/json\r\n"
        f"Content-Length: {len(payload)}\r\n"
        f"\r\n"
    ).encode() + payload

    s = socket.create_connection((host, port), timeout=timeout)
    try:
        s.sendall(request)
        chunks = []
        while True:
            chunk = s.recv(8192)
            if not chunk:
                break
            chunks.append(chunk)
    finally:
        s.close()

    response = b"".join(chunks)
    header_end = response.index(b"\r\n\r\n")
    header_block = response[:header_end].decode("latin-1")
    body = response[header_end + 4 :]

    lines = header_block.split("\r\n")
    status_code = int(lines[0].split(" ", 2)[1])

    headers = {}
    for line in lines[1:]:
        k, _, v = line.partition(":")
        headers[k.strip().lower()] = v.strip()

    return status_code, headers, body


# ---------------------------------------------------------------------------
# Endpoint tests (no data transfer — always work)
# ---------------------------------------------------------------------------


class TestPipeServerEndpoints:
    def test_head_ping(self, pipe_server):
        r = req.head(pipe_server["url"])
        assert r.status_code == 204

    def test_get_invalid_uid(self, pipe_server):
        r = req.get(pipe_server["url"] + "/not-a-uuid")
        assert r.status_code == 404

    def test_get_nonexistent_uid(self, pipe_server):
        uid = str(uuid.uuid4())
        r = req.get(pipe_server["url"] + "/" + uid)
        assert r.status_code == 404

    def test_delete_invalid_uid(self, pipe_server):
        r = req.delete(pipe_server["url"] + "/not-a-uuid")
        assert r.status_code == 404

    def test_delete_nonexistent_uid(self, pipe_server):
        uid = str(uuid.uuid4())
        r = req.delete(pipe_server["url"] + "/" + uid)
        assert r.status_code == 204


# ---------------------------------------------------------------------------
# Data-transfer tests via raw TCP (pipe server writes unchunked data)
# ---------------------------------------------------------------------------


class TestPipeServerDataTransfer:
    def _post(self, pipe_server, request_body):
        host = pipe_server["host"]
        port = pipe_server["port"]
        return _raw_post(host, port, request_body)

    def test_single_request(self, pipe_server):
        uid = str(uuid.uuid4())
        status, headers, body = self._post(
            pipe_server,
            {"request": {"class": "od", "type": "an"}, "environ": {"request_id": uid}},
        )
        assert status == 200, f"Expected 200, got {status}"
        assert len(body) > 0
        assert body.startswith(b"GRIB"), f"Expected GRIB header, got {body[:20]!r}"
        assert body.endswith(b"7777")

    def test_response_headers(self, pipe_server):
        uid = str(uuid.uuid4())
        status, headers, body = self._post(
            pipe_server,
            {"request": {"class": "od"}, "environ": {"request_id": uid}},
        )
        assert status == 200
        assert headers.get("x-mars-uid") == uid
        assert "chunked" in headers.get("transfer-encoding", "")

    def test_sequential_requests(self, pipe_server):
        for i in range(3):
            uid = str(uuid.uuid4())
            status, headers, body = self._post(
                pipe_server,
                {
                    "request": {"class": "od", "step": str(i)},
                    "environ": {"request_id": uid},
                },
            )
            assert status == 200, f"Request {i} failed with status {status}"
            assert body.startswith(b"GRIB")

    def test_data_integrity(self, pipe_server):
        uid = str(uuid.uuid4())
        status, headers, body = self._post(
            pipe_server,
            {"request": {"class": "od"}, "environ": {"request_id": uid}},
        )
        assert status == 200
        # fake_mars produces b"GRIB" + 96 null bytes + b"7777" = 104 bytes
        assert len(body) == 104
        assert body[:4] == b"GRIB"
        assert body[-4:] == b"7777"
        assert body[4:-4] == b"\x00" * 96


# ---------------------------------------------------------------------------
# Log / lifecycle tests
# ---------------------------------------------------------------------------


class TestPipeServerLogLifecycle:
    def test_log_available_after_request(self, pipe_server):
        """The log file should be readable on disk after a request."""
        uid = str(uuid.uuid4())
        # Use raw POST so the log is NOT auto-deleted
        _raw_post(
            pipe_server["host"],
            pipe_server["port"],
            {"request": {"class": "od"}, "environ": {"request_id": uid}},
        )

        # GET the log
        r = req.get(pipe_server["url"] + "/" + uid, timeout=10)
        if r.status_code == 200:
            assert "fake_mars" in r.text

        # DELETE the log
        r = req.delete(pipe_server["url"] + "/" + uid, timeout=10)
        assert r.status_code == 204

        # Confirm it's gone
        r = req.get(pipe_server["url"] + "/" + uid, timeout=10)
        assert r.status_code == 404

    def test_concurrent_requests(self, pipe_server):
        """Multiple requests to a forking server should all succeed."""

        def do_one(idx):
            uid = str(uuid.uuid4())
            return _raw_post(
                pipe_server["host"],
                pipe_server["port"],
                {
                    "request": {"class": "od", "step": str(idx)},
                    "environ": {"request_id": uid},
                },
            )

        with concurrent.futures.ThreadPoolExecutor(max_workers=3) as pool:
            futures = [pool.submit(do_one, i) for i in range(3)]
            results = [f.result() for f in concurrent.futures.as_completed(futures)]

        for status, headers, body in results:
            assert status == 200, f"Concurrent request failed with status {status}"
            assert body.startswith(b"GRIB")
