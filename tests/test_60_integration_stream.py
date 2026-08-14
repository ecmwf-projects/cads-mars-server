"""Integration tests for the stream-mode HTTP server (server_cache_and_stream.py).

The stream server writes MARS output to a file, then reads and streams
it back to the client using proper chunked HTTP encoding (with ENDR marker).
"""

import os
import uuid

import requests as req
from conftest import requires_linux

from cads_mars_server.client import RemoteMarsClient, RemoteMarsClientCluster

pytestmark = requires_linux


# ---------------------------------------------------------------------------
# Endpoint tests
# ---------------------------------------------------------------------------


class TestStreamServerEndpoints:
    def test_head_ping(self, stream_server):
        r = req.head(stream_server["url"])
        assert r.status_code == 204

    def test_get_invalid_uid(self, stream_server):
        r = req.get(stream_server["url"] + "/not-a-uuid")
        assert r.status_code == 404

    def test_get_nonexistent_uid(self, stream_server):
        uid = str(uuid.uuid4())
        r = req.get(stream_server["url"] + "/" + uid)
        assert r.status_code == 404

    def test_delete_invalid_uid(self, stream_server):
        r = req.delete(stream_server["url"] + "/not-a-uuid")
        assert r.status_code == 404

    def test_delete_nonexistent_uid(self, stream_server):
        uid = str(uuid.uuid4())
        r = req.delete(stream_server["url"] + "/" + uid)
        assert r.status_code == 204


# ---------------------------------------------------------------------------
# Data-transfer tests — the stream server uses proper chunked encoding
# ---------------------------------------------------------------------------


class TestStreamServerDataTransfer:
    def test_single_request(self, stream_server, tmp_path):
        target = str(tmp_path / "output.grib")
        client = RemoteMarsClient(url=stream_server["url"], timeout=30)
        result = client.execute(
            request={"class": "od", "type": "an", "levtype": "sfc"},
            environ={"request_id": str(uuid.uuid4())},
            target=target,
        )
        assert result.error is None, f"Unexpected error: {result.error}"
        assert os.path.exists(target)
        data = open(target, "rb").read()
        assert len(data) > 0
        assert data.startswith(b"GRIB")

    def test_mars_log_returned_and_cleaned_up(self, stream_server, tmp_path):
        """The server must not remove the MARS log file itself.

        After the transfer the client GETs the log (it becomes
        Result.message) and then DELETEs it.
        """
        target = str(tmp_path / "output.grib")
        uid = str(uuid.uuid4())
        client = RemoteMarsClient(url=stream_server["url"], timeout=30)
        result = client.execute(
            request={"class": "od", "type": "an", "levtype": "sfc"},
            environ={"request_id": uid},
            target=target,
        )
        assert result.error is None, f"Unexpected error: {result.error}"
        # The message is the MARS log content, not "None" or an error string
        assert "fake_mars" in result.message
        # The client's final DELETE removed the log from the server's logdir
        logfile = os.path.join(stream_server["logdir"], f"{uid}.log")
        assert not os.path.exists(logfile)

    def test_cluster_single_server(self, stream_server, tmp_path):
        target = str(tmp_path / "output.grib")
        cluster = RemoteMarsClientCluster(
            urls=[stream_server["url"]],
            retries=2,
            delay=1,
            timeout=30,
        )
        result = cluster.execute(
            request={"class": "od"},
            environ={"request_id": str(uuid.uuid4())},
            target=target,
        )
        assert result.error is None, f"Unexpected error: {result.error}"
        assert os.path.exists(target)
        data = open(target, "rb").read()
        assert data.startswith(b"GRIB")

    def test_sequential_requests(self, stream_server, tmp_path):
        client = RemoteMarsClient(url=stream_server["url"], timeout=30)
        for i in range(3):
            target = str(tmp_path / f"out_{i}.grib")
            result = client.execute(
                request={"class": "od", "step": str(i)},
                environ={"request_id": str(uuid.uuid4())},
                target=target,
            )
            assert result.error is None, f"Request {i} failed: {result.error}"
            assert os.path.exists(target)
            assert os.path.getsize(target) > 0

    def test_multi_request_list(self, stream_server, tmp_path):
        target = str(tmp_path / "multi.grib")
        cluster = RemoteMarsClientCluster(
            urls=[stream_server["url"]],
            retries=1,
            timeout=30,
        )
        result = cluster.execute(
            request=[
                {"class": "od", "type": "an"},
                {"class": "od", "type": "fc"},
            ],
            environ={"request_id": str(uuid.uuid4())},
            target=target,
        )
        assert result.error is None, f"Unexpected error: {result.error}"
        assert os.path.exists(target)
        # Two requests appended → file should be at least 2× the default data size
        assert os.path.getsize(target) >= 200

    def test_data_integrity(self, stream_server, tmp_path):
        """Verify the transferred data matches what fake_mars wrote."""
        target = str(tmp_path / "integrity.grib")
        client = RemoteMarsClient(url=stream_server["url"], timeout=30)
        result = client.execute(
            request={"class": "od"},
            environ={"request_id": str(uuid.uuid4())},
            target=target,
        )
        assert result.error is None
        data = open(target, "rb").read()
        # Default fake GRIB: b"GRIB" + 96 null bytes + b"7777"
        expected = b"GRIB" + b"\x00" * 96 + b"7777"
        assert data == expected


# ---------------------------------------------------------------------------
# Concurrency and stability
# ---------------------------------------------------------------------------


class TestStreamServerStability:
    def test_concurrent_requests(self, stream_server, tmp_path):
        import concurrent.futures

        def do_one(idx):
            target = str(tmp_path / f"concurrent_{idx}.grib")
            client = RemoteMarsClient(url=stream_server["url"], timeout=30)
            return client.execute(
                request={"class": "od", "step": str(idx)},
                environ={"request_id": str(uuid.uuid4())},
                target=target,
            )

        with concurrent.futures.ThreadPoolExecutor(max_workers=3) as pool:
            futures = [pool.submit(do_one, i) for i in range(3)]
            results = [f.result() for f in concurrent.futures.as_completed(futures)]

        for r in results:
            assert r.error is None, f"Concurrent request failed: {r.error}"

    def test_rapid_sequential_requests(self, stream_server, tmp_path):
        """Rapid-fire requests to verify no resource leaks."""
        client = RemoteMarsClient(url=stream_server["url"], timeout=30)
        for i in range(5):
            target = str(tmp_path / f"rapid_{i}.grib")
            result = client.execute(
                request={"class": "od"},
                environ={"request_id": str(uuid.uuid4())},
                target=target,
            )
            assert result.error is None


# ---------------------------------------------------------------------------
# Log lifecycle — stream server cleans up logs after POST
# ---------------------------------------------------------------------------


class TestStreamServerLogLifecycle:
    def test_delete_log_after_request(self, stream_server, tmp_path):
        uid = str(uuid.uuid4())
        r = req.post(
            stream_server["url"],
            json={"request": {"class": "od"}, "environ": {"request_id": uid}},
            stream=True,
            timeout=30,
        )
        for _ in r.iter_content(chunk_size=4096):
            pass

        # Stream server cleans up log files automatically,
        # so the log may or may not be available.
        r_del = req.delete(stream_server["url"] + "/" + uid, timeout=10)
        assert r_del.status_code in (204, 404)
