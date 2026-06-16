"""Integration tests for the WebSocket server + client.

The ws_server spawns MARS as a subprocess and streams logs over WebSocket.
The ws_client (mars_via_ws / _run_one_server) connects, receives logs and
a final state message, then returns a Result.
"""

import asyncio
import json
import logging
import os

import websockets
from conftest import requires_linux

from cads_mars_server.log_filter import create_default_log_handler
from cads_mars_server.ws_client import _run_one_server, mars_via_ws_sync

pytestmark = requires_linux

# ---------------------------------------------------------------------------
# Basic connectivity
# ---------------------------------------------------------------------------


class TestWsServerConnectivity:
    def test_connect_and_close(self, ws_server):
        """A client should be able to open and close a connection."""

        async def _test():
            async with websockets.connect(ws_server["url"]) as ws:
                # Verify connection is alive by sending a ping
                pong = await ws.ping()
                await pong

        asyncio.run(_test())


# ---------------------------------------------------------------------------
# Protocol tests via raw websocket messages
# ---------------------------------------------------------------------------


class TestWsProtocol:
    def test_invalid_json(self, ws_server):
        """Server should respond with an error on invalid JSON."""

        async def _test():
            async with websockets.connect(ws_server["url"]) as ws:
                await ws.send("not json at all")
                resp = json.loads(await ws.recv())
                assert resp["type"] == "state"
                assert resp["status"] == "error"
                assert "Invalid JSON" in resp["error"]

        asyncio.run(_test())

    def test_missing_fields(self, ws_server):
        """Server should respond with an error if required fields are missing."""

        async def _test():
            async with websockets.connect(ws_server["url"]) as ws:
                await ws.send(json.dumps({"cmd": "start", "requests": [{}]}))
                resp = json.loads(await ws.recv())
                assert resp["type"] == "state"
                assert resp["status"] == "error"

        asyncio.run(_test())

    def test_kill_no_running_job(self, ws_server):
        """Sending kill when no job is running should return an error state."""

        async def _test():
            async with websockets.connect(ws_server["url"]) as ws:
                await ws.send(json.dumps({"cmd": "kill"}))
                resp = json.loads(await ws.recv())
                assert resp["type"] == "state"
                assert resp["status"] == "error"
                assert "No running job" in resp["error"]

        asyncio.run(_test())


# ---------------------------------------------------------------------------
# Full job lifecycle via raw messages
# ---------------------------------------------------------------------------


class TestWsJobLifecycle:
    def _make_environ(self, job_id="test-job-001"):
        return {
            "request_id": job_id,
            "user_id": "testuser",
            "namespace": "test",
            "host": "testhost",
            "username": "tester",
        }

    def test_start_and_finish(self, ws_server):
        """Start a job and verify it finishes with returncode 0."""
        shared = ws_server["shared_root"]
        job_dir = os.path.join(shared, "job1")
        os.makedirs(job_dir, exist_ok=True)

        async def _test():
            async with websockets.connect(ws_server["url"]) as ws:
                await ws.send(
                    json.dumps(
                        {
                            "cmd": "start",
                            "requests": [{"class": "od", "type": "an"}],
                            "environ": self._make_environ("job-finish-test"),
                            "target": "/job1/data.grib",
                        }
                    )
                )

                states = []
                logs = []
                async for raw in ws:
                    msg = json.loads(raw)
                    if msg.get("type") == "state":
                        states.append(msg)
                        if msg.get("status") in ("finished", "error", "killed"):
                            break
                    elif msg.get("type") == "log":
                        logs.append(msg.get("line", ""))
                    elif msg.get("type") == "heartbeat":
                        continue

                # We should have a "started" and a "finished"
                statuses = [s["status"] for s in states]
                assert "started" in statuses
                assert "finished" in statuses

                finished = [s for s in states if s["status"] == "finished"][0]
                assert int(finished["returncode"]) == 0

                # fake_mars should have produced some log output
                assert any("fake_mars" in line for line in logs)

                # Output file should exist
                output = os.path.join(job_dir, "data.grib")
                assert os.path.exists(output)

        asyncio.run(_test())


# ---------------------------------------------------------------------------
# Integration via ws_client functions
# ---------------------------------------------------------------------------


class TestWsClientIntegration:
    def _make_environ(self, job_id="ws-client-test"):
        return {
            "request_id": job_id,
            "user_id": "testuser",
            "namespace": "test",
            "host": "testhost",
            "username": "tester",
        }

    def test_run_one_server(self, ws_server):
        """Test _run_one_server against a real WS server."""
        shared = ws_server["shared_root"]
        job_dir = os.path.join(shared, "ws_one")
        os.makedirs(job_dir, exist_ok=True)

        log_handler = create_default_log_handler(filter_logs=False)
        logger = logging.getLogger("test_ws_client")

        async def _test():
            result = await _run_one_server(
                ws_server["url"],
                requests=[{"class": "od", "type": "an"}],
                environ=self._make_environ("ws-one-test"),
                target="/ws_one/data.grib",
                logger=logger,
                log_handler=log_handler,
            )
            return result

        result = asyncio.run(_test())
        assert result.error is None, f"Unexpected error: {result.error}"
        assert result.message is not None

        output = os.path.join(job_dir, "data.grib")
        assert os.path.exists(output)

    def test_mars_via_ws_sync(self, ws_server):
        """Test the synchronous wrapper mars_via_ws_sync."""
        shared = ws_server["shared_root"]
        job_dir = os.path.join(shared, "ws_sync")
        os.makedirs(job_dir, exist_ok=True)

        logger = logging.getLogger("test_ws_sync")

        result = mars_via_ws_sync(
            server_list=[ws_server["url"]],
            requests=[{"class": "od"}],
            environ=self._make_environ("ws-sync-test"),
            target="/ws_sync/data.grib",
            logger=logger,
            filter_logs=False,
        )
        assert result.error is None, f"Unexpected error: {result.error}"

        output = os.path.join(job_dir, "data.grib")
        assert os.path.exists(output)

    def test_multiple_requests_in_list(self, ws_server):
        """Multiple MARS requests in a single call."""
        shared = ws_server["shared_root"]
        job_dir = os.path.join(shared, "ws_multi")
        os.makedirs(job_dir, exist_ok=True)

        logger = logging.getLogger("test_ws_multi")

        result = mars_via_ws_sync(
            server_list=[ws_server["url"]],
            requests=[
                {"class": "od", "type": "an"},
                {"class": "od", "type": "fc"},
            ],
            environ=self._make_environ("ws-multi-test"),
            target="/ws_multi/data.grib",
            logger=logger,
            filter_logs=False,
        )
        assert result.error is None, f"Unexpected error: {result.error}"


# ---------------------------------------------------------------------------
# Error handling
# ---------------------------------------------------------------------------


class TestWsErrorHandling:
    def test_missing_workdir(self, ws_server):
        """Requesting a target whose workdir doesn't exist should error."""

        async def _test():
            async with websockets.connect(ws_server["url"]) as ws:
                await ws.send(
                    json.dumps(
                        {
                            "cmd": "start",
                            "requests": [{"class": "od"}],
                            "environ": {
                                "request_id": "err-test",
                                "user_id": "testuser",
                                "namespace": "test",
                                "host": "testhost",
                                "username": "tester",
                            },
                            "target": "/nonexistent_dir_xyz/data.grib",
                        }
                    )
                )
                resp = json.loads(await ws.recv())
                assert resp["type"] == "state"
                assert resp["status"] == "error"

        asyncio.run(_test())
