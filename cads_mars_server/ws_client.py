import asyncio
import json
import os
import random
from typing import Any, Iterable

import websockets

from cads_mars_server.client import Result
from cads_mars_server.config import (
    RETRY_DELAY,
    MAX_RETRIES,
    REQUEST_TIMEOUT,
    WS_PING_INTERVAL,
    WS_CLOSE_TIMEOUT,
)


async def _run_one_server(
    ws_url: str,
    requests: list[dict[str, Any]],
    environ: dict[str, Any],
    target: str,
    logger=None,
) -> Result:
    logs: list[str] = []

    if logger:
        logger.info(f"Connecting to MARS server at {ws_url}")

    async with websockets.connect(
        ws_url, ping_interval=WS_PING_INTERVAL, close_timeout=WS_CLOSE_TIMEOUT
    ) as ws:
        # SEND START COMMAND
        await ws.send(
            json.dumps(
                {
                    "cmd": "start",
                    "requests": requests,
                    "environ": environ,
                    "target": target,
                }
            )
        )

        # RECEIVE JOB STREAM
        async for raw in ws:
            msg = json.loads(raw)
            mtype = msg.get("type")

            if mtype == "heartbeat":
                if logger:
                    logger.info(f"Received heartbeat from {ws_url}")
                continue

            if mtype == "log":
                line = msg.get("line", "")
                logs.append(line)
                if logger:
                    logger.info(line)
                else:
                    print(line)
                continue

            if mtype == "state":
                status = msg.get("status")

                if status == "started":
                    if logger:
                        logger.info(f"Job started on server {ws_url}")
                    else:
                        print(f"Job started on server {ws_url}")
                    continue

                if status == "error":
                    err = msg.get("error", "Unknown server error")
                    logs.append(f"ERROR: {err}")
                    if logger:
                        logger.error(f"Error from server: {err}")
                    else:
                        print(f"Error from server: {err}")
                    return Result(error=RuntimeError(err), message="\n".join(logs))

                if status == "finished":
                    returncode = int(msg.get("returncode", 0))
                    if returncode != 0:
                        logs.append(f"ERROR: job finished with returncode={returncode}")
                        return Result(
                            error=RuntimeError(f"MARS job failed (returncode={returncode})"),
                            message="\n".join(logs),
                        )
                    return Result(error=None, message="\n".join(logs))

                if status == "killed":
                    logs.append("ERROR: job killed by server")
                    return Result(
                        error=RuntimeError("MARS job killed by server"),
                        message="\n".join(logs),
                    )

        # If the websocket closes without an explicit finished/killed/error state:
        logs.append("ERROR: connection closed before job finished")
        return Result(
            error=RuntimeError("Connection closed before job finished"),
            message="\n".join(logs),
        )


async def mars_via_ws(
    server_list: Iterable[str],
    requests: list[dict[str, Any]] | dict[str, Any],
    environ: dict[str, Any],
    target: str,
    logger=None,
) -> Result:
    """
    Execute a MARS retrieval via websocket servers.

    Returns a :class:`~cads_mars_server.client.Result` instance (same shape as the HTTP client),
    so callers can uniformly handle success/failure.
    """
    reqs = requests if isinstance(requests, list) else [requests]

    servers = list(server_list)
    random.shuffle(servers)

    last_result: Result | None = None

    for attempt in range(MAX_RETRIES):
        for ws_url in servers:
            try:
                # Apply an overall timeout to each server attempt.
                last_result = await asyncio.wait_for(
                    _run_one_server(ws_url, reqs, environ, target, logger=logger),
                    timeout=REQUEST_TIMEOUT,
                )
                # If the server returned an error, we try the next server (or next retry cycle).
                if not last_result.error:
                    return last_result
            except (
                websockets.exceptions.ConnectionClosedError,
                websockets.exceptions.InvalidStatusCode,
                ConnectionRefusedError,
                TimeoutError,
                asyncio.TimeoutError,
                OSError,
            ) as e:
                if logger:
                    logger.warning(f"Server {ws_url} failed ({e}); retrying...")
                else:
                    print(f"Server {ws_url} failed ({e}); retrying...")
            await asyncio.sleep(RETRY_DELAY)

        if logger:
            logger.info(f"Retry cycle {attempt + 1}/{MAX_RETRIES}")
        else:
            print(f"Retry cycle {attempt + 1}/{MAX_RETRIES}")

    # ALL SERVERS FAILED
    if last_result is None:
        last_result = Result(
            error=RuntimeError("All servers unreachable after retries"),
            message="",
        )
    else:
        last_result.error = last_result.error or RuntimeError(
            "All servers failed after retries"
        )
    return last_result


def mars_via_ws_sync(
    server_list,
    requests,
    environ,
    target: str,
    logger=None,
) -> Result:
    return asyncio.run(mars_via_ws(server_list, requests, environ, target, logger=logger))


if __name__ == "__main__":
    # Minimal manual test (kept intentionally small)
    ws_url = [os.getenv("MARS_WS_URL", "ws://localhost:9001")]
    requests = [
        {
            "dataset": ["reanalysis"],
            "time": ["00:00:00"],
            "param": ["140212"],
            "class": ["ea"],
            "expect": ["any"],
            "number": ["all"],
            "levtype": ["sfc"],
            "date": "20240908/to/20240920",
        }
    ]
    environ = {
        "user_id": "37b6b138-0224-4875-b5c5-a3db813b6b01",
        "request_id": "c3cc4ee4-88b4-4b0c-912a-cfb32ee6b6c5",
        "namespace": "cci1:dev-pool",
        "host": "worker-dev-cci1-1",
        "username": "cci1:dev-pool:a3db813b6b01",
    }

    res = mars_via_ws_sync(ws_url, requests, environ, target="/tmp/foo.grib")
    print(res)
