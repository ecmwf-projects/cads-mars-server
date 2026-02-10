import asyncio
import json
import os
import random
from typing import Any, Iterable, Optional

import websockets

from cads_mars_server.client import Result
from cads_mars_server.config import (
    CLIENT_FILTER_LOGS,
    MAX_RETRIES,
    REQUEST_TIMEOUT,
    RETRY_DELAY,
    WS_CLOSE_TIMEOUT,
    WS_PING_INTERVAL,
)
from cads_mars_server.log_filter import (
    LogHandler,
    create_default_log_handler,
)


async def _run_one_server(
    ws_url: str,
    requests: list[dict[str, Any]],
    environ: dict[str, Any],
    target: str,
    logger=None,
    log_handler: Optional[LogHandler] = None,
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
                logs.append(line)  # Always store raw line

                # Process log line with custom handler
                if log_handler:
                    try:
                        display_line = await log_handler(line, ws, logger)
                        if display_line:
                            if logger:
                                # Use appropriate log level based on content
                                if "❌" in display_line:
                                    logger.error(display_line)
                                elif "⚠️" in display_line:
                                    logger.warning(display_line)
                                else:
                                    logger.info(display_line)
                            else:
                                print(display_line)
                    except Exception as e:
                        # Log handler raised exception - abort request
                        if logger:
                            logger.error(f"Log handler aborted request: {e}")
                        raise
                else:
                    # No handler, show raw logs
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
                            error=RuntimeError(
                                f"MARS job failed (returncode={returncode})"
                            ),
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
    filter_logs: Optional[bool] = None,
    log_handler: Optional[LogHandler] = None,
) -> Result:
    """
    Execute a MARS retrieval via websocket servers.

    Args:
        server_list: List of WebSocket server URLs to try
        requests: MARS request dict or list of dicts
        environ: Environment variables for MARS
        target: Target file path for output
        logger: Optional logger instance
        filter_logs: If True/False, enables/disables default log filtering.
                     If None (default), uses CLIENT_FILTER_LOGS config setting.
                     Ignored if log_handler is provided.
        log_handler: Optional custom log handler function. If provided, this takes
                     precedence over filter_logs. The handler receives (line, ws, logger)
                     and should return formatted line or None to suppress.
                     Can raise exceptions to abort request or send commands to ws.

    Returns a :class:`~cads_mars_server.client.Result` instance (same shape as the HTTP client),
    so callers can uniformly handle success/failure.
    """
    # Use custom log handler if provided, otherwise create default handler
    if log_handler is None:
        # Use filter_logs to determine if we should filter
        if filter_logs is None:
            filter_logs = CLIENT_FILTER_LOGS
        log_handler = create_default_log_handler(filter_logs=filter_logs)

    reqs = requests if isinstance(requests, list) else [requests]

    servers = list(server_list)
    random.shuffle(servers)

    last_result: Result | None = None

    for attempt in range(MAX_RETRIES):
        for ws_url in servers:
            try:
                # Apply an overall timeout to each server attempt.
                last_result = await asyncio.wait_for(
                    _run_one_server(
                        ws_url,
                        reqs,
                        environ,
                        target,
                        logger=logger,
                        log_handler=log_handler,
                    ),
                    timeout=REQUEST_TIMEOUT,
                )
                # If the server returned an error, we try the next server (or next retry cycle).
                if not last_result.error:
                    return last_result
            except (
                websockets.exceptions.ConnectionClosedError,
                websockets.exceptions.InvalidStatus,
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
    filter_logs: Optional[bool] = None,
    log_handler: Optional[LogHandler] = None,
) -> Result:
    return asyncio.run(
        mars_via_ws(
            server_list,
            requests,
            environ,
            target,
            logger=logger,
            filter_logs=filter_logs,
            log_handler=log_handler,
        )
    )


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
