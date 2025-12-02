import json
import asyncio
import websockets
from pathlib import Path

import asyncio
import websockets
import json
import random
import time

RETRY_DELAY = 2
MAX_RETRIES = 10
REQUEST_TIMEOUT = 30


async def mars_via_ws(server_list, requests, environ, target, logger=None):
    """
    server_list: list of ws://host:port
    request_payload: mars request (your JSON)
    environ: your environment dict (copied from original client)
    target_dir: where result will be placed in worker FS
    """

    logs = []
    result_file = None
    returncode = None

    # ---------------------------
    # TRY SERVERS IN RANDOM ORDER
    # ---------------------------
    servers = server_list[:]
    random.shuffle(servers)

    for attempt in range(MAX_RETRIES):
        for ws_url in servers:
            if logger:
                logger.info(f"Connecting to MARS server at {ws_url}")
            try:
                async with websockets.connect(
                    ws_url, ping_interval=None, close_timeout=30
                ) as ws:

                    # ---------------------------
                    # SEND START COMMAND
                    # ---------------------------
                    await ws.send(json.dumps({
                        "cmd": "start",
                        "requests": requests,
                        "environ": environ,
                        "target": target
                    }))

                    # ---------------------------
                    # RECEIVE JOB STREAM
                    # ---------------------------
                    async for raw in ws:
                        msg = json.loads(raw)
                        mtype = msg.get("type")

                        if mtype == "heartbeat":
                            if logger:
                                logger.info(f"Received heartbeat from {ws_url}")
                            continue

                        if mtype == "log":
                            logs.append(msg["line"])
                            if logger:
                                logger.info(msg["line"])
                            else:
                                print(msg["line"])
                            continue

                        if mtype == "state":
                            if msg["status"] == "started":
                                if logger:
                                    logger.info(f"Job started on server {ws_url}")
                                else:
                                    print(f"Job started on server {ws_url}")
                                continue

                            if msg["status"] == "error":
                                logs.append(f"Error from server: {msg['error']}")
                                if logger:
                                    logger.error(f"Error from server: {msg['error']}")
                                else:
                                    print(f"Error from server: {msg['error']}")
                                assert False, f"Server error: {msg['error']}"

                            if msg["status"] == "finished":
                                returncode = msg["returncode"]
                                return logs, returncode

                            if msg["status"] == "killed":
                                return logs, -9

            except (websockets.exceptions.ConnectionClosedError,
                    websockets.exceptions.InvalidStatusCode,
                    ConnectionRefusedError,
                    TimeoutError):
                if logger:
                    logger.warning(f"Server {ws_url} failed; retrying...")
                else:
                    print(f"Server {ws_url} failed; retrying...")
                await asyncio.sleep(RETRY_DELAY)

        if logger:
            logger.info(f"Retry cycle {attempt+1}/{MAX_RETRIES}")
        else:
            print(f"Retry cycle {attempt+1}/{MAX_RETRIES}")
    # ---------------------------
    # ALL SERVERS FAILED
    # ---------------------------
    raise RuntimeError("All servers unreachable after retries")


def mars_via_ws_sync(server_list, request_payload, environ, target, logger=None):
    logs, returncode = asyncio.run(
        mars_via_ws(server_list, request_payload, environ, target, logger=logger)
    )
    return {'message': logs, 'returncode': returncode}

if __name__ == "__main__":
    import os
    ws_url = [os.getenv("MARS_WS_URL", "ws://localhost:9001")]

    # Example usage
    requests = [{
        'dataset': ['reanalysis'],
        'time': [f'{hour:02d}:00:00'],
        'param': ['140212'],
        'class': ['ea'],
        'expect': ['any'],
        'number': ['all'],
        'levtype': ['sfc'],
        'date': '20240908/to/20240920'
    } for hour in range(24)]
    environ = {
        'user_id': '37b6b138-0224-4875-b5c5-a3db813b6b01',
        'request_id': 'c3cc4ee4-88b4-4b0c-912a-cfb32ee6b6c5',
        'namespace': 'cci1:dev-pool',
        'host': 'worker-dev-cci1-1',
        'username': 'cci1:dev-pool:a3db813b6b01'
    }
    logs, returncode = asyncio.run(
        mars_via_ws(ws_url, requests, environ, target="/download-cci1-0006/foo.grib")
    )

    print(f"Return code: {returncode}")