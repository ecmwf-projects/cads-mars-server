import asyncio
import json
import logging
import os
import signal
import pty
import subprocess
import threading
from pathlib import Path
from cads_mars_server.server import tidy

import websockets

log = logging.getLogger("ws-mars")
log.setLevel(logging.DEBUG)

# Shared filesystem root as seen by the **server**


async def handle_client(websocket):
    """
    Protocol:
        Client → Server:
            {"cmd": "start", "requests": [...], "environ": {...}, "target_dir": "..."}
            {"cmd": "kill"}

        Server → Client:
            {"type": "log", "line": "..."}
            {"type": "state", "status": "...", ...}
    """
    SHARED_ROOT = Path("/cache")

    proc = None
    job_id = None
    workdir = None

    loop = asyncio.get_running_loop()

    try:
        async for raw in websocket:
            try:
                msg = json.loads(raw)
            except Exception:
                await websocket.send(json.dumps({
                    "type": "state",
                    "status": "error",
                    "error": "Invalid JSON"
                }))
                continue

            cmd = msg.get("cmd")

            # -------------------------
            # START JOB
            # -------------------------
            if cmd == "start":
                requests = msg.get("requests", [{}])
                requests = requests if isinstance(requests, list) else [requests]

                environ = msg.get("environ", {})
                target_dir = Path(msg.get("target_dir", "")).relative_to("/")
                workdir = SHARED_ROOT / target_dir
                log.info(f"Request received: {requests} {environ} to be executed in {workdir}")
                result_file = target_dir / 'data.grib'

                assert os.path.exists(workdir), f"Workdir {workdir} does not exist"
                assert 'request_id' in environ, "Missing request_id in environ"
                assert 'user_id' in environ, "Missing user_id in environ"
                assert 'namespace' in environ, "Missing namespace in environ"
                assert 'host' in environ, "Missing host in environ"
                assert 'username' in environ, "Missing username in environ"

                job_id = environ.get("request_id")
                request_file = workdir / f'{job_id}.mars'
                target_file = workdir / 'data.grib'

                # Build request.json used by your current HTTP server
                with open(request_file, "w") as f:
                    for request in requests:
                        f.write("RETRIEVE,\n")
                        for key, value in request.items():
                            if key.lower() != 'target':
                                f.write("{0}={1},\n".format(key, tidy(value)))
                        f.write(f"TARGET='{target_file}'\n")
                
                log.info(f"Written request file {request_file}")

                # Inform client
                await websocket.send(json.dumps({
                    "type": "state",
                    "status": "started",
                    "job_id": job_id
                }))

                
                # ---------------------------------------------------
                # PTY — critical for real-time logs
                # ---------------------------------------------------
                master_fd, slave_fd = pty.openpty()

                # Launch mars binary via same logic as your server
                proc = subprocess.Popen(
                    ["mars", str(request_file), '2>&1'],
                    cwd=str(workdir),
                    stdout=subprocess.PIPE,
                    stderr=subprocess.STDOUT,
                    text=True,
                    bufsize=1,
                )

                os.close(slave_fd)

                # ---------------------------------------------------
                # THREAD: stream mars logs -> websocket
                # ---------------------------------------------------
                def stream_logs():
                    try:
                        with os.fdopen(master_fd) as f:
                            for line in f:
                                line = line.rstrip("\n")
                                loop.call_soon_threadsafe(
                                    asyncio.create_task,
                                    websocket.send(json.dumps({
                                        "type": "log",
                                        "line": line
                                    }))
                                )
                    except Exception as exc:
                        log.error("log streaming thread failed: %s", exc)

                threading.Thread(target=stream_logs, daemon=True).start()

            # -------------------------
            # KILL JOB
            # -------------------------
            elif cmd == "kill":
                if proc and proc.poll() is None:
                    proc.terminate()
                    try:
                        os.unlink(request_file)
                        os.unlink(result_file)
                    except FileNotFoundError:
                        pass
                    await websocket.send(json.dumps({
                        "type": "state",
                        "status": "killed",
                        "job_id": job_id,
                    }))
                else:
                    await websocket.send(json.dumps({
                        "type": "state",
                        "status": "error",
                        "error": "No running job",
                    }))

        # EXIT LOOP → JOB FINISHED
        if proc:
            rc = proc.wait()
            await websocket.send(json.dumps({
                "type": "state",
                "status": "finished",
                "returncode": rc,
                "job_id": job_id,
                "result": str(result_file) if rc == 0 else None,
            }))
            return

    except Exception as exc:
        log.error("WebSocket session failed: %s", exc)
        if proc and proc.poll() is None:
            proc.terminate()


def start_ws_server(host="0.0.0.0", port=9001):
    """
    Start the WebSocket server. Called from __main__.
    """
    logging.basicConfig(level=logging.INFO)
    log.info(f"Starting MARS WebSocket server on ws://{host}:{port}")

    return websockets.serve(handle_client, host, port)