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
import setproctitle

import websockets

from cads_mars_server.config import (
    SHARED_ROOT,
    HEARTBEAT_INTERVAL,
    WS_CLOSE_TIMEOUT,
    WS_PING_INTERVAL,
    DEBUG_MODE,
)

log = logging.getLogger("ws-mars")
log.setLevel(logging.DEBUG if DEBUG_MODE else logging.INFO)

# Heartbeat payload
HEARTBEAT_PAYLOAD = json.dumps({"type": "heartbeat"})


def safe_cleanup(*paths):
    """
    Safely remove files, logging any issues without raising exceptions.
    
    Args:
        *paths: Variable number of file paths to remove
    """
    for path in paths:
        if path is None:
            continue
        try:
            if os.path.exists(path):
                os.unlink(path)
                log.debug(f"Cleaned up: {path}")
        except Exception as e:
            log.warning(f"Failed to cleanup {path}: {e}")

async def handle_client(websocket):
    """
    Protocol:
        Client → Server:
            {"cmd": "start", "requests": [...], "environ": {...}, "target": "..."}
            {"cmd": "kill"}

        Server → Client:
            {"type": "log", "line": "..."}
            {"type": "state", "status": "...", ...}
    """

    loop = asyncio.get_running_loop()

    proc = None
    job_id = None
    workdir = None
    request_file = None
    result_file = None

    # -----------------------------------------------------
    # HEARTBEAT TASK (keep LBs & proxies from closing idle links)
    # -----------------------------------------------------
    async def heartbeat_task():
        try:
            while True:
                await asyncio.sleep(HEARTBEAT_INTERVAL)
                await websocket.send(HEARTBEAT_PAYLOAD)
        except websockets.exceptions.ConnectionClosed:
            pass

    hb_task = asyncio.create_task(heartbeat_task())

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
                # Validate all required fields BEFORE any operations
                try:
                    assert "requests" in msg, "Missing 'requests' field in message"
                    assert "environ" in msg, "Missing 'environ' field in message"
                    assert "target" in msg, "Missing 'target' field in message"
                    
                    environ = msg["environ"]
                    assert isinstance(environ, dict), "'environ' must be a dictionary"
                    
                    assert 'request_id' in environ, "Missing 'request_id' in environ"
                    assert 'user_id' in environ, "Missing 'user_id' in environ"
                    assert 'namespace' in environ, "Missing 'namespace' in environ"
                    assert 'host' in environ, "Missing 'host' in environ"
                    assert 'username' in environ, "Missing 'username' in environ"
                    
                    target_file = Path(msg["target"]).relative_to("/")
                    target_dir = target_file.parent
                    workdir = SHARED_ROOT / target_dir
                    
                    assert os.path.exists(workdir), f"Workdir {workdir} does not exist"
                    
                except AssertionError as exc:
                    log.error(f"Validation error: {exc}")
                    await websocket.send(json.dumps({
                        "type": "state",
                        "status": "error",
                        "error": str(exc)
                    }))
                    continue
                
                # All validations passed, now proceed with job setup
                requests = msg["requests"]
                requests = requests if isinstance(requests, list) else [requests]
                result_file = SHARED_ROOT / target_file
                job_id = environ["request_id"]
                
                log.info(f"Request received: {len(requests)} request(s) for job {job_id} to be executed in {workdir}")

                setproctitle.setproctitle(f"cads_mars_server {job_id}")

                request_file = workdir / f'{job_id}.mars'
                target_file_path = workdir / 'data.grib'

                # Build request.json used by your current HTTP server
                with open(request_file, "w") as f:
                    for request in requests:
                        f.write("RETRIEVE,\n")
                        for key, value in request.items():
                            if key.lower() != 'target':
                                f.write("{0}={1},\n".format(key, tidy(value)))
                        f.write(f"TARGET='{target_file_path}'\n")
                
                log.info(f"Written request file {request_file}")

                # Inform client
                await websocket.send(json.dumps({
                    "type": "state",
                    "status": "started",
                    "job_id": job_id,
                }))

                
                # ---------------------------------------------------
                # PTY — critical for real-time logs
                # ---------------------------------------------------
                master_fd, slave_fd = pty.openpty()

                env = os.environ.copy()
                for k, v in environ.items():
                    if v is not None:
                        env[f"MARS_ENVIRON_{k.upper()}"] = str(v)

                env.update({'MARS_AUTO_SPLIT_BY_DATES': '1'})

                # Launch mars binary
                proc = subprocess.Popen(
                    ["mars", str(request_file), '2>&1'],
                    env=env,
                    stdout=slave_fd,
                    stderr=slave_fd,
                    preexec_fn=os.setsid,
                    text=True,
                    bufsize=0,
                    close_fds=True,
                )

                os.close(slave_fd)

                # ---------------------------------------------------
                # THREAD: stream mars logs -> websocket
                # ---------------------------------------------------
                def stream_logs():
                    try:
                        with os.fdopen(master_fd) as f:
                            with open(f"{job_id}.log", "w") as _log:
                                for line in f:
                                    _log.write(line)
                                    line = line.rstrip("\n")
                                    log.debug(line)
                                    loop.call_soon_threadsafe(
                                        asyncio.create_task,
                                        websocket.send(json.dumps({
                                            "type": "log",
                                            "line": line
                                        }))
                                    )
                    except OSError as exc:
                        # OSError 5 = Input/output error (PTY closed) → safe to ignore
                        if exc.errno != 5:
                            log.error("Unexpected PTY error: %s", exc)
                        else:
                            return

                threading.Thread(target=stream_logs, daemon=True).start()


                # ---------------------------------------------------
                # THREAD: detect process end → send finished
                # ---------------------------------------------------
                def monitor_process():
                    rc = proc.wait()
                    try:
                        loop.call_soon_threadsafe(
                            asyncio.create_task,
                            websocket.send(json.dumps({
                                "type": "state",
                                "status": "finished",
                                "returncode": rc,
                                "job_id": job_id,
                            }))
                        )
                        # Clean up request file after successful completion
                        safe_cleanup(request_file)
                        loop.call_soon_threadsafe(asyncio.create_task, websocket.close())
                    except Exception as exc:
                        log.error("Error signaling finished: %s", exc)

                threading.Thread(target=monitor_process, daemon=True).start()

            # -------------------------
            # KILL JOB
            # -------------------------
            elif cmd == "kill":
                if proc and proc.poll() is None:
                    proc.terminate()
                    safe_cleanup(request_file, result_file)
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
                "job_id": job_id
            }))
            return
            
    except websockets.exceptions.ConnectionClosedOK:
        # Normal closure (client and server both sent Close 1000)
        log.debug("WebSocket closed normally (1000).")
        safe_cleanup(request_file)

    except websockets.exceptions.ConnectionClosedError as exc:
        # Abnormal close (not 1000)
        log.warning("WebSocket closed unexpectedly: %s", exc)
        safe_cleanup(request_file, result_file)

    except Exception as exc:
        log.error("WebSocket session failed: %s", exc)
        safe_cleanup(request_file, result_file)

    finally:
        hb_task.cancel()
        if proc and proc.poll() is None:
            proc.terminate()


def start_ws_server(host="0.0.0.0", port=9001):
    """
    Start the WebSocket server. Called from __main__.
    """
    logging.basicConfig(level=logging.INFO)
    log.info(f"Starting MARS WebSocket server on ws://{host}:{port}")

    return websockets.serve(handle_client, host, port)