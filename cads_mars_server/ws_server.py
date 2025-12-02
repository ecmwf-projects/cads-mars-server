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
log.setLevel(logging.DEBUG if os.getenv("MARS_WS_DEBUG") == "1" else logging.INFO)

# Shared filesystem root as seen by the **server**
SHARED_ROOT = Path("/cache")

# Heartbeat interval to keep LB from killing idle sessions
HEARTBEAT_INTERVAL = 20      # seconds
HEARTBEAT_PAYLOAD = json.dumps({"type": "heartbeat"})

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
                requests = msg.get("requests", [{}])
                requests = requests if isinstance(requests, list) else [requests]

                environ = msg.get("environ", {})
                target_file = Path(msg.get("target", "")).relative_to("/")
                target_dir = target_file.parent
                workdir = SHARED_ROOT / target_dir
                log.info(f"Request received: {requests} {environ} to be executed in {workdir}")
                result_file = SHARED_ROOT / target_file

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
                    "job_id": job_id,
                }))

                
                # ---------------------------------------------------
                # PTY — critical for real-time logs
                # ---------------------------------------------------
                master_fd, slave_fd = pty.openpty()

                # Launch mars binary via same logic as your server
                proc = subprocess.Popen(
                    ["mars", str(request_file), '2>&1'],
                    cwd=str(workdir),
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
                            for line in f:
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
                        tidy(workdir)
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
                "job_id": job_id
            }))
            return
    except AssertionError as exc:
        log.error("Assertion error: %s", exc)
        await websocket.send(json.dumps({
            "type": "state",
            "status": "error",
            "error": str(exc),
        }))

    except websockets.exceptions.ConnectionClosedOK:
        # Normal closure (client and server both sent Close 1000)
        log.debug("WebSocket closed normally (1000).")
        pass

    except websockets.exceptions.ConnectionClosedError as exc:
        # Abnormal close (not 1000)
        log.warning("WebSocket closed unexpectedly: %s", exc)

    except Exception as exc:
        log.error("WebSocket session failed: %s", exc)

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