import asyncio
import json
import logging
import os
import signal
import pty
import subprocess
import threading
import time
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
    MAX_CONCURRENT_CONNECTIONS,
)

log = logging.getLogger("ws-mars")
log.setLevel(logging.DEBUG if DEBUG_MODE else logging.INFO)

# Track active connections
active_connections = 0
active_connections_lock = threading.Lock()


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
    
    global active_connections
    
    client_addr = f"{websocket.remote_address[0]}:{websocket.remote_address[1]}" if websocket.remote_address else "unknown"
    
    # Check connection limit BEFORE incrementing
    with active_connections_lock:
        if MAX_CONCURRENT_CONNECTIONS > 0 and active_connections >= MAX_CONCURRENT_CONNECTIONS:
            log.warning(f"Connection limit reached ({MAX_CONCURRENT_CONNECTIONS}), rejecting connection from {client_addr}")
            await websocket.close(1008, f"Server at capacity ({MAX_CONCURRENT_CONNECTIONS} connections)")
            return
        active_connections += 1
        current_count = active_connections
    
    log.info(f"New WebSocket connection from {client_addr} (active: {current_count}/{MAX_CONCURRENT_CONNECTIONS if MAX_CONCURRENT_CONNECTIONS > 0 else 'unlimited'})")

    loop = asyncio.get_running_loop()

    proc = None
    job_id = None
    workdir = None
    request_file = None
    result_file = None
    hb_task = None
    job_running = False  # Flag to track if a job is actively running

    # Helper to safely send messages from threads, catching connection close exceptions
    async def safe_send(message):
        """
        Send message, catching normal connection close exceptions.
        Use this for sends from threads to avoid orphaned task exceptions.
        Do NOT use in heartbeat_task - it needs ConnectionClosed to propagate for cleanup.
        """
        try:
            await websocket.send(message)
        except (websockets.exceptions.ConnectionClosedOK, websockets.exceptions.ConnectionClosedError) as e:
            # Connection closed - this is normal during shutdown, don't spam logs
            log.debug(f"Could not send message, connection closed: {e}")
        except Exception as e:
            log.error(f"Unexpected error sending message: {e}")
    
    async def safe_close():
        """Safely close websocket, catching exceptions."""
        try:
            await websocket.close()
        except Exception as e:
            log.debug(f"Error closing websocket: {e}")

    try:
        # -----------------------------------------------------
        # HEARTBEAT TASK (monitor process health and detect client disconnect)
        # -----------------------------------------------------
        async def heartbeat_task():
            """Send heartbeat with process status. If client disconnects, kill MARS process."""
            try:
                while True:
                    await asyncio.sleep(HEARTBEAT_INTERVAL)
                    
                    # Build heartbeat with process status
                    heartbeat = {
                        "type": "heartbeat",
                        "timestamp": time.time(),
                    }
                    
                    if job_running and proc:
                        # Check if process is still running
                        poll_result = proc.poll()
                        if poll_result is None:
                            # Process still running
                            heartbeat["job_status"] = "running"
                            heartbeat["job_id"] = job_id
                            heartbeat["pid"] = proc.pid
                        else:
                            # Process finished (will be handled by monitor_process)
                            heartbeat["job_status"] = "finished"
                            heartbeat["job_id"] = job_id
                    else:
                        heartbeat["job_status"] = "idle"
                    
                    # Send directly (not via safe_send) so ConnectionClosed propagates for cleanup
                    await websocket.send(json.dumps(heartbeat))
                    
            except websockets.exceptions.ConnectionClosed:
                # Client disconnected - kill MARS process if running
                if job_running and proc and proc.poll() is None:
                    log.warning(f"Client {client_addr} disconnected while job {job_id} running (PID {proc.pid}). Terminating MARS process.")
                    try:
                        proc.terminate()
                        time.sleep(2)
                        if proc.poll() is None:
                            proc.kill()  # Force kill if terminate didn't work
                        safe_cleanup(request_file, result_file)
                    except Exception as e:
                        log.error(f"Error killing MARS process: {e}")
            except Exception as e:
                log.error(f"Heartbeat task error: {e}")

        hb_task = asyncio.create_task(heartbeat_task())
        async for raw in websocket:
            try:
                msg = json.loads(raw)
            except Exception as e:
                log.warning(f"Invalid JSON from {client_addr}: {e}")
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
                    log.error(f"Validation error from {client_addr}: {exc}")
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

                # Mark job as running
                job_running = True
                
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
                                    # Use safe_send to avoid orphaned task exceptions
                                    loop.call_soon_threadsafe(
                                        asyncio.create_task,
                                        safe_send(json.dumps({
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
                    nonlocal job_running
                    rc = proc.wait()
                    job_running = False  # Mark job as no longer running
                    
                    # CRITICAL: Ensure output file is flushed to CephFS before signaling completion
                    # This prevents cache coherency issues when clients on different VMs try to access the file
                    if rc == 0 and os.path.exists(target_file_path):
                        try:
                            file_size = os.path.getsize(target_file_path)
                            log.info(f"Starting sync of output file {target_file_path} (size: {file_size / 1024 / 1024:.2f} MB)")
                            sync_start = time.time()
                            
                            # Open file and explicitly fsync to flush all data to the filesystem
                            with open(target_file_path, 'rb') as f:
                                os.fsync(f.fileno())
                            
                            sync_duration = time.time() - sync_start
                            log.info(f"Output file {target_file_path} successfully synced to CephFS in {sync_duration:.3f} seconds")
                        except Exception as e:
                            log.warning(f"Failed to sync output file: {e}")
                    
                    try:
                        # Use safe_send to avoid orphaned task exceptions
                        loop.call_soon_threadsafe(
                            asyncio.create_task,
                            safe_send(json.dumps({
                                "type": "state",
                                "status": "finished",
                                "returncode": rc,
                                "job_id": job_id,
                            }))
                        )
                        # Clean up request file after successful completion
                        safe_cleanup(request_file)
                        # Use safe_close to avoid exceptions from closing websocket
                        loop.call_soon_threadsafe(
                            asyncio.create_task,
                            safe_close()
                        )
                    except Exception as exc:
                        log.error("Error signaling finished: %s", exc)

                threading.Thread(target=monitor_process, daemon=True).start()

            # -------------------------
            # KILL JOB
            # -------------------------
            elif cmd == "kill":
                if proc and proc.poll() is None:
                    log.info(f"Killing MARS process for job {job_id} (PID {proc.pid}) at client request")
                    proc.terminate()
                    job_running = False
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
        log.debug(f"WebSocket from {client_addr} closed normally (1000).")
        if proc and proc.poll() is None:
            log.info(f"Job {job_id} still running (PID {proc.pid}), terminating due to normal client disconnect")
        safe_cleanup(request_file)

    except websockets.exceptions.ConnectionClosedError as exc:
        # Abnormal close (not 1000)
        log.warning(f"WebSocket from {client_addr} closed unexpectedly: %s", exc)
        if proc and proc.poll() is None:
            log.warning(f"Job {job_id} still running (PID {proc.pid}), terminating due to abnormal disconnect")
        safe_cleanup(request_file, result_file)

    except Exception as exc:
        log.error(f"WebSocket session from {client_addr} failed: %s", exc)
        safe_cleanup(request_file, result_file)

    finally:
        # Always cancel heartbeat and cleanup
        if hb_task:
            hb_task.cancel()
        if proc and proc.poll() is None:
            proc.terminate()
        
        # Decrement active connection count
        with active_connections_lock:
            active_connections -= 1
            current_count = active_connections
        log.info(f"Connection from {client_addr} closed (active: {current_count}/{MAX_CONCURRENT_CONNECTIONS if MAX_CONCURRENT_CONNECTIONS > 0 else 'unlimited'})")


def start_ws_server(host="0.0.0.0", port=9001):
    """
    Start the WebSocket server. Called from __main__.
    """
    logging.basicConfig(level=logging.INFO)
    log.info(f"Starting MARS WebSocket server on ws://{host}:{port}")
    if MAX_CONCURRENT_CONNECTIONS > 0:
        log.info(f"Max concurrent connections: {MAX_CONCURRENT_CONNECTIONS}")
    else:
        log.info("Max concurrent connections: unlimited (configure MARS_MAX_CONCURRENT_CONNECTIONS to set a limit)")

    return websockets.serve(handle_client, host, port)