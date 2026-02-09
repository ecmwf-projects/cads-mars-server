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

# Track all spawned processes for cleanup on shutdown
active_processes = set()
active_processes_lock = threading.Lock()


def check_cephfs_health():
    """
    Check CephFS health and report MDS/OSD connection issues.
    Returns dict with health info and warnings.
    
    Note: OSD connection errors (like 'osd583 10.106.20.41:6971 socket closed')
    indicate storage backend issues. These IPs are OSDs (data storage nodes),
    not monitors from your mount string. You cannot fix OSD issues by changing
    mount monitors - contact storage team about failing OSDs.
    """
    health_info = {
        "mds_issues": [],
        "osd_issues": [],
        "mount_info": None,
        "recent_errors": []
    }
    
    try:
        # Check for CephFS mount points
        with open("/proc/mounts", "r") as f:
            for line in f:
                if "type ceph" in line:
                    parts = line.split()
                    health_info["mount_info"] = {
                        "source": parts[0],
                        "mountpoint": parts[1],
                        "options": parts[3]
                    }
                    log.info(f"CephFS mounted: {parts[0]} on {parts[1]}")
                    break
        
        # Check recent dmesg for ceph errors
        try:
            result = subprocess.run(
                ["dmesg", "-T", "--level=err,warn", "|", "grep", "-i", "ceph", "|", "tail", "-20"],
                shell=True,
                capture_output=True,
                text=True,
                timeout=5
            )
            if result.stdout.strip():
                errors = result.stdout.strip().split('\n')
                health_info["recent_errors"] = errors[-10:]  # Last 10 errors
                
                # Parse for specific OSD/MDS issues
                for error in errors[-10:]:
                    if "osd" in error.lower() and "socket closed" in error.lower():
                        # Extract OSD number and IP
                        import re
                        match = re.search(r'osd(\d+).*?(\d+\.\d+\.\d+\.\d+)', error)
                        if match:
                            osd_num, ip = match.groups()
                            health_info["osd_issues"].append({"osd": osd_num, "ip": ip, "error": "socket_closed"})
                    elif "mds" in error.lower():
                        health_info["mds_issues"].append(error)
        except Exception as e:
            log.debug(f"Could not check dmesg: {e}")
        
        # Try to get MDS session info from debugfs
        try:
            debugfs_paths = Path("/sys/kernel/debug/ceph").glob("*/mdsc")
            for mdsc_path in debugfs_paths:
                try:
                    with open(mdsc_path, "r") as f:
                        mdsc_info = f.read()
                        # Look for active MDS sessions
                        if "mds" in mdsc_info:
                            log.debug(f"MDS session info: {mdsc_info[:200]}")
                except PermissionError:
                    log.debug("No permission to read MDS debug info (needs root)")
                except Exception as e:
                    log.debug(f"Could not read MDS debug info: {e}")
        except Exception as e:
            log.debug(f"Could not access ceph debugfs: {e}")
        
        # Report findings
        if health_info["osd_issues"]:
            unique_osds = {}
            for issue in health_info["osd_issues"]:
                key = (issue["osd"], issue["ip"])
                unique_osds[key] = issue
            log.warning(f"Detected {len(unique_osds)} OSDs with connection issues:")
            for (osd, ip), issue in unique_osds.items():
                log.warning(f"  - OSD {osd} at {ip}: {issue['error']}")
            log.warning(
                "NOTE: These are storage backend (OSD) IPs, not mount string monitor IPs. "
                "Contact storage team about these failing OSDs - "
                "changing mount monitors will not resolve this issue."
            )
        
        if health_info["mds_issues"]:
            log.warning(f"Detected {len(health_info['mds_issues'])} MDS issues in logs")
            for issue in health_info["mds_issues"][:5]:
                log.warning(f"  - {issue}")
        
    except Exception as e:
        log.warning(f"Error checking CephFS health: {e}")
    
    return health_info


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
                        # Kill the entire process group
                        os.killpg(os.getpgid(proc.pid), signal.SIGTERM)
                        time.sleep(2)
                        if proc.poll() is None:
                            log.warning(f"Force killing process group for PID {proc.pid}")
                            os.killpg(os.getpgid(proc.pid), signal.SIGKILL)
                        safe_cleanup(request_file, result_file)
                    except (ProcessLookupError, OSError) as e:
                        log.debug(f"Process {proc.pid} already terminated: {e}")
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
                
                # Track this process for cleanup on shutdown
                with active_processes_lock:
                    active_processes.add(proc)

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
                    
                    # Send completion signal and close connection IMMEDIATELY
                    # Don't wait for fsync - it can take 100-800ms due to CephFS backend issues
                    # This frees up the connection slot for new requests
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
                    
                    # NOW do fsync AFTER connection closed (non-blocking for client)
                    # Rationale: Given CephFS backend OSD issues causing 100-800ms fsync delays,
                    # it's better to notify client immediately and free up connection slot.
                    # If client on different VM tries to read before fsync completes, they will
                    # either wait for data or get an error - acceptable tradeoff vs blocking connections.
                    if rc == 0 and os.path.exists(target_file_path):
                        try:
                            file_size = os.path.getsize(target_file_path)
                            log.info(f"Starting background sync of output file {target_file_path} (size: {file_size / 1024 / 1024:.2f} MB)")
                            sync_start = time.time()
                            
                            # Open file and explicitly fsync to flush all data to the filesystem
                            with open(target_file_path, 'rb') as f:
                                os.fsync(f.fileno())
                            
                            sync_duration = time.time() - sync_start
                            
                            # Warn if fsync is unusually slow (potential MDS/OSD issues)
                            if sync_duration > 0.5:  # 500ms threshold
                                log.warning(
                                    f"⚠️  SLOW FSYNC: {sync_duration:.3f}s for {target_file_path} "
                                    f"({file_size / 1024 / 1024:.2f} MB) - CephFS storage backend issue. "
                                    f"This typically indicates OSD connection problems (reconnects/failovers). "
                                    f"Check 'dmesg -T | grep -i ceph' for OSD socket errors. "
                                    f"Run 'check-cephfs-health' for detailed diagnostics."
                                )
                            else:
                                log.info(f"Output file {target_file_path} successfully synced to CephFS in {sync_duration:.3f} seconds")
                        except Exception as e:
                            log.warning(f"Failed to sync output file: {e}")

                threading.Thread(target=monitor_process, daemon=True).start()

            # -------------------------
            # KILL JOB
            # -------------------------
            elif cmd == "kill":
                if proc and proc.poll() is None:
                    log.info(f"Killing MARS process for job {job_id} (PID {proc.pid}) at client request")
                    try:
                        # Kill the entire process group
                        os.killpg(os.getpgid(proc.pid), signal.SIGTERM)
                    except (ProcessLookupError, OSError) as e:
                        log.debug(f"Process {proc.pid} already terminated: {e}")
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
        
        # Kill MARS process and its children if still running
        if proc:
            try:
                if proc.poll() is None:
                    log.info(f"Cleaning up MARS process (PID {proc.pid}) for job {job_id}")
                    try:
                        # Kill the entire process group (mars + bash + any children)
                        os.killpg(os.getpgid(proc.pid), signal.SIGTERM)
                        try:
                            proc.wait(timeout=5)
                        except subprocess.TimeoutExpired:
                            log.warning(f"Force killing process group (PID {proc.pid}) for job {job_id}")
                            os.killpg(os.getpgid(proc.pid), signal.SIGKILL)
                            proc.wait()
                    except (ProcessLookupError, OSError) as e:
                        log.debug(f"Process {proc.pid} already terminated: {e}")
                
                # Remove from active processes tracking
                with active_processes_lock:
                    active_processes.discard(proc)
            except Exception as e:
                log.error(f"Error cleaning up process: {e}")
        
        # Decrement active connection count
        with active_connections_lock:
            active_connections -= 1
            current_count = active_connections
        log.info(f"Connection from {client_addr} closed (active: {current_count}/{MAX_CONCURRENT_CONNECTIONS if MAX_CONCURRENT_CONNECTIONS > 0 else 'unlimited'})")


def cleanup_orphaned_processes():
    """
    Kill any orphaned mars/bash processes from previous crashes.
    This prevents process accumulation during restart loops.
    """
    try:
        # Find orphaned mars processes
        result = subprocess.run(
            ["pgrep", "-f", "mars.*\.request"],
            capture_output=True,
            text=True
        )
        if result.returncode == 0 and result.stdout.strip():
            orphaned_pids = result.stdout.strip().split('\n')
            log.info(f"Found {len(orphaned_pids)} orphaned mars processes from previous run")
            for pid in orphaned_pids:
                try:
                    # Kill the process group
                    pgid = os.getpgid(int(pid))
                    os.killpg(pgid, signal.SIGTERM)
                    log.info(f"Killed orphaned process group {pgid}")
                except (ProcessLookupError, OSError) as e:
                    log.debug(f"Process {pid} already gone: {e}")
    except Exception as e:
        log.warning(f"Error cleaning up orphaned processes: {e}")


def kill_all_active_processes():
    """
    Kill all active MARS processes. Called on shutdown.
    """
    with active_processes_lock:
        if active_processes:
            log.info(f"Killing {len(active_processes)} active MARS processes")
            for proc in list(active_processes):
                try:
                    if proc.poll() is None:
                        # Kill the entire process group
                        os.killpg(os.getpgid(proc.pid), signal.SIGKILL)
                        log.info(f"Killed process group for PID {proc.pid}")
                except (ProcessLookupError, OSError) as e:
                    log.debug(f"Process {proc.pid} already terminated: {e}")
            active_processes.clear()


def start_ws_server(host="0.0.0.0", port=9001):
    """
    Start the WebSocket server. Called from __main__.
    """
    logging.basicConfig(level=logging.INFO)
    
    # Clean up any orphaned processes from previous crashes
    log.info("Checking for orphaned processes from previous run...")
    cleanup_orphaned_processes()
    
    # Check CephFS health and report issues
    log.info("Checking CephFS health...")
    cephfs_health = check_cephfs_health()
    if cephfs_health["mount_info"]:
        log.info(f"CephFS mounted from: {cephfs_health['mount_info']['source']}")
        if cephfs_health["osd_issues"] or cephfs_health["mds_issues"]:
            log.warning(
                f"⚠️  CephFS backend issues detected: "
                f"{len(cephfs_health['osd_issues'])} OSD connection problems, "
                f"{len(cephfs_health['mds_issues'])} MDS problems. "
                f"Performance may be degraded (slow fsync, high latency). "
                f"These are storage cluster issues - contact storage team with OSD details."
            )
    else:
        log.warning("No CephFS mount detected - file sync may not work correctly")
    
    log.info(f"Starting MARS WebSocket server on ws://{host}:{port}")
    if MAX_CONCURRENT_CONNECTIONS > 0:
        log.info(f"Max concurrent connections: {MAX_CONCURRENT_CONNECTIONS}")
    else:
        log.info("Max concurrent connections: unlimited (configure MARS_MAX_CONCURRENT_CONNECTIONS to set a limit)")
    
    # Start metrics export thread if enabled
    if METRICS_ENABLED:
        log.info(f"Metrics collection enabled, exporting to {METRICS_FILE} every {METRICS_EXPORT_INTERVAL}s")
        metrics_thread = threading.Thread(target=periodic_metrics_export, daemon=True)
        metrics_thread.start()
    
    return websockets.serve(handle_client, host, port)