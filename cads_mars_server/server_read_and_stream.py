"""
MARS server that writes data to a file, then streams it back to the client.

This is a third execution mode alongside:
- server.py:    pipe mode — MARS writes to a pipe, data streamed in real time.
- ws_server.py: shared-volume mode — MARS writes to CephFS, client reads directly.

Here MARS writes to a local/generic directory.  Once the process finishes the
server reads the file back and streams it to the client over the same chunked
HTTP protocol that ``client.py`` (RemoteMarsClient) already speaks.

The protocol re-uses the ENDR / EROR / RWND markers so the existing
``RemoteMarsClient`` / ``RemoteMarsClientCluster`` work without changes.
"""

import http.server
import json
import logging
import os
import re
import signal
import socket
import socketserver
import tempfile
import time
import uuid

import setproctitle

from .tools import bytes

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(process)d %(levelname)s %(module)s - %(funcName)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

LOG = logging.getLogger(__name__)
ACCEPT_SOCKET = None


def validate_uuid(uid):
    return re.match(r"^[a-f0-9-]{36}$", uid)


# --------------------------------------------------------------------------- #
# Data-directory resolution — distribute across shared volumes
# --------------------------------------------------------------------------- #

def _resolve_datadir(uid, shared_root, shares, cache_folder):
    """Return a data directory for *uid*, spreading across *shares*.

    The path is ``{shared_root}/{share}/{cache_folder}`` where *share* is
    chosen from *shares* using a hash of *uid* so that requests are evenly
    distributed.  If no share is usable (missing / not a directory) the
    function falls back to ``/tmp``.
    """
    if shares:
        idx = hash(uid) % len(shares)
        ordered = shares[idx:] + shares[:idx]
        for share in ordered:
            candidate = os.path.join(shared_root, share, cache_folder)
            parent = os.path.join(shared_root, share)
            if os.path.isdir(parent):
                os.makedirs(candidate, exist_ok=True)
                LOG.info(f"{uid} Using data directory: {candidate}")
                return candidate
            LOG.warning(f"{uid} Share volume not available: {parent}")

    LOG.warning(f"{uid} No shared volumes available, falling back to /tmp")
    return tempfile.mkdtemp(prefix="mars_stream_")


# --------------------------------------------------------------------------- #
# Request handling — reuse the same tidying logic from server.py
# --------------------------------------------------------------------------- #
from .server import tidy  # noqa: E402


def run_mars(*, mars_executable, request, uid, logdir, environ, datadir):
    """Fork and exec MARS, writing output to *datadir*/<uid>.grib.

    Returns (target_path, pid).
    """
    target_path = os.path.join(datadir, f"{uid}.grib")

    request_pipe_r, request_pipe_w = os.pipe()
    os.set_inheritable(request_pipe_r, True)
    os.set_inheritable(request_pipe_w, True)

    pid = os.fork()

    if pid:
        # Parent — feed the request and return
        if isinstance(request, dict):
            requests = [request]
        else:
            requests = request

        assert isinstance(requests, list)

        def out(text):
            text = text.encode()
            assert os.write(request_pipe_w, text) == len(text)

        for req in requests:
            out("RETRIEVE,\n")
            for key, value in req.items():
                out("{0}={1},\n".format(key, tidy(value)))
            out("TARGET='{0}'\n".format(target_path))

        os.close(request_pipe_r)
        os.close(request_pipe_w)

        return target_path, pid

    # ----- child process -----
    os.dup2(request_pipe_r, 0)
    os.close(request_pipe_w)

    out_fd = os.open(
        os.path.join(logdir, f"{uid}.log"),
        os.O_WRONLY | os.O_CREAT | os.O_TRUNC,
        0o644,
    )
    os.dup2(out_fd, 1)
    os.dup2(out_fd, 2)

    env = dict(os.environ)
    for k, v in environ.items():
        if v is not None:
            env[f"MARS_ENVIRON_{k.upper()}"] = str(v)
    env.setdefault("MARS_ENVIRON_REQUEST_ID", uid)

    os.execlpe(mars_executable, mars_executable, env)


# --------------------------------------------------------------------------- #
# Timeout helper
# --------------------------------------------------------------------------- #

def timeout_handler(signum, frame):
    LOG.warning("Timeout triggered")
    raise TimeoutError()


# --------------------------------------------------------------------------- #
# HTTP handler
# --------------------------------------------------------------------------- #

STREAM_CHUNK_SIZE = 1024 * 1024  # 1 MiB


class Handler(http.server.BaseHTTPRequestHandler):
    logdir = "."
    shared_root = "/cache"
    shares = []
    cache_folder = "mars_data"
    timeout = 30
    mars_executable = "/usr/local/bin/mars"
    wbufsize = 1024 * 1024
    disable_nagle_algorithm = True

    # ------------------------------------------------------------------ POST
    def do_POST(self):
        signal.signal(signal.SIGALRM, timeout_handler)

        length = int(self.headers["content-length"])
        data = json.loads(self.rfile.read(length))

        request = data["request"]
        environ = data["environ"]

        LOG.info("POST %s %s", request, environ)

        uid = environ.get("request_id")
        if uid is None:
            uid = str(uuid.uuid4())

        setproctitle.setproctitle(f"cads_mars_server_stream {uid}")

        datadir = _resolve_datadir(
            uid, self.shared_root, self.shares, self.cache_folder,
        )

        target_path, pid = run_mars(
            mars_executable=self.mars_executable,
            request=request,
            uid=uid,
            logdir=self.logdir,
            environ=environ,
            datadir=datadir,
        )

        logfile = os.path.join(self.logdir, f"{uid}.log")

        try:
            # ---- wait for MARS to finish ----
            _, status = os.waitpid(pid, 0)

            exit_info = self._decode_exit(status)

            if exit_info["error"]:
                self._send_error_response(uid, exit_info)
                return

            # ---- MARS succeeded — stream the file back ----
            if not os.path.exists(target_path):
                LOG.error(f"{uid} MARS succeeded but output file missing: {target_path}")
                self._send_error_response(
                    uid,
                    {"code": 500, "message": "exited", "value": 1, "error": True},
                )
                return

            self._stream_file(uid, target_path)
        finally:
            self._cleanup_data(target_path)
            self._cleanup_data(logfile)

    # ------------------------------------------------------------------ GET
    def do_GET(self):
        """Return the MARS log file for the given UID."""
        uid = self.path.split("/")[-1]
        LOG.info("GET %s", uid)

        if not validate_uuid(uid):
            self.send_response(404)
            self.end_headers()
            return

        logfile = os.path.join(self.logdir, f"{uid}.log")
        if not os.path.exists(logfile):
            self.send_response(404)
            self.end_headers()
            return

        with open(logfile, "rb") as f:
            self.send_response(200)
            self.send_header("Content-type", "text/plain")
            self.send_header("Content-Disposition", f"attachment; filename={uid}.log")
            self.send_header("Content-Length", os.fstat(f.fileno()).st_size)
            self.end_headers()
            self.wfile.write(f.read())

    # -------------------------------------------------------------- DELETE
    def do_DELETE(self):
        """Delete the log file for the given UID."""
        uid = self.path.split("/")[-1]
        LOG.info("DELETE %s", uid)

        if not validate_uuid(uid):
            self.send_response(404)
            self.end_headers()
            return

        logfile = os.path.join(self.logdir, f"{uid}.log")
        if os.path.exists(logfile):
            os.unlink(logfile)

        self.send_response(204)
        self.end_headers()

    # ----------------------------------------------------------------- HEAD
    def do_HEAD(self):
        LOG.info("ping")
        self.send_response(204)
        self.end_headers()

    # ================================================================ helpers

    def _decode_exit(self, status):
        """Translate waitpid *status* into an info dict."""
        if status == 0:
            return {"error": False}

        if os.WIFSIGNALED(status):
            sig = os.WTERMSIG(status)
            retry_next = sig in (signal.SIGHUP, signal.SIGTERM, signal.SIGQUIT)
            return {
                "error": True,
                "code": 500,
                "message": "killed",
                "value": sig,
                "retry_same_host": False,
                "retry_next_host": retry_next,
            }

        exitcode = os.WEXITSTATUS(status)
        if exitcode >= 128:
            sig = exitcode - 128
            retry_next = sig in (signal.SIGHUP, signal.SIGTERM, signal.SIGQUIT)
            return {
                "error": True,
                "code": 500,
                "message": "killed",
                "value": sig,
                "retry_same_host": False,
                "retry_next_host": retry_next,
            }

        return {
            "error": True,
            "code": 400,
            "message": "exited",
            "value": exitcode,
            "retry_same_host": False,
            "retry_next_host": False,
        }

    def _send_error_response(self, uid, info):
        """Send an error back as header + JSON body (same as server.py)."""
        code = info.get("code", 500)
        kwargs = {info["message"]: info["value"]}

        signal.alarm(20)
        self.send_response(code)
        self.send_header("X-MARS-UID", uid)
        self.send_header("Content-type", "application/json")
        self.send_header("X-MARS-EXIT-CODE", str(info["value"]))
        if info.get("retry_same_host"):
            self.send_header("X-MARS-RETRY-SAME-HOST", 1)
        if info.get("retry_next_host"):
            self.send_header("X-MARS-RETRY-NEXT-HOST", 1)
        if info["message"] == "killed":
            self.send_header("X-MARS-SIGNAL", str(info["value"]))
        self.end_headers()
        self.wfile.write(json.dumps(kwargs).encode())
        signal.alarm(0)

        LOG.error(f"{uid} MARS error response sent: {kwargs}")

    def _stream_file(self, uid, path):
        """Read *path* and stream it to the client using chunked encoding.

        The wire format is identical to what ``server.py`` produces so
        ``RemoteMarsClient._transfer`` can consume it unchanged.
        """
        file_size = os.path.getsize(path)
        LOG.info(f"{uid} Streaming {bytes(file_size)} from {path}")

        signal.alarm(20)
        self.send_response(200)
        self.send_header("X-MARS-UID", uid)
        self.send_header("Content-type", "application/binary")
        self.send_header("Transfer-Encoding", "chunked")
        self.end_headers()
        signal.alarm(0)

        total = 0
        start = time.time()
        count = 0

        with open(path, "rb") as f:
            while True:
                chunk = f.read(STREAM_CHUNK_SIZE)
                if not chunk:
                    break

                signal.alarm(20)
                try:
                    self.wfile.write(f"{len(chunk):x}\r\n".encode())
                    self.wfile.write(chunk)
                    self.wfile.write(b"\r\n")
                except IOError:
                    LOG.error(f"{uid} Error streaming data to client")
                    raise
                signal.alarm(0)

                total += len(chunk)
                count += 1

        # Send ENDR marker so the client knows the transfer is complete
        self.wfile.write(b"4\r\nENDR\r\n")

        # Chunked-encoding terminator
        self.wfile.write(b"0\r\n\r\n")

        elapsed = time.time() - start
        rate = bytes(total / elapsed) if elapsed > 0 else "N/A"
        LOG.info(
            f"{uid} Streamed {bytes(total)} in {elapsed:.1f}s, {rate}/s, chunks: {count:,}"
        )

    @staticmethod
    def _cleanup_data(path):
        """Remove the temporary data file."""
        try:
            if path and os.path.exists(path):
                os.unlink(path)
        except OSError as e:
            LOG.warning(f"Failed to clean up {path}: {e}")

    # -------------------------------------------------------------- overrides
    def handle(self):
        """Close the accept socket so the parent can restart without 'Address already in use'."""
        ACCEPT_SOCKET.close()
        return super().handle()


# --------------------------------------------------------------------------- #
# Server wiring
# --------------------------------------------------------------------------- #

class ReuseAddressHTTPServer(http.server.HTTPServer):
    def server_bind(self):
        global ACCEPT_SOCKET
        ACCEPT_SOCKET = self.socket
        self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        super().server_bind()


class ForkingHTTPServer(socketserver.ForkingMixIn, ReuseAddressHTTPServer):
    pass


def setup_server(
    mars_executable,
    host,
    port,
    timeout=30,
    logdir=".",
    shared_root=None,
    shares=None,
    cache_folder=None,
):
    """Create and return a ready-to-serve HTTP server.

    Parameters
    ----------
    mars_executable : str
        Path to the ``mars`` binary.
    host, port : str, int
        Bind address.
    timeout : int
        Send-data timeout (seconds).
    logdir : str
        Directory for MARS log files.
    shared_root : str | None
        Root of the shared volumes.  Read from config if *None*.
    shares : list[str] | None
        List of volume names under *shared_root*.  Read from config if *None*.
    cache_folder : str | None
        Sub-folder inside each share for MARS data.  Read from config if *None*.
    """
    from .config import (
        CACHE_FOLDER as _cfg_cache_folder,
        SHARED_ROOT as _cfg_shared_root,
        SHARES as _cfg_shares,
    )

    if shared_root is None:
        shared_root = str(_cfg_shared_root)
    if shares is None:
        shares = _cfg_shares
    if cache_folder is None:
        cache_folder = _cfg_cache_folder

    os.makedirs(logdir, exist_ok=True)

    LOG.info(
        f"Stream server: shared_root={shared_root}, "
        f"shares={shares}, cache_folder={cache_folder}"
    )

    _ = {
        "mars_executable": mars_executable,
        "timeout": timeout,
        "logdir": logdir,
        "shared_root": shared_root,
        "shares": shares,
        "cache_folder": cache_folder,
    }

    class ThisHandler(Handler):
        timeout = _["timeout"]
        mars_executable = _["mars_executable"]
        logdir = _["logdir"]
        shared_root = _["shared_root"]
        shares = _["shares"]
        cache_folder = _["cache_folder"]

    server = ForkingHTTPServer((host, port), ThisHandler)
    return server
