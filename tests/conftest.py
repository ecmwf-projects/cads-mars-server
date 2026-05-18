"""Shared test fixtures for cads-mars-server."""

import os
import platform
import socket
import subprocess
import sys
import time

import pytest

IS_MACOS = platform.system() == "Darwin"

# Custom markers
requires_linux = pytest.mark.skipif(
    IS_MACOS,
    reason="Forking servers crash on macOS (setproctitle + CoreFoundation fork-safety)",
)

# Prevent loading production config — must be set before any cads_mars_server imports
os.environ.setdefault("MARS_CONFIG_FILE", "/nonexistent/mars/test_config.yaml")

FAKE_MARS_PATH = os.path.join(os.path.dirname(__file__), "fake_mars.py")
WS_SERVER_HELPER = os.path.join(os.path.dirname(__file__), "_ws_server_helper.py")


def _server_env():
    """Environment for server subprocesses."""
    env = os.environ.copy()
    env["OBJC_DISABLE_INITIALIZE_FORK_SAFETY"] = "YES"
    env["MARS_CONFIG_FILE"] = "/nonexistent/mars/test_config.yaml"
    return env


@pytest.fixture(scope="session")
def fake_mars_path():
    """Return path to the fake mars executable and ensure it is executable."""
    os.chmod(FAKE_MARS_PATH, 0o755)
    return FAKE_MARS_PATH


def find_free_port():
    """Find a free TCP port on localhost."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def wait_for_server(host, port, timeout=15):
    """Wait until a TCP server is accepting connections."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            with socket.create_connection((host, port), timeout=1):
                return True
        except (ConnectionRefusedError, OSError):
            time.sleep(0.2)
    raise TimeoutError(f"Server {host}:{port} did not start within {timeout}s")


def _terminate(proc, timeout=5):
    """Terminate a subprocess, escalating to kill if needed."""
    proc.terminate()
    try:
        proc.wait(timeout=timeout)
    except subprocess.TimeoutExpired:
        proc.kill()
        proc.wait()


# ---------------------------------------------------------------------------
# Pipe-mode HTTP server (server.py)
# ---------------------------------------------------------------------------


@pytest.fixture
def pipe_server(fake_mars_path, tmp_path):
    """Start a pipe-mode MARS server and yield connection info."""
    port = find_free_port()
    logdir = str(tmp_path / "logs")
    os.makedirs(logdir, exist_ok=True)

    proc = subprocess.Popen(
        [
            sys.executable,
            "-m",
            "cads_mars_server",
            "server",
            "-m",
            fake_mars_path,
            "-h",
            "127.0.0.1",
            "-p",
            str(port),
            "-l",
            logdir,
        ],
        env=_server_env(),
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    try:
        wait_for_server("127.0.0.1", port)
    except TimeoutError:
        proc.kill()
        out, err = proc.communicate(timeout=5)
        raise RuntimeError(
            f"Pipe server failed to start.\nstdout: {out.decode()}\nstderr: {err.decode()}"
        )

    yield {"url": f"http://127.0.0.1:{port}", "host": "127.0.0.1", "port": port, "logdir": logdir}

    _terminate(proc)


# ---------------------------------------------------------------------------
# Stream-mode HTTP server (server_cache_and_stream.py)
# ---------------------------------------------------------------------------


@pytest.fixture
def stream_server(fake_mars_path, tmp_path):
    """Start a stream-mode MARS server and yield connection info."""
    port = find_free_port()
    logdir = str(tmp_path / "logs")
    shared_root = str(tmp_path / "shared")
    share_name = "vol0"
    os.makedirs(logdir, exist_ok=True)
    os.makedirs(os.path.join(shared_root, share_name), exist_ok=True)

    proc = subprocess.Popen(
        [
            sys.executable,
            "-m",
            "cads_mars_server",
            "stream-server",
            "-m",
            fake_mars_path,
            "-h",
            "127.0.0.1",
            "-p",
            str(port),
            "-l",
            logdir,
            "--shared-root",
            shared_root,
            "--shares",
            share_name,
            "--cache-folder",
            "mars_data",
        ],
        env=_server_env(),
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    try:
        wait_for_server("127.0.0.1", port)
    except TimeoutError:
        proc.kill()
        out, err = proc.communicate(timeout=5)
        raise RuntimeError(
            f"Stream server failed to start.\n"
            f"stdout: {out.decode()}\nstderr: {err.decode()}"
        )

    yield {
        "url": f"http://127.0.0.1:{port}",
        "port": port,
        "logdir": logdir,
        "shared_root": shared_root,
    }

    _terminate(proc)


# ---------------------------------------------------------------------------
# WebSocket server (ws_server.py)
# ---------------------------------------------------------------------------


@pytest.fixture
def ws_server(fake_mars_path, tmp_path):
    """Start a WebSocket MARS server and yield connection info."""
    port = find_free_port()
    shared_root = tmp_path / "cache"
    shared_root.mkdir()

    # Create a "mars" symlink pointing to our fake_mars.py
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    (bin_dir / "mars").symlink_to(fake_mars_path)

    env = os.environ.copy()
    env["MARS_SHARED_ROOT"] = str(shared_root)
    env["MARS_CONFIG_FILE"] = "/nonexistent/mars/test_config.yaml"
    env["PATH"] = f"{bin_dir}:{env.get('PATH', '')}"
    # Shorter heartbeat for tests
    env["MARS_HEARTBEAT_INTERVAL"] = "5"

    proc = subprocess.Popen(
        [sys.executable, WS_SERVER_HELPER, str(port)],
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    try:
        wait_for_server("127.0.0.1", port)
    except TimeoutError:
        proc.kill()
        out, err = proc.communicate(timeout=5)
        raise RuntimeError(
            f"WS server failed to start.\n"
            f"stdout: {out.decode()}\nstderr: {err.decode()}"
        )

    yield {
        "url": f"ws://127.0.0.1:{port}",
        "port": port,
        "shared_root": str(shared_root),
    }

    _terminate(proc)
