import argparse
import http.server
import json
import logging
import os
import re
import signal
import socket
import socketserver
import struct
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


# From the MARS code

IDENT = r"[_0-9A-Za-z]+[_\.\-\+A-Za-z0-9:\t ]*[_\.\-\+A-Za-z0-9]*"
NUMB = r"[\-\.]*[0-9]+[\.0-9]*[Ee]*[\-\+]*[0-9]*"


def tidy(data):
    if isinstance(data, str):
        data = data.strip()

        if data.startswith("'"):
            assert data.endswith("'")
            return data

        if data.startswith('"'):
            assert data.endswith('"')
            return data

        if "/" in data and not data.startswith("/"):
            return tidy(data.split("/"))

    if isinstance(data, list):
        return "/".join([tidy(v) for v in data])

    data = str(data)
    if re.match(IDENT, data):
        return data

    if re.match(NUMB, data):
        return data

    if '"' in data:
        assert "'" not in data
        return "'{0}'".format(data)

    return '"{0}"'.format(data)


def mars(*, mars_executable, request, uid, logdir, environ):
    data_pipe_r, data_pipe_w = os.pipe()
    request_pipe_r, request_pipe_w = os.pipe()

    os.set_inheritable(data_pipe_r, True)
    os.set_inheritable(data_pipe_w, True)
    os.set_inheritable(request_pipe_r, True)
    os.set_inheritable(request_pipe_w, True)

    pid = os.fork()

    if pid:
        if isinstance(request, dict):
            requests = [request]
        else:
            requests = request

        assert isinstance(requests, list)

        def out(text):
            text = text.encode()
            assert os.write(request_pipe_w, text) == len(text)

        for request in requests:
            out("RETRIEVE,\n")
            for key, value in request.items():
                out("{0}={1},\n".format(key, tidy(value)))

            out("TARGET='&{0}'\n".format(data_pipe_w))

        os.close(data_pipe_w)
        os.close(request_pipe_r)
        os.close(request_pipe_w)

        return data_pipe_r, pid

    # Child process
    os.dup2(request_pipe_r, 0)
    os.close(request_pipe_w)
    os.close(data_pipe_r)

    out = os.open(
        os.path.join(logdir, f"{uid}.log"),
        os.O_WRONLY | os.O_CREAT | os.O_TRUNC,
        0o644,
    )
    os.dup2(out, 1)
    os.dup2(out, 2)

    env = dict(os.environ)

    for k, v in environ.items():
        if v is not None:
            env[f"MARS_ENVIRON_{k.upper()}"] = str(v)

    env.setdefault("MARS_ENVIRON_REQUEST_ID", uid)

    os.execlpe(mars_executable, mars_executable, env)


# https://stackoverflow.com/questions/48613006/python-sendall-not-raising-connection-closed-error


def timeout_handler(signum, frame):
    LOG.warning("Timeout triggered")
    raise TimeoutError()


class Handler(http.server.BaseHTTPRequestHandler):
    logdir = "."
    timeout = 30
    mars_executable = "/usr/local/bin/mars"
    wbufsize = 1024 * 1024
    disable_nagle_algorithm = True

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

        setproctitle.setproctitle(f"cads_mars_server {uid}")

        fd, pid = mars(
            mars_executable=self.mars_executable,
            request=request,
            uid=uid,
            logdir=self.logdir,
            environ=environ,
        )

        count = 0

        def send_header(
            code,
            exited=None,
            killed=None,
            retry_same_host=None,
            retry_next_host=None,
        ):
            LOG.info(
                f"Sending header code={code} exited={exited} killed={killed}"
                f" retry_same_host={retry_same_host} retry_next_host={retry_next_host}"
            )
            signal.alarm(self.timeout)
            self.send_response(code)
            self.send_header("X-MARS-UID", uid)
            if exited is None and killed is None:
                self.send_header("Content-type", "application/binary")
                self.send_header("Transfer-Encoding", "chunked")
            else:
                self.send_header("Content-type", "application/json")
                if exited is not None:
                    self.send_header("X-MARS-EXIT-CODE", str(exited))
                if killed is not None:
                    self.send_header("X-MARS-SIGNAL", str(killed))
                if retry_same_host is not None:
                    self.send_header("X-MARS-RETRY-SAME-HOST", int(retry_same_host))
                if retry_next_host is not None:
                    self.send_header("X-MARS-RETRY-NEXT-HOST", int(retry_next_host))

            self.end_headers()
            signal.alarm(0)

        total = 0
        start = time.time()
        data = None
        try:
            os.set_blocking(fd, True)

            while True:
                data = os.read(fd, self.wbufsize)

                if not data:
                    break

                if count == 0:
                    send_header(200)

                # socket timeout is not working
                signal.alarm(self.timeout)
                total += len(data)
                # LOG.info(f"Sending data {len(data)} total {total:_}")
                self.wfile.write(data)
                signal.alarm(0)

                count += 1

        except:
            LOG.exception("Error sending data")
            raise

        finally:
            signal.alarm(0)  # Just in case

            os.close(fd)
            _, code = os.waitpid(pid, 0)

            if count == 0 and code != 0:
                kwargs = {}

                if os.WIFSIGNALED(code):
                    status = 500
                    code = os.WTERMSIG(code)
                    message = "killed"
                else:
                    # Because MARS runs in a shell, the exit code may be the value $?
                    code = os.WEXITSTATUS(code)
                    if code >= 128:  # Process terminated by signal
                        status = 500
                        code = code - 128
                        message = "killed"
                    else:
                        status = 400
                        message = "exited"

                kwargs[message] = code
                if message == "killed":
                    # Don't retry if killed KILL so we can cancel the job
                    kwargs["retry_next_host"] = code in (
                        signal.SIGHUP,
                        signal.SIGTERM,
                        signal.SIGQUIT,
                    )
                    kwargs["retry_same_host"] = False

                send_header(status, **kwargs)
                self.wfile.write(json.dumps(kwargs).encode())

        elapsed = time.time() - start
        LOG.info(
            f"Transfered {bytes(total)} in {elapsed:.1f}s, {bytes(total/elapsed)}, chunks: {count:,}"
        )

    def do_GET(self):
        """
        Retrieve the log file for the given UID.
        """

        uid = self.path.split("/")[-1]

        LOG.info("GET %s", uid)

        if not validate_uuid(uid):
            self.send_response(404)
            self.end_headers()
            return

        log = os.path.join(self.logdir, f"{uid}.log")
        if not os.path.exists(log):
            self.send_response(404)
            self.end_headers()
            return

        with open(log, "rb") as f:
            self.send_response(200)
            self.send_header("Content-type", "text/plain")
            self.send_header("Content-Disposition", f"attachment; filename={uid}.log")
            self.send_header("Content-Length", os.fstat(f.fileno()).st_size)
            self.end_headers()
            self.wfile.write(f.read())

    def do_DELETE(self):
        """
        Delete the log file for the given UID.
        """

        uid = self.path.split("/")[-1]

        LOG.info("DELETE %s", uid)

        if not validate_uuid(uid):
            self.send_response(404)
            self.end_headers()
            return

        log = os.path.join(self.logdir, f"{uid}.log")
        if os.path.exists(log):
            os.unlink(log)
        self.send_response(204)
        self.end_headers()

    def do_HEAD(self):
        # Used as a 'ping'
        LOG.info("ping occuring")
        self.send_response(204)
        self.end_headers()

    def handle(self) -> None:
        """
        Close the accept socket so the main server can restart
        without a "Address already in use" error.
        """
        ACCEPT_SOCKET.close()
        return super().handle()


class ReuseAddressHTTPServer(http.server.HTTPServer):
    def server_bind(self):
        global ACCEPT_SOCKET
        ACCEPT_SOCKET = self.socket

        self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.socket.setsockopt(
            socket.SOL_SOCKET, socket.SO_LINGER, struct.pack("ii", 1, 0)
        )
        super().server_bind()


class ForkingHTTPServer(socketserver.ForkingMixIn, ReuseAddressHTTPServer):
    pass


def setup_server(mars_executable, host, port, timeout=30, logdir="."):
    _ = {
        "mars_executable": mars_executable,
        "timeout": timeout,
        "logdir": logdir,
    }

    class ThisHandler(Handler):
        timeout = _["timeout"]
        mars_executable = _["mars_executable"]
        logdir = _["logdir"]

    server = ForkingHTTPServer((host, port), ThisHandler)
    return server
