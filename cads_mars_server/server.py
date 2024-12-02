import http.server
import json
import logging
import os
import re
import select
import signal
import socket
import socketserver
import time
import uuid
import hashlib
import random
from .config import get_config
from pymemcache.client.hash import HashClient
import setproctitle
from .cache import WorkerCache, request_hash

from .tools import bytes

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(process)d %(levelname)s %(module)s - %(funcName)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

LOG = logging.getLogger(__name__)
ACCEPT_SOCKET = None

config = get_config()

CACHE_ROOT = config['CACHE_ROOT']

SHARES = config['SHARES']

MARS_CACHE_FOLDER = config['MARS_CACHE_FOLDER']
MEMCACHED = config['MEMCACHED']


def validate_uuid(uid):
    return re.match(r"^[a-f0-9-]{36}$", uid)

# From the MARS code

IDENT = r"[_0-9A-Za-z]+[_\.\-\+A-Za-z0-9:\t ]*[_\.\-\+A-Za-z0-9]*"
NUMB = r"[\-\.]*[0-9]+[\.0-9]*[Ee]*[\-\+]*[0-9]*"


def extract_transfer_bytes(file_path):
    # Define the regular expression pattern
    pattern = re.compile(r"Transfering (\d+) bytes")

    # Read the file
    with open(file_path, 'r') as file:
        for line in file:
            # Search for the pattern in each line
            match = pattern.search(line)
            if match:
                # Extract and return the number of bytes
                return int(match.group(1))

    # Return None if no match is found
    return None


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


def mars_target(*, mars_executable, request, uid, logdir, environ):
    request_pipe_r, request_pipe_w = os.pipe()
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
                if key != 'target':
                    out("{0}={1},\n".format(key, tidy(value)))
            out("{0}={1}\n".format('target', tidy(request['target'])))
        
        os.close(request_pipe_r)
        os.close(request_pipe_w)

        return request_pipe_w, pid

    os.dup2(request_pipe_r, 0)
    os.close(request_pipe_w)
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
    LOG.info(f'{request_pipe_w} is a request?')

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
    client_type = dict(
        pipe=mars,
        file=mars_target,
    )

    def do_POST(self):
        signal.signal(signal.SIGALRM, timeout_handler)

        length = int(self.headers["content-length"])
        data = json.loads(self.rfile.read(length))

        request = data["request"]
        environ = data["environ"]
        type = data.get("type", "file")

        LOG.info("POST %s %s", request, environ)

        uid = environ.get("request_id")
        if uid is None:
            uid = str(uuid.uuid4())

        setproctitle.setproctitle(f"cads_mars_server {uid}")

        getattr(self, f'_{type}')(request, environ, uid)


    def _pipe(self, request, environ, uid):

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
            signal.alarm(20)
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
                ready, _, _ = select.select([fd, self.rfile], [], [])

                data = os.read(fd, self.wbufsize)

                if self.rfile in ready:
                    LOG.error("Client closed connection")
                    try:
                        LOG.error("Killing mars process %s", pid)
                        os.kill(pid, signal.SIGKILL)
                    except Exception as e:
                        LOG.error("Error killing mars process %s", e)
                        pass
                    raise IOError("Client closed connection")

                if not data:
                    break

                if count == 0:
                    send_header(200)

                # socket timeout is not working
                signal.alarm(20)
                total += len(data)
                # LOG.info(f"Sending data {len(data)} total {total:_}")
                try:
                    self.wfile.write(data)
                except IOError:
                    try:
                        LOG.error("Error sending data")
                        LOG.error("Killing mars process %s", pid)
                        os.kill(pid, signal.SIGKILL)
                    except Exception as e:
                        LOG.error("Error killing mars process %s", e)
                        pass
                    raise
                signal.alarm(0)

                count += 1

        except:
            LOG.exception("Error sending data")
            raise

        finally:
            signal.alarm(0)  # Just in case

            os.close(fd)
            _, code = os.waitpid(pid, 0)

            if code != 0:
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

                LOG.error("MARS exited in error %s", kwargs)
                if count == 0:
                    LOG.error("Sending error message in header")
                    send_header(status, **kwargs)
                    self.wfile.write(json.dumps(kwargs).encode())
                else:
                    LOG.error("Sending error message in stream")
                    self.wfile.write("4\r\nEROR\r\n".encode())
                    message = json.dumps(kwargs)
                    self.wfile.write(f"{len(message):x}\r\n{message}\r\n".encode())
                    self.wfile.write("0\r\n\r\n".encode())

        elapsed = time.time() - start
        LOG.info(
            f"Transfered {bytes(total)} in {elapsed:.1f}s, {bytes(total/elapsed)}, chunks: {count:,}"
        )

    def _file(self, request, environ, uid):
        cache = WorkerCache(HashClient(MEMCACHED))
        rq_hash = request_hash(request)
        log_file = os.path.join(self.logdir, f"{uid}.log")

        def send_header(
                code,
                result,
                exited=None,
                killed=None,
                retry_same_host=None,
                retry_next_host=None,
            ):
                LOG.info(
                    f"Sending header code={code} exited={exited} killed={killed}"
                    f" retry_same_host={retry_same_host} retry_next_host={retry_next_host}"
                )
                signal.alarm(20)
                self.send_response(code)
                self.send_header("X-MARS-UID", uid)
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
                self.wfile.write(
                    json.dumps(result).encode('utf-8')
                )
                if result['status'] == 'FAILED':
                    cache.delete(rq_hash)
                signal.alarm(0)

        _cache = cache.get(rq_hash)
        run = False
        if _cache:
            if _cache['status'] in ['RUNNING', 'QUEUED']:
                LOG.info(f'Request for {rq_hash} is already running on {_cache["host"]}')
                with open(log_file, 'w') as _f:
                    _f.write('Waiting and serving file from cads_mars_server cache')
                    send_header(200, _cache)
                    return
            elif _cache['status'] == 'COMPLETED':
                out_file = _cache['target']
                LOG.info(f'Cached request {rq_hash} for request {uid}')
                if os.path.exists(out_file):
                    with open(log_file, 'w') as _f:
                        _f.write('File returned from cads_mars_server cache')
                    send_header(200, _cache)
                    return
            elif _cache['status'] == 'FAILED':
                out_file = _cache['target']
                _cache = dict(
                    status='QUEUED',
                    host=os.uname().nodename,
                    mars=MARS_CACHE_FOLDER,
                    share=out_file.split('/')[1],
                    target=out_file,
                    access=0
                )
                run = True
        else:
            out_file = os.path.join(CACHE_ROOT, random.sample(SHARES, 1)[0], MARS_CACHE_FOLDER,f'{rq_hash}.grib')
            _cache = dict(
                status='QUEUED',
                host=os.uname().nodename,
                mars=MARS_CACHE_FOLDER,
                share=out_file.split('/')[1],
                target=out_file,
                access=0
            )
            run = True
            cache.set(rq_hash, _cache)
        start = time.time()

        request.update({'target': _cache['target']})
        if run:
            fd, pid = mars_target(
                mars_executable=self.mars_executable,
                request=request,
                uid=uid,
                logdir=self.logdir,
                environ=environ,
            )
            cache.set(rq_hash, _cache)

        wayting = True

        t0 = time.time()
        while wayting and run:
            expected_size = extract_transfer_bytes(log_file)
            if expected_size:
                wayting = False
                _cache.update({'size': expected_size, 'status': 'RUNNING', 'access': _cache.get('access', 0) + 1})
                cache.set(rq_hash, _cache)
                break
            if time.time() - t0 > 40:
                wayting = False
            time.sleep(.004)
        total = 0
        t0 = time.time()
        try:
            if total < expected_size:
                while True:
                    if os.path.exists(request['target']):
                        total = os.stat(request['target']).st_size
                        LOG.info(f'{total / expected_size * 100:0.2f}% of {expected_size}')
                        if total >= expected_size:
                            break
                    time.sleep(.1)

            if total == expected_size:
                _cache.update({'status': 'COMPLETED'})
                cache.set(rq_hash, _cache)
                elapsed = time.time() - start
                LOG.info(
                    f"Transfered {bytes(total)} in {elapsed:.1f}s, {bytes(total/elapsed)}"
                )
                send_header(200, _cache)
        except:
            _cache['status'] = 'FAILED'
            cache.set(rq_hash, _cache)
            send_header(500, _cache)


    def do_GET(self):
        """Retrieve the log file for the given UID."""
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
        """Delete the log file for the given UID."""
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

    def handle(self):
        """Close the accept socket so the main server can restart without a "Address already in use" error."""
        ACCEPT_SOCKET.close()
        return super().handle()


class ReuseAddressHTTPServer(http.server.HTTPServer):
    def server_bind(self):
        global ACCEPT_SOCKET
        ACCEPT_SOCKET = self.socket

        self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

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
