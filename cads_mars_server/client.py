import http
import logging
import random
import socket
import time

import requests
import setproctitle
import urllib3
from urllib3.connectionpool import HTTPConnectionPool

from .tools import bytes

LOG = logging.getLogger(__name__)


class ConnectionWithKeepAlive(HTTPConnectionPool.ConnectionCls):
    def _new_conn(self):
        conn = super()._new_conn()
        conn.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
        if hasattr(socket, "TCP_KEEPIDLE"):
            conn.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPIDLE, 60 * 10)
        conn.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPINTVL, 10)
        conn.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPCNT, 3)
        return conn


HTTPConnectionPool.ConnectionCls = ConnectionWithKeepAlive


class Result:
    def __init__(
        self,
        error=None,
        message=None,
        retry_same_host=False,
        retry_next_host=False,
    ):
        self.error = error
        self.message = message
        self.retry_same_host = retry_same_host
        self.retry_next_host = retry_next_host

    def __repr__(self):
        message = "None" if self.message is None else self.message[:10]
        return (
            f"{self.__class__.__name__}(error={self.error!r}, retry_same_host={self.retry_same_host},"
            f" retry_next_host={self.retry_next_host}, message={message}... )"
        )


class RemoteMarsClientSession:
    def __init__(self, url, request, environ, target, timeout=60):
        self.url = url
        self.request = request
        self.environ = environ
        self.target = target
        self.uid = None
        self.endr_recieved = False
        self.timeout = timeout

    def _transfer(self, r):
        start = time.time()
        total = 0
        with open(self.target, "wb") as f:
            self.endr_recieved = False
            count = 0
            for chunk in r.raw.read_chunked():
                count += 1
                total += len(chunk)
                if len(chunk) == 4:
                    if chunk == b"RWND":
                        f.seek(0)
                        f.truncate(0)
                        continue

                    if chunk == b"EROR":
                        raise ValueError("Error received")

                    if chunk == b"ENDR":
                        self.endr_recieved = True
                        continue

                    raise ValueError(f"Unknown message {chunk}")

                f.write(chunk)

            if not self.endr_recieved:
                raise ValueError("ENDR not received")

        elapsed = time.time() - start
        LOG.info(f"Transfered {bytes(total)} in {elapsed:.1f}s, {bytes(total/elapsed)}")

    def execute(self):
        LOG.info(f"Calling {self.url} {self.request} {self.environ}")

        error = None

        try:
            requests.head(self.url, timeout=self.timeout)
            r = requests.post(
                self.url,
                json=dict(
                    request=self.request,
                    environ=self.environ,
                ),
                stream=True,
            )
        except requests.exceptions.Timeout as e:
            LOG.error(f"Timeout {e}")
            return Result(error=e, retry_next_host=True)
        except requests.exceptions.ConnectionError as e:
            LOG.error(f"Connection error {e}")
            return Result(error=e, retry_next_host=True)

        try:
            r.raise_for_status()
        except requests.exceptions.HTTPError as e:
            LOG.error(f"HTTP error {e}")
            error = e

        uid = None
        code = r.status_code
        if code not in (http.HTTPStatus.BAD_REQUEST, http.HTTPStatus.OK):
            retry_same_host = code in (
                http.HTTPStatus.BAD_GATEWAY,
                http.HTTPStatus.GATEWAY_TIMEOUT,
                http.HTTPStatus.INTERNAL_SERVER_ERROR,
                http.HTTPStatus.REQUEST_TIMEOUT,
                http.HTTPStatus.SERVICE_UNAVAILABLE,
            )

            retry_next_host = code in (http.HTTPStatus.TOO_MANY_REQUESTS,)

            if "X-MARS-SIGNAL" in r.headers:
                signal = int(r.headers["X-MARS-SIGNAL"])
                LOG.error(f"MARS client kill by signal {signal}")

            if "X-MARS-RETRY-SAME-HOST" in r.headers:
                retry_same_host = int(r.headers["X-MARS-RETRY-SAME-HOST"])

            if "X-MARS-RETRY-NEXT-HOST" in r.headers:
                retry_next_host = int(r.headers["X-MARS-RETRY-NEXT-HOST"])

            return Result(
                error=error,
                message=r.text or str(error),
                retry_same_host=retry_same_host,
                retry_next_host=retry_next_host or retry_same_host,
            )

        uid = r.headers["X-MARS-UID"]

        if code == http.HTTPStatus.BAD_REQUEST:
            if "X-MARS-EXIT-CODE" in r.headers:
                exitcode = int(r.headers["X-MARS-EXIT-CODE"])
                LOG.error(f"MARS client exited with code {exitcode}")

        if code == http.HTTPStatus.OK:
            try:
                self._transfer(r)
            except urllib3.exceptions.ProtocolError as e:
                LOG.exception("Error transferring file (1)")
                return Result(error=e, retry_same_host=True, retry_next_host=True)
            except Exception as e:
                LOG.exception("Error transferring file (2)")
                error = e

        logfile = None

        try:
            r = requests.get(self.url + "/" + uid)
            r.raise_for_status()
            logfile = r.text
        except (requests.exceptions.HTTPError, requests.exceptions.ConnectionError):
            LOG.exception("Error getting log file")

        try:
            r = requests.delete(self.url + "/" + uid)
            r.raise_for_status()
            self.uid = None
        except (requests.exceptions.HTTPError, requests.exceptions.ConnectionError):
            LOG.exception("Error deleting log file")

        return Result(error=error, message=logfile or str(error))

    def __del__(self):
        try:
            if self.uid is not None:
                requests.delete(self.url + "/" + self.uid)
        except Exception:
            pass


class RemoteMarsClient:
    def __init__(self, url, retries=3, delay=10, timeout=60):
        self.url = url
        self.retries = retries
        self.delay = delay
        self.timeout = timeout

    def execute(self, request, environ, target):
        session = RemoteMarsClientSession(
            self.url, request, environ, target, self.timeout
        )

        for i in range(self.retries):
            reply = session.execute()
            if not reply.error:
                return reply

            if not reply.retry_same_host:
                return reply

            LOG.error(f"Error {reply}")
            LOG.error(f"Retry on the same host {self.url}")

            time.sleep(self.delay)

        return reply


class RemoteMarsClientCluster:
    def __init__(self, urls, retries=3, delay=10, timeout=60, log=LOG):
        self.urls = urls
        self.retries = retries
        self.delay = delay
        self.timeout = timeout
        self.log = log

    def execute(self, request, environ, target):
        random.shuffle(self.urls)
        saved = setproctitle.getproctitle()
        request_id = request.get("request_id", "unknown")
        try:

            for url in self.urls:

                setproctitle.setproctitle(f"cads_mars_client {request_id} {url}")

                client = RemoteMarsClient(
                    url,
                    self.retries,
                    self.delay,
                    self.timeout,
                    log=self.log,
                )

                reply = client.execute(request, environ, target)
                if not reply.error:
                    return reply

                if not reply.retry_next_host:
                    return reply

                LOG.error(f"Error {reply}")
                LOG.error(f"Retry on the next host {url}")
        finally:
            setproctitle.setproctitle(saved)

        return reply
