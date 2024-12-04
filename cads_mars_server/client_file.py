import http
import json
import logging
import os
import random
import socket
import time

import requests
import setproctitle
import urllib3
from urllib3.connectionpool import HTTPConnectionPool

from .tools import bytes
from .client_pipe import ConnectionWithKeepAlive, Result, ClientError

LOG = logging.getLogger(__name__)

CACHE_ROOT = '/cache'


HTTPConnectionPool.ConnectionCls = ConnectionWithKeepAlive


class RemoteMarsClientSession:
    def __init__(
        self,
        *,
        url,
        request,
        environ,
        target,
        open_mode="wb",
        position=0,
        timeout=60,
        log=LOG,
    ):
        self.url = url
        self.request = request
        self.environ = environ
        self.target = target
        self.uid = None
        self.endr_recieved = False
        self.timeout = timeout
        self.log = log
        self.open_mode = open_mode
        self.position = position

    

    def execute(self):
        self.log.info(f"Calling {self.url} {self.request} {self.environ}")

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
            self.log.error(f"Timeout {e}")
            return Result(error=e, retry_next_host=True)
        except requests.exceptions.ConnectionError as e:
            self.log.error(f"Connection error {e}")
            return Result(error=e, retry_next_host=True)
        

        try:
            r.raise_for_status()
        except requests.exceptions.HTTPError as e:
            self.log.error(f"HTTP error {e}")
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
                self.log.error(f"MARS client kill by signal {signal}")

            if "X-MARS-RETRY-SAME-HOST" in r.headers:
                retry_same_host = int(r.headers["X-MARS-RETRY-SAME-HOST"])

            if "X-MARS-RETRY-NEXT-HOST" in r.headers:
                retry_next_host = int(r.headers["X-MARS-RETRY-NEXT-HOST"])
            data = None
            if "X-DATA" in r.headers:
                data = json.loads(r.headers["X-DATA"])

            return Result(
                error=error,
                message=r.text or str(error),
                retry_same_host=retry_same_host,
                retry_next_host=retry_next_host or retry_same_host,
                data=data
            )

        uid = r.headers["X-MARS-UID"]

        if code == http.HTTPStatus.BAD_REQUEST:
            if "X-MARS-EXIT-CODE" in r.headers:
                exitcode = int(r.headers["X-MARS-EXIT-CODE"])
                self.log.error(f"MARS client exited with code {exitcode}")

        if code == http.HTTPStatus.OK:
            try:
                self._transfer(r)
            except ClientError as e:
                self.log.exception("Error transferring file (ClientError)")
                return Result(
                    error=e,
                    retry_same_host=e.retry_same_host,
                    retry_next_host=e.retry_next_host,
                )
            except urllib3.exceptions.ProtocolError as e:
                self.log.exception("Error transferring file (ProtocolError)")
                return Result(error=e, retry_same_host=True, retry_next_host=True)
            except Exception as e:
                self.log.exception("Error transferring file (Other errors)")
                error = e

        logfile = None

        try:
            r = requests.get(self.url + "/" + uid)
            r.raise_for_status()
            logfile = r.text
        except (requests.exceptions.HTTPError, requests.exceptions.ConnectionError):
            self.log.exception("Error getting log file")

        try:
            r = requests.delete(self.url + "/" + uid)
            r.raise_for_status()
            self.uid = None
        except (requests.exceptions.HTTPError, requests.exceptions.ConnectionError):
            self.log.exception("Error deleting log file")

        return Result(error=error, message=logfile or str(error))

    def __del__(self):
        try:
            if self.uid is not None:
                requests.delete(self.url + "/" + self.uid)
        except Exception:
            pass

class RemoteMarsClientSession:
    def __init__(
        self,
        *,
        url,
        request,
        environ,
        timeout=60,
        log=LOG,
    ):
        self.url = url
        self.request = request
        self.environ = environ
        self.uid = None
        self.endr_recieved = False
        self.timeout = timeout
        self.log = log

    def execute(self):
        self.log.info(f"Calling {self.url} {self.request} {self.environ}")

        error = None

        try:
            requests.head(self.url, timeout=self.timeout)
            r = requests.post(
                self.url,
                json=dict(
                    request=self.request,
                    environ=self.environ,
                    type='file'
                ),
                stream=False,
            )
        except requests.exceptions.Timeout as e:
            self.log.error(f"Timeout {e}")
            return Result(error=e, retry_next_host=True)
        except requests.exceptions.ConnectionError as e:
            self.log.error(f"Connection error {e}")
            return Result(error=e, retry_next_host=True)

        try:
            r.raise_for_status()
        except requests.exceptions.HTTPError as e:
            self.log.error(f"HTTP error {e}")
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
                self.log.error(f"MARS client kill by signal {signal}")

            if "X-MARS-RETRY-SAME-HOST" in r.headers:
                retry_same_host = int(r.headers["X-MARS-RETRY-SAME-HOST"])

            if "X-MARS-RETRY-NEXT-HOST" in r.headers:
                retry_next_host = int(r.headers["X-MARS-RETRY-NEXT-HOST"])
            if "X-DATA" in r.headers:
                data = json.loads(r.headers["X-DATA"])

            return Result(
                error=error,
                message=r.text or str(error),
                retry_same_host=retry_same_host,
                retry_next_host=retry_next_host or retry_same_host,
                data=data
            )

        uid = r.headers["X-MARS-UID"]

        if code == http.HTTPStatus.BAD_REQUEST:
            if "X-MARS-EXIT-CODE" in r.headers:
                exitcode = int(r.headers["X-MARS-EXIT-CODE"])
                self.log.error(f"MARS client exited with code {exitcode}")
        # print(r.headers)
        # assert r.headers['X-DATA'], f'No result presented'
        # res = r.headers['X-DATA']
        # print('xxxx', res)
        # res = json.loads(res)
        res = None
        if code == http.HTTPStatus.OK:
            try:
                res = json.loads(r.headers['X-DATA'])
            except:
                print(r.headers)
            if not res:
                return Result(error=error, retry_same_host=True, retry_next_host=True, message='No result presented')
            try:
                if 'target' in res:
                    res['target'] = res['target'].replace(CACHE_ROOT, '')
                    if os.path.exists(res['target']):
                        details = os.stat(res['target'])
                    else:
                        details = os.stat(res['target'].replace(CACHE_ROOT, ''))
                    while details.st_size < res['size'] and res['status'] in ('QUEUED', 'RUNNING', ):
                        time.sleep(.5)
                else:
                    return Result(
                        error=e,
                        retry_same_host=True,
                        retry_next_host=True
                    )
            except ClientError as e:
                self.log.exception("Error transferring file (ClientError)")
                return Result(
                    error=e,
                    retry_same_host=e.retry_same_host,
                    retry_next_host=e.retry_next_host,
                    data=res
                )
            except urllib3.exceptions.ProtocolError as e:
                self.log.exception("Error transferring file (ProtocolError)")
                return Result(error=e, retry_same_host=True, retry_next_host=True, data=res)
            except Exception as e:
                self.log.exception("Error transferring file (Other errors)")
                error = e

        logfile = None

        try:
            r = requests.get(self.url + "/" + uid)
            r.raise_for_status()
            logfile = r.text
        except (requests.exceptions.HTTPError, requests.exceptions.ConnectionError):
            self.log.exception("Error getting log file")

        try:
            r = requests.delete(self.url + "/" + uid)
            r.raise_for_status()
            self.uid = None
        except (requests.exceptions.HTTPError, requests.exceptions.ConnectionError):
            self.log.exception("Error deleting log file")

        return Result(error=error, message=logfile or str(error), data=res)

    def __del__(self):
        try:
            if self.uid is not None:
                requests.delete(self.url + "/" + self.uid)
        except Exception:
            pass

class RemoteMarsClient:
    def __init__(
        self,
        *,
        url,
        retries=3,
        delay=10,
        timeout=60,
        log=LOG,
    ):
        self.url = url
        self.retries = retries
        self.delay = delay
        self.timeout = timeout
        self.log = log

    def execute(self, request, environ):
        session = RemoteMarsClientSession(
            url=self.url,
            request=request,
            environ=environ,
            timeout=self.timeout,
            log=self.log,
        )
        self.log.info(f'Session for {self.url} prepared')
        for i in range(self.retries):
            reply = session.execute()
            if not reply.error:
                return reply

            if not reply.retry_same_host:
                return reply

            self.log.error(f"Error {reply}")
            self.log.error(f"Retry on the same host {self.url}")

            time.sleep(self.delay)

        return reply


class RemoteMarsClientCluster:
    def __init__(self, urls, retries=3, delay=10, timeout=60, log=LOG):
        self.urls = urls
        self.retries = retries
        self.delay = delay
        self.timeout = timeout
        self.log = log

    def execute(self, request, environ):
        if isinstance(request, dict):
            return self._execute(request, environ)

        req = {}
        messages = []

        for r in request:
            req.update(r)

            result = self._execute(
                req, environ
            )
            messages.append(f"{result.message}")

            if result.error:
                result.message = "\n".join(messages)
                return result

        result.message = "\n".join(messages)
        return result

    def _execute(self, request, environ):
        random.shuffle(self.urls)
        saved = setproctitle.getproctitle()
        # request_id = environ.get("request_id", "unknown")
        try:
            for url in self.urls:
                # setproctitle.setproctitle(f"cads_mars_client {request_id} {url}")

                client = RemoteMarsClient(
                    url=url,
                    retries=self.retries,
                    delay=self.delay,
                    timeout=self.timeout,
                    log=self.log,
                )

                reply = client.execute(request, environ)
                if not reply.error:
                    return reply

                if not reply.retry_next_host:
                    return reply

                self.log.error(f"Error {reply}")
                self.log.error(f"Retry on the next host {url}")
        finally:
            setproctitle.setproctitle(saved)

        return reply

class RemoteMarsClientCluster:
    def __init__(self, urls, retries=3, delay=10, timeout=60, log=LOG):
        self.urls = urls
        self.retries = retries
        self.delay = delay
        self.timeout = timeout
        self.log = log

    def execute(self, request, environ):
        if isinstance(request, dict):
            return self._execute(request, environ)

        req = {}
        messages = []

        for r in request:
            req.update(r)

            result = self._execute(
                req, environ
            )
            messages.append(f"{result.message}")

            if result.error:
                result.message = "\n".join(messages)
                return result

        result.message = "\n".join(messages)
        return result

    def _execute(self, request, environ):
        random.shuffle(self.urls)
        saved = setproctitle.getproctitle()
        # request_id = environ.get("request_id", "unknown")
        try:
            for url in self.urls:
                # setproctitle.setproctitle(f"cads_mars_client {request_id} {url}")

                client = RemoteMarsClient(
                    url=url,
                    retries=self.retries,
                    delay=self.delay,
                    timeout=self.timeout,
                    log=self.log,
                )

                reply = client.execute(request, environ)
                if not reply.error:
                    return reply

                if not reply.retry_next_host:
                    return reply

                self.log.error(f"Error {reply}")
                self.log.error(f"Retry on the next host {url}")
        finally:
            setproctitle.setproctitle(saved)

        return reply
    
class MarsAdaptor:
    def __init__(
            self,
            request,
            env
    ) -> None:
        self.request = request
        self.env = env
