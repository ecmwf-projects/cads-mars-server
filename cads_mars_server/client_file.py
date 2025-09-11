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
from .config import get_config, local_target

LOG = logging.getLogger(__name__)
LOG.setLevel(logging.DEBUG)


HTTPConnectionPool.ConnectionCls = ConnectionWithKeepAlive


class RemoteMarsClientSession:
    config = get_config()
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
            header_rq = requests.head(self.url, timeout=self.timeout)
            remote_config = json.loads(header_rq.headers.get('CACHE_CONFIG', '{}'))
            self.log.debug(f"Remote config {remote_config}")
            if remote_config: # and all(_ in remote_config['SHARES'] for _ in self.config['SHARES']):
                try:
                    assert set(remote_config.get('SHARES', [])) >= set(self.config['SHARES']), f"Remote shares {remote_config.get('SHARES', [])} do not cover local shares {self.config['SHARES']}"
                except AssertionError as e:
                    self.log.debug(f"Remote shares {remote_config.get('SHARES', [])} do not cover local shares {self.config['SHARES']}")
                r = requests.post(
                    self.url,
                    json=dict(
                        request=self.request,
                        environ=self.environ,
                        type='file'
                    ),
                    stream=False,
                )
            else:
                self.log.error(f"The server {self.url} does not disclose its mars cache settings")
                return Result(error=f"The server {self.url} does not disclose its mars cache settings", retry_next_host=True)
        except requests.exceptions.Timeout as e:
            self.log.error(f"Timeout {e}")
            return Result(error=e, retry_next_host=True)
        except requests.exceptions.ConnectionError as e:
            self.log.error(f"Connection error {e}")
            return Result(error=e, retry_next_host=True)
        except AssertionError as e:
            self.log.error(f"Configuration error {e}")
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
                retry_same_host = bool(int(r.headers["X-MARS-RETRY-SAME-HOST"]))

            if "X-MARS-RETRY-NEXT-HOST" in r.headers:
                retry_next_host = bool(int(r.headers["X-MARS-RETRY-NEXT-HOST"]))
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
        res = None
        if code == http.HTTPStatus.OK:
            try:
                res = json.loads(r.headers['X-DATA'])
            except:
                self.log.debug(r.headers, exc_info=True)
            if not res:
                return Result(error=error, retry_same_host=True, retry_next_host=True, message='No result presented')
            try:
                if 'target' in res:
                    target = local_target(res)
                    self.log.debug(res)
                    while res['status'] in ('QUEUED', 'RUNNING', ):
                        time.sleep(.5)
                        return Result(error=error, retry_same_host=True, retry_next_host=False, message=str(res))
                    
                    if res['status'] == 'COMPLETED':
                        if target.startswith('http'):
                            # we have the file in a remote location and we return the URL
                            requests.head(target).raise_for_status()
                        else:
                            assert os.path.exists(target), f'File not found in the destination {target}'
                        # res = json.loads(requests.get(self.url + "/" + uid).headers['X-DATA'])
                        return Result(
                            error=None,
                            retry_same_host=True,
                            retry_next_host=True,
                            message=requests.get(self.url + "/" + uid).text,
                            data=res,
                        )
                    elif res['status'] == 'FAILED':
                        return Result(
                            error=res['message'],
                            message=requests.get(self.url + "/" + uid).text,
                            retry_same_host=False,
                            retry_next_host=True,
                            data=res
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
            logfile = self.get(uid)  # to get the final log message
            self.log.info(f"Log file for {uid} from {self.url}: {logfile}")
        except Exception as e:
            self.log.exception(f"Error getting log file for {uid} from {self.url}")
            logfile = str(e)

        try:
            r = requests.delete(self.url + "/" + uid)
            r.raise_for_status()
            self.uid = None
        except (requests.exceptions.HTTPError, requests.exceptions.ConnectionError):
            self.log.exception("Error deleting log file")

        return Result(error=error, message=logfile or str(error), data=res)
    def get(self, key):
        try:
            r = requests.get(self.url + "/" + key)
            r.raise_for_status()
            return Result(data=json.loads(r.text))
        except (requests.exceptions.HTTPError, requests.exceptions.ConnectionError) as e:
            self.log.exception("Error getting log file")
            return Result(error=str(e))
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

    def execute(self, requests, environ):
        if isinstance(requests, dict):
            return self._execute(requests, environ)

        req = {}
        messages = []

        for r in requests:
            try:
                req.update(r)
            except Exception as e:
                print(f"Error updating request with {r}: {e}")
                raise
            
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
        request_id = environ.get("request_id", "unknown")
        try:
            for url in self.urls:
                setproctitle.setproctitle(f"cads_mars_client {request_id} {url}")

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

   