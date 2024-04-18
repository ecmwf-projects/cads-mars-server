import json
import logging
import os
import sys

import click

from . import client, server


# Create empty click group
@click.group()
def mars_cli() -> None:
    pass


logger = logging.getLogger(__name__)


@mars_cli.command("client")
@click.argument(
    "request_file",
    nargs=1,
    default="req",
)
@click.option(
    "--target",
    "-t",
    help=("target file to store the result."),
    default="data.grib",
)
@click.option(
    "--uid",
    "-u",
    help=("User id of request."),
    default="anonymous",
)
@click.option(
    "--server-list",
    "-s",
    help=("File which contains the list of URLs of the servers."),
    default="./server.list",
)
def this_client(request_file, target, uid, server_list) -> None:
    """
    A MARS client is spawned to execute a request. The request should be passed as a JSON file.
    """
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(process)d %(levelname)s %(module)s - %(funcName)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    if os.path.exists(server_list):
        with open(server_list) as f:
            urls = f.read().splitlines()
    else:
        urls = [
            "http://localhost:9000",
        ]
    cluster = client.RemoteMarsClientCluster(
        urls=urls,
        retries=3,
        delay=10,
        # timeout=None,
    )

    with open(request_file) as f:
        request = json.load(f)

    environ = dict(uid=uid)

    reply = cluster.execute(request, environ, target)
    logger.info(reply.message)


@mars_cli.command("server")
@click.option(
    "--mars-executable",
    "-m",
    help="Path to the mars executable",
    default="/usr/local/bin/mars",
)
@click.option(
    "--host",
    "-h",
    help="Host to listen on",
    default="",
)
@click.option(
    "--port",
    "-p",
    help="Port to listen on",
    default=9000,
)
@click.option(
    "--timeout",
    "-t",
    help="Timeout sendind data to client",
    type=int,
    default=30,
)
@click.option(
    "--logdir",
    "-l",
    help="Path to the log directory",
    default=".",
)
@click.option(
    "--pidfile",
    help="PID file",
    default=None,
)
@click.option(
    "--daemonize",
    help="Detach the server from the terminal",
    action="store_true",
)
def this_server(
    mars_executable, host, port, timeout, logdir, pidfile, daemonize
) -> None:
    """
    Set up a MARS server to execute requests.
    """
    logger.info(f"Starting Server {host}:{port} {logdir}")

    _server = server.setup_server(mars_executable, host, port, timeout, logdir)

    if daemonize:
        # TODO:use that with modern python
        # import daemon

        # with daemon.DaemonContext():
        #     _server.serve_forever()

        # For now, we will use the following
        pid = os.fork()
        if pid > 0:
            # exit first parent
            sys.exit(0)

        os.setsid()

        pid = os.fork()
        if pid > 0:
            # exit from second parent
            sys.exit(0)

    if pidfile:
        with open(pidfile, "w") as f:
            f.write(str(os.getpid()))

    _server.serve_forever()
