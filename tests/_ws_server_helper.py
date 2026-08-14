#!/usr/bin/env python3
"""Helper to start a WebSocket MARS server for integration tests."""

import asyncio
import os
import sys

# Prevent production config before any package imports (an explicitly set
# MARS_CONFIG_FILE must exist, so fall back to the tests' empty config)
os.environ.setdefault(
    "MARS_CONFIG_FILE",
    os.path.join(os.path.dirname(__file__), "empty_config.yaml"),
)

import websockets  # noqa: E402

from cads_mars_server.ws_server import handle_client  # noqa: E402


async def main():
    port = int(sys.argv[1])
    async with websockets.serve(handle_client, "127.0.0.1", port):
        print(f"WS test server listening on 127.0.0.1:{port}", flush=True)
        await asyncio.Future()  # run forever


if __name__ == "__main__":
    asyncio.run(main())
