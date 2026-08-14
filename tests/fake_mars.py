#!/usr/bin/env python3
"""Fake MARS executable for testing.

Reads a MARS request from stdin or a file argument, extracts the TARGET,
writes fake data to the target (pipe fd or file path), and exits.

Environment variables
---------------------
MARS_EXIT_CODE      Exit code (default: 0)
MARS_TEST_DATA      Data to write as UTF-8 bytes (default: fake GRIB)
MARS_TEST_DATA_SIZE Size in bytes of random data to write (overrides MARS_TEST_DATA)
MARS_TEST_DELAY     Seconds to sleep before writing (default: 0)
"""

import os
import re
import sys
import time


def main():
    # Read request: from file argument or stdin
    if len(sys.argv) > 1 and os.path.isfile(sys.argv[1]):
        with open(sys.argv[1]) as f:
            request = f.read()
    else:
        request = sys.stdin.read()

    print(f"fake_mars: received request ({len(request)} bytes)", flush=True)
    print(
        "fake_mars: MARS_MAX_RETRIEVE_SIZE="
        f"{os.environ.get('MARS_MAX_RETRIEVE_SIZE', 'unset')}",
        flush=True,
    )

    # Optional delay
    delay = float(os.environ.get("MARS_TEST_DELAY", "0"))
    if delay > 0:
        print(f"fake_mars: sleeping {delay}s", flush=True)
        time.sleep(delay)

    # Extract TARGET from request
    m = re.search(r"TARGET='([^']+)'", request)
    if not m:
        print(
            "fake_mars: ERROR - no TARGET found in request",
            file=sys.stderr,
            flush=True,
        )
        sys.exit(1)

    target = m.group(1)
    print(f"fake_mars: TARGET={target}", flush=True)

    # Determine data to write
    data_size = os.environ.get("MARS_TEST_DATA_SIZE")
    if data_size:
        data = os.urandom(int(data_size))
    else:
        test_data = os.environ.get("MARS_TEST_DATA")
        if test_data:
            data = test_data.encode()
        else:
            # Default: fake GRIB-like data (104 bytes)
            data = b"GRIB" + b"\x00" * 96 + b"7777"

    # Write data to target
    if target.startswith("&"):
        # Pipe mode: write to file descriptor
        fd = int(target[1:])
        print(f"fake_mars: writing {len(data)} bytes to fd {fd}", flush=True)
        os.write(fd, data)
        os.close(fd)
    else:
        # File mode: write to file path
        print(f"fake_mars: writing {len(data)} bytes to {target}", flush=True)
        parent = os.path.dirname(target)
        if parent:
            os.makedirs(parent, exist_ok=True)
        with open(target, "wb") as f:
            f.write(data)

    print("fake_mars: done", flush=True)

    exit_code = int(os.environ.get("MARS_EXIT_CODE", "0"))
    sys.exit(exit_code)


if __name__ == "__main__":
    main()
