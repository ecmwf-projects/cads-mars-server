import json
import asyncio
import websockets
from pathlib import Path

async def mars_via_ws(ws_url, requests, environ, target_dir="/download-cci1-0007"):
    """
    Executes a MARS request using the new websocket channel.
    Returns:
        (output_file_path, logs, returncode)
    """

    logs = []
    output_file = None
    returncode = None

    async with websockets.connect(ws_url) as ws:
        # Start job
        await ws.send(json.dumps({"cmd": "start", "requests": requests, "environ": environ, "target_dir": target_dir}))

        async for raw in ws:
            msg = json.loads(raw)
            t = msg.get("type")

            if t == "log":
                print(msg["line"], end="")
                logs.append(msg["line"])

            elif t == "state":
                status = msg.get("status")

                if status == "started":
                    rel = msg.get("result")
                    output_file = str(Path(rel))

                elif status == "finished":
                    returncode = msg["returncode"]
                    break

                elif status == "killed":
                    returncode = -9
                    break

    return output_file, logs, returncode

if __name__ == "__main__":
    import os
    ws_url = os.getenv("MARS_WS_URL", "ws://localhost:9001")

    # Example usage
    request = {
        'dataset': ['reanalysis'],
        'time': ['22:00:00'],
        'param': ['140212'],
        'class': ['ea'],
        'expect': ['any'],
        'number': ['all'],
        'levtype': ['sfc'],
        'date': ['2024-09-08']
    }
    environ = {
        'user_id': '37b6b138-0224-4875-b5c5-a3db813b6b01',
        'request_id': 'c3cc4ee4-88b4-4b0c-912a-cfb32ee6b6c5',
        'namespace': 'cci1:dev-pool',
        'host': 'worker-dev-cci1-1',
        'username': 'cci1:dev-pool:a3db813b6b01'
    }
    output_file, logs, returncode = asyncio.run(
        mars_via_ws(ws_url, [request], environ, target_dir="/download-cci1-0006/")
    )

    print("\n".join(logs))
    #print(f"Output file: {output_file}")
    print(f"Return code: {returncode}")