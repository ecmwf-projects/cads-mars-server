import asyncio
from cads_mars_server.ws_server import start_ws_server

if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    server = loop.run_until_complete(start_ws_server())
    loop.run_forever()