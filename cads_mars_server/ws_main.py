import asyncio
from cads_mars_server.ws_server import start_ws_server

async def main():
    server = await start_ws_server()
    # server is a Serve object; entering the context keeps it running
    async with server:
        await asyncio.Future()  # run forever

if __name__ == "__main__":
    asyncio.run(main())