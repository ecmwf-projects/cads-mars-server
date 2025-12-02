import asyncio
from cads_mars_server.ws_server import start_ws_server

def main():
    async def runner():
        server = await start_ws_server()
        async with server:
            await asyncio.Future()   # run forever

    asyncio.run(runner())

if __name__ == "__main__":
    main()