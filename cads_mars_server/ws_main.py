import asyncio
import logging
import signal
from cads_mars_server.ws_server import start_ws_server, kill_all_active_processes

log = logging.getLogger("ws-mars")

def main():
    # Flag to track if we're shutting down
    shutdown_event = asyncio.Event()
    
    def handle_shutdown(signum, frame):
        """Handle shutdown signals gracefully."""
        sig_name = signal.Signals(signum).name
        log.info(f"Received {sig_name}, initiating graceful shutdown...")
        log.info("Killing all active MARS processes...")
        kill_all_active_processes()
        shutdown_event.set()
    
    # Register signal handlers
    signal.signal(signal.SIGTERM, handle_shutdown)
    signal.signal(signal.SIGINT, handle_shutdown)
    
    async def runner():
        server = await start_ws_server()
        async with server:
            try:
                await shutdown_event.wait()
            except asyncio.CancelledError:
                log.info("Server task cancelled")
            finally:
                log.info("Shutting down server...")
                kill_all_active_processes()

    try:
        asyncio.run(runner())
    except KeyboardInterrupt:
        log.info("Keyboard interrupt received")
    finally:
        log.info("Server stopped")

if __name__ == "__main__":
    main()