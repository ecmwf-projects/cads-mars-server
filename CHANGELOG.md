# Changelog

All notable changes to cads-mars-server will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [3.0.0] - 2026-02-10

### Added

#### WebSocket Client/Server Architecture

**Why WebSocket instead of HTTP?**

The new WebSocket-based architecture provides significant advantages over traditional HTTP request/polling patterns:

- **Real-time bidirectional communication**: Logs stream from server to client as they occur, eliminating polling overhead
- **Long-running request support**: Single persistent connection handles jobs that may run for minutes or hours
- **Efficient resource usage**: No repeated polling requests consuming server resources and network bandwidth
- **Interactive control**: Client can send commands (e.g., kill) to running jobs without establishing new connections
- **Lower latency**: Immediate notification of job completion, errors, or status changes
- **Simplified connection management**: Automatic reconnection and failover built into the protocol

Traditional HTTP polling would require:
- Periodic status check requests (wasted bandwidth, server load)
- Delayed notifications (polling interval limits responsiveness)
- Complex state management on server for status queries
- Additional API endpoints for job control

WebSocket provides a natural fit for the workflow: submit job → stream logs → receive result.

---

#### Core Features

- **WebSocket-based MARS client** (`ws_client.py`, `ws_server.py`) for shared filesystem deployments
  - Asynchronous request handling with server-side job monitoring
  - Connection pooling with automatic failover across multiple servers
  - Real-time log streaming from MARS processes to clients
  - Bidirectional communication for job control (kill, heartbeat)
  - Configurable retry logic and connection timeouts
  
- **Modal client selection via `USE_SHARES` configuration**
  - `MARS_USE_SHARES=false` (default): Traditional pipe-based client (fully backward compatible)
  - `MARS_USE_SHARES=true`: WebSocket client for shared filesystem deployments
  - Configuration via environment variable or YAML file (`/etc/cads-mars-server.yaml`)

- **Client-side log filtering** ([LOG_FILTERING.md](docs/LOG_FILTERING.md))
  - Reduces noise from verbose MARS output
  - Pattern-based filtering (errors, warnings, progress indicators)
  - Message deduplication for repeated lines
  - Injectable custom log handlers:
    - Parse logs with custom logic
    - Raise exceptions to abort requests on specific error conditions
    - Send real-time commands to server (e.g., kill on timeout)
    - Integrate with external monitoring systems
  - Controlled via `CLIENT_FILTER_LOGS` config (default: enabled)

- **CephFS health diagnostics** ([CEPHFS_ARCHITECTURE.md](docs/CEPHFS_ARCHITECTURE.md))
  - `check-cephfs-health` console script for diagnosing filesystem issues
  - Documentation of CephFS architecture (MON/MDS/OSD components)
  - Startup health checks with warnings for detected issues

### Fixed

- **Process group management for WebSocket server**
  - Properly terminates entire process groups (parent + bash + MARS + children)
  - Prevents orphaned processes during restarts or crashes
  - Graceful shutdown with SIGTERM/SIGINT signal handlers
  - Startup cleanup of orphaned processes from previous runs

- **WebSocket connection handling**
  - Moved filesystem sync operations after connection close to prevent blocking
  - Improved connection resource management during slow storage operations

### Changed

- **Configuration system**: Centralized in `config.py` with environment variable and YAML file support
- **Process title tracking**: Uses `setproctitle` for easier process identification and management

### Documentation

- **[README.md](README.md)**: Complete guide to both pipe and WebSocket modes with configuration examples
- **[LOG_FILTERING.md](docs/LOG_FILTERING.md)**: Client-side log filtering with custom handler examples
- **[CEPHFS_ARCHITECTURE.md](docs/CEPHFS_ARCHITECTURE.md)**: CephFS architecture and diagnostic guide

### Migration Guide

#### For Existing Deployments

**No action required** - The default behavior (`USE_SHARES=false`) maintains full backward compatibility with the existing pipe-based client. All current deployments will continue to work without any changes.

#### To Adopt WebSocket Mode

WebSocket mode requires:
- Shared filesystem accessible by both clients and servers
- WebSocket server(s) running on worker nodes
- Network connectivity between clients and servers

**1. Enable WebSocket client:**

```bash
# Environment variable
export MARS_USE_SHARES=true

# Or in /etc/cads-mars-server.yaml
use_shares: true
```

**2. Configure WebSocket server list:**

```bash
# Environment variable (comma-separated)
export MARS_WS_SERVERS="ws://worker1:9001,ws://worker2:9001"

# Or in configuration file
mars_ws_servers:
  - ws://worker1:9001
  - ws://worker2:9001
```

**3. Start WebSocket server on worker nodes:**

```bash
ws-mars-server --host 0.0.0.0 --port 9001
```

See [README.md](README.md) for complete deployment examples.

### Breaking Changes

- **Dependency version requirement**: Applications using `USE_SHARES=true` must use `cads-mars-server>=3.0.0`
- **Shared filesystem required**: WebSocket mode assumes client and server have access to the same filesystem paths
- **Custom log handlers**: Must be `async` functions (default filtering works without changes)

---

## [0.2.5.1] - Previous Release

(Earlier changes not documented)
