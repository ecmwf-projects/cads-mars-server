# cads-mars-server

A proxy MARS server and client designed for CADS with two operational modes:

1. **Pipe Mode** (default): Traditional synchronous client using MARS stdin/stdout pipes
2. **Shares Mode**: Asynchronous WebSocket-based client with shared filesystem access

## Features

### Pipe Mode (Default)
- Direct MARS process execution via stdin/stdout
- Synchronous request handling
- No additional server infrastructure required
- Backward compatible with all existing deployments

### Shares Mode (WebSocket)
- Asynchronous job processing with server-side execution
- Real-time log streaming from MARS processes
- Connection pooling with automatic failover
- Client-side log filtering to reduce verbose output
- Graceful process management (no orphaned processes)
- CephFS health diagnostics and monitoring

## Installation

```bash
pip install cads-mars-server
```

For development:
```bash
git clone <repository>
cd cads-mars-server
pip install -e .
```

## Quick Start

### Using Pipe Mode (Default)

```python
from cads_adaptors.adaptors.mars import execute_mars
from cads_adaptors import Context

# Executes using traditional pipe client
result = execute_mars(
    request={"class": "ea", "date": "20240101", ...},
    context=Context(),
    target_fname="output.grib",
    target_dir="/path/to/output"
)
```

### Using Shares Mode (WebSocket)

**1. Enable WebSocket client:**

```bash
# Via environment variable
export MARS_USE_SHARES=true

# Or in /etc/cads-mars-server.yaml
use_shares: true
```

**2. Start WebSocket server(s):**

```bash
# Start server on worker node
ws-mars-server --host 0.0.0.0 --port 9001

# Or via systemd (recommended)
systemctl enable --now ws-mars-server.service
```

**3. Configure server list:**

```bash
# Environment variable (comma-separated)
export MARS_WS_SERVERS="ws://worker1:9001,ws://worker2:9001,ws://worker3:9001"

# Or in config file
mars_ws_servers:
  - ws://worker1:9001
  - ws://worker2:9001
  - ws://worker3:9001
```

**4. Execute requests:**

```python
from cads_adaptors.adaptors.mars import execute_mars

# Now uses WebSocket client with automatic failover
result = execute_mars(
    request={"class": "ea", "date": "20240101", ...},
    context=context,
    target_fname="output.grib",
    target_dir="/shared/filesystem/path"  # Must be accessible by servers
)
```

## Configuration

### Modal Behavior: USE_SHARES

The `USE_SHARES` configuration controls which client is used:

| Setting | Client Type | Use Case |
|---------|------------|----------|
| `false` (default) | Pipe | Existing deployments, no infrastructure changes needed |
| `true` | WebSocket | New deployments, better performance, shared filesystem |

### Configuration Methods

**Environment Variables:**
```bash
export MARS_USE_SHARES=true
export MARS_WS_SERVERS="ws://server1:9001,ws://server2:9001"
export MARS_CLIENT_FILTER_LOGS=true  # Enable log filtering
export MARS_MAX_CONCURRENT_CONNECTIONS=10  # Server connection limit
```

**Config File (`/etc/cads-mars-server.yaml`):**
```yaml
use_shares: true
mars_ws_servers:
  - ws://server1:9001
  - ws://server2:9001
client_filter_logs: true
max_concurrent_connections: 10
```

### All Configuration Options

| Option | Env Var | Config Key | Default | Description |
|--------|---------|------------|---------|-------------|
| Client Mode | `MARS_USE_SHARES` | `use_shares` | `false` | Use WebSocket client instead of pipe |
| WS Servers | `MARS_WS_SERVERS` | `mars_ws_servers` | (required) | Comma-separated or list of WebSocket URLs |
| Log Filtering | `MARS_CLIENT_FILTER_LOGS` | `client_filter_logs` | `true` | Filter verbose MARS output |
| Max Connections | `MARS_MAX_CONCURRENT_CONNECTIONS` | `max_concurrent_connections` | `0` (unlimited) | Server connection limit |
| Request Timeout | `MARS_REQUEST_TIMEOUT` | `request_timeout` | `30` | Timeout per server attempt (seconds) |
| Heartbeat | `MARS_HEARTBEAT_INTERVAL` | `heartbeat_interval` | `30` | Job heartbeat interval (seconds) |
| Shared Root | `MARS_SHARED_ROOT` | `shared_root` | `/cache` | Shared filesystem root path |
| Debug Mode | `MARS_WS_DEBUG` | `debug_mode` | `false` | Enable debug logging |

## Client-Side Log Filtering

WebSocket mode includes intelligent log filtering to reduce noise:

```python
from cads_adaptors.adaptors.mars import execute_mars

# Use default filtering (shows errors, warnings, progress)
result = execute_mars(request, context=context)

# Inject custom log handler
async def my_handler(line: str, ws, logger) -> Optional[str]:
    if "FATAL" in line:
        await ws.send(json.dumps({"cmd": "kill"}))
        raise RuntimeError(f"Aborted: {line}")
    return line if "ERROR" in line else None

result = execute_mars(request, context=context, log_handler=my_handler)
```

See [docs/LOG_FILTERING.md](docs/LOG_FILTERING.md) for custom handler examples.

## CephFS Diagnostics

For deployments using CephFS, the WebSocket server includes health monitoring:

```bash
# Check CephFS health
check-cephfs-health
```

See [docs/CEPHFS_ARCHITECTURE.md](docs/CEPHFS_ARCHITECTURE.md) for details on CephFS architecture and troubleshooting.

## Deployment

### Running WebSocket Server

Start the WebSocket server on worker nodes:

```bash
# Basic usage
ws-mars-server --host 0.0.0.0 --port 9001

# With configuration
ws-mars-server --host 0.0.0.0 --port 9001 \
  --shared-root /shared/filesystem \
  --max-connections 20
```

### Systemd Service Example

```ini
[Unit]
Description=MARS WebSocket Server
After=network.target

[Service]
Type=simple
User=mars
ExecStart=/usr/local/bin/ws-mars-server --host 0.0.0.0 --port 9001
Restart=always
RestartSec=10
Environment=MARS_SHARED_ROOT=/cache
Environment=MARS_MAX_CONCURRENT_CONNECTIONS=20

[Install]
WantedBy=multi-user.target
```

## Troubleshooting

### WebSocket Connection Failures

```bash
# Check server is running
ps aux | grep ws-mars-server

# Test connection
curl -i -N -H "Connection: Upgrade" -H "Upgrade: websocket" \
  ws://server:9001
```

### Orphaned Processes

The WebSocket server uses process group management to prevent orphaned processes:

```bash
# Check for orphaned processes
ps aux | grep "[m]ars"

# Server automatically cleans up orphaned processes on startup
```

### CephFS Issues

For CephFS-specific problems:

```bash
# Check CephFS health
check-cephfs-health

# View recent kernel errors
dmesg -T | grep -i ceph | tail -20
```

See [docs/CEPHFS_ARCHITECTURE.md](docs/CEPHFS_ARCHITECTURE.md) for architecture details and troubleshooting.

## Quick Start

```python
>>> import cads_mars_server

```

## Workflow for developers/contributors

For best experience create a new conda environment (e.g. DEVELOP) with Python 3.12:

```
conda create -n DEVELOP -c conda-forge python=3.12
conda activate DEVELOP
```

Before pushing to GitHub, run the following commands:

1. Update conda environment: `make conda-env-update`
1. Install this package: `pip install -e .`
1. Sync with the latest [template](https://github.com/ecmwf-projects/cookiecutter-conda-package) (optional): `make template-update`
1. Run quality assurance checks: `make qa`
1. Run tests: `make unit-tests`
1. Run the static type checker: `make type-check`
1. Build the documentation (see [Sphinx tutorial](https://www.sphinx-doc.org/en/master/tutorial/)): `make docs-build`

## License

```
Copyright 2024, European Union.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
```
