# Configuration Guide

## Overview

`cads-mars-server` supports two configuration methods:

1. **YAML Configuration File** (recommended for systemd services)
2. **Environment Variables** (recommended for Kubernetes/containers)

**Configuration Precedence:**
1. Environment variables (highest priority)
2. YAML configuration file
3. Built-in defaults (lowest priority)

---

## YAML Configuration File

### Default Location

```
/etc/cads-mars-server.yaml
```

### Custom Location

Set via environment variable:
```bash
export MARS_CONFIG_FILE=/path/to/custom-config.yaml
```

### Example Configuration

```yaml
# Server ports
pipe_port: 9000
shares_port: 9001

# Shared filesystem
shared_root: /mnt/cephfs

# Client selection
use_shares: true

# Retry configuration
retry_delay: 3
max_retries: 15
request_timeout: 60

# WebSocket settings
heartbeat_interval: 30
ws_close_timeout: 30

# Logging
debug_mode: false
```

### For systemd Services

1. **Copy example configuration:**
   ```bash
   sudo cp cads-mars-server.yaml.example /etc/cads-mars-server.yaml
   sudo chmod 644 /etc/cads-mars-server.yaml
   ```

2. **Edit configuration:**
   ```bash
   sudo vim /etc/cads-mars-server.yaml
   ```

3. **Install and start service:**
   ```bash
   sudo cp cads-mars-server.service.example /etc/systemd/system/cads-mars-server.service
   sudo systemctl daemon-reload
   sudo systemctl enable cads-mars-server
   sudo systemctl start cads-mars-server
   ```

4. **Check status:**
   ```bash
   sudo systemctl status cads-mars-server
   sudo journalctl -u cads-mars-server -f
   ```

---

## Environment Variables

All configuration values can be overridden with environment variables. This is the recommended approach for containerized deployments.

### Available Variables

| Environment Variable | YAML Key | Default | Description |
|---------------------|----------|---------|-------------|
| `MARS_CONFIG_FILE` | N/A | `/etc/cads-mars-server.yaml` | Path to config file |
| `MARS_PIPE_PORT` | `pipe_port` | `9000` | Pipe server port |
| `MARS_SHARES_PORT` | `shares_port` | `9001` | WebSocket server port |
| `MARS_SHARED_ROOT` | `shared_root` | `/cache` | Shared filesystem root |
| `MARS_USE_SHARES` | `use_shares` | `false` | Use WebSocket client |
| `MARS_RETRY_DELAY` | `retry_delay` | `2` | Retry delay (seconds) |
| `MARS_MAX_RETRIES` | `max_retries` | `10` | Maximum retries |
| `MARS_REQUEST_TIMEOUT` | `request_timeout` | `30` | Request timeout (seconds) |
| `MARS_HEARTBEAT_INTERVAL` | `heartbeat_interval` | `20` | Heartbeat interval (seconds) |
| `MARS_WS_CLOSE_TIMEOUT` | `ws_close_timeout` | `30` | WebSocket close timeout |
| `MARS_WS_PING_INTERVAL` | `ws_ping_interval` | `null` | WebSocket ping interval |
| `MARS_WS_DEBUG` | `debug_mode` | `false` | Enable debug logging |

### For Kubernetes Deployments

Create a ConfigMap:

```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: mars-server-config
data:
  MARS_USE_SHARES: "true"
  MARS_SHARES_PORT: "9001"
  MARS_SHARED_ROOT: "/mnt/cephfs"
  MARS_RETRY_DELAY: "3"
  MARS_MAX_RETRIES: "15"
  MARS_REQUEST_TIMEOUT: "60"
  MARS_HEARTBEAT_INTERVAL: "30"
```

Reference in deployment:

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: mars-server
spec:
  template:
    spec:
      containers:
      - name: mars-server
        image: mars-server:latest
        envFrom:
        - configMapRef:
            name: mars-server-config
```

---

## Configuration Examples

### Example 1: Production with Shared Filesystem

**YAML (`/etc/cads-mars-server.yaml`):**
```yaml
use_shares: true
shared_root: /mnt/cephfs
pipe_port: 9000
shares_port: 9001
retry_delay: 3
max_retries: 15
request_timeout: 60
heartbeat_interval: 30
ws_close_timeout: 45
debug_mode: false
```

**Environment Override (takes precedence):**
```bash
# Enable debug mode temporarily
export MARS_WS_DEBUG=true
```

### Example 2: Development Environment

**YAML:**
```yaml
use_shares: false
pipe_port: 9000
shared_root: /tmp/mars_cache
debug_mode: true
retry_delay: 1
max_retries: 3
request_timeout: 30
```

### Example 3: Kubernetes with CephFS

**ConfigMap:**
```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: mars-server-config
data:
  MARS_USE_SHARES: "true"
  MARS_SHARED_ROOT: "/mnt/cephfs"
  MARS_SHARES_PORT: "9001"
  MARS_RETRY_DELAY: "3"
  MARS_MAX_RETRIES: "15"
  MARS_REQUEST_TIMEOUT: "90"
  MARS_HEARTBEAT_INTERVAL: "30"
```

---

## Validation

### Check Current Configuration

You can verify loaded configuration in Python:

```python
from cads_mars_server.config import (
    DEFAULT_PIPE_PORT,
    DEFAULT_SHARES_PORT,
    USE_SHARES,
    SHARED_ROOT,
    DEBUG_MODE,
)

print(f"Pipe Port: {DEFAULT_PIPE_PORT}")
print(f"Shares Port: {DEFAULT_SHARES_PORT}")
print(f"Use Shares: {USE_SHARES}")
print(f"Shared Root: {SHARED_ROOT}")
print(f"Debug Mode: {DEBUG_MODE}")
```

### Test Configuration Loading

```bash
# Test with YAML file
cat > /tmp/test-config.yaml <<EOF
use_shares: true
shares_port: 9999
debug_mode: true
EOF

MARS_CONFIG_FILE=/tmp/test-config.yaml python3 -c "
from cads_mars_server.config import DEFAULT_SHARES_PORT, USE_SHARES, DEBUG_MODE
print(f'Shares Port: {DEFAULT_SHARES_PORT}')
print(f'Use Shares: {USE_SHARES}')
print(f'Debug Mode: {DEBUG_MODE}')
"

# Expected output:
# Shares Port: 9999
# Use Shares: True
# Debug Mode: True
```

---

## Troubleshooting

### Configuration Not Loading

**Problem:** Settings in YAML file are ignored.

**Solutions:**
1. Check file location:
   ```bash
   echo $MARS_CONFIG_FILE
   ls -l /etc/cads-mars-server.yaml
   ```

2. Verify YAML syntax:
   ```bash
   python3 -c "import yaml; print(yaml.safe_load(open('/etc/cads-mars-server.yaml')))"
   ```

3. Check file permissions:
   ```bash
   chmod 644 /etc/cads-mars-server.yaml
   ```

4. Install PyYAML if missing:
   ```bash
   pip install pyyaml
   ```

### Environment Variables Not Working

**Problem:** Environment variables are ignored.

**Solutions:**
1. Verify variable is set:
   ```bash
   echo $MARS_USE_SHARES
   ```

2. For systemd, ensure variables are in service file:
   ```ini
   [Service]
   Environment="MARS_USE_SHARES=true"
   ```

3. Reload systemd after changes:
   ```bash
   sudo systemctl daemon-reload
   sudo systemctl restart cads-mars-server
   ```

### Configuration Precedence Issues

**Problem:** Not sure which configuration is being used.

**Solution:** Environment variables ALWAYS override file settings:

```yaml
# /etc/cads-mars-server.yaml
use_shares: false  # Will be overridden
```

```bash
export MARS_USE_SHARES=true  # This wins
```

### Debug Configuration Loading

Enable debug mode to see configuration loading:

```bash
export MARS_WS_DEBUG=true
python3 -m cads_mars_server.ws_main
```

---

## Migration from Environment-Only Configuration

If you're upgrading from a version that only used environment variables:

1. **No action required** - Environment variables still work exactly as before
2. **Optional:** Create YAML file for easier management
3. **Recommended:** For systemd services, migrate to YAML configuration

**Migration steps:**

1. Create configuration file:
   ```bash
   sudo cp cads-mars-server.yaml.example /etc/cads-mars-server.yaml
   ```

2. Transfer settings from environment to YAML:
   ```bash
   # Old: systemd service had many Environment= lines
   # New: Single config file with all settings
   ```

3. Remove environment variables from service file (optional):
   ```bash
   sudo vim /etc/systemd/system/cads-mars-server.service
   # Remove or comment out Environment= lines
   ```

4. Reload and restart:
   ```bash
   sudo systemctl daemon-reload
   sudo systemctl restart cads-mars-server
   ```

---

## Best Practices

### For Production

1. **Use YAML file** for systemd services
2. **Keep sensitive values** out of config file if possible
3. **Use environment variables** for deployment-specific overrides
4. **Version control** your config file template
5. **Document** any non-default settings

### For Development

1. **Use environment variables** for quick testing
2. **Keep a local config file** for consistent development environment
3. **Enable debug mode** during development

### For Kubernetes

1. **Use ConfigMaps** for configuration
2. **Use Secrets** for sensitive values (if any)
3. **Don't use config files** - stick with environment variables
4. **Keep defaults** in deployment manifests

---

## Dependencies

- **PyYAML** (optional): Required for YAML file support
  ```bash
  pip install pyyaml
  ```
  
  If PyYAML is not installed, the server will fall back to environment variables and built-in defaults.

---

## Security Considerations

1. **File Permissions:** Ensure config file has appropriate permissions
   ```bash
   sudo chown root:mars /etc/cads-mars-server.yaml
   sudo chmod 640 /etc/cads-mars-server.yaml
   ```

2. **No Secrets:** Don't store passwords or tokens in config file

3. **Systemd Hardening:** Use systemd security features (see example service file)

4. **Shared Filesystem:** Ensure proper access controls on shared filesystem

---

## Getting Help

If you have configuration issues:

1. Check this documentation
2. Verify configuration precedence
3. Enable debug mode to see what's happening
4. Check system logs: `journalctl -u cads-mars-server`
5. Contact support with your (sanitized) configuration
