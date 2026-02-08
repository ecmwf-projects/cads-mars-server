"""
Centralized configuration for MARS server and client components.

Configuration precedence (highest to lowest):
1. Environment variables (MARS_*)
2. Configuration file (YAML)
3. Built-in defaults

For systemd services, use /etc/cads-mars-server.yaml as primary configuration.
For Kubernetes pods, use environment variables.
"""

import os
from pathlib import Path
from typing import Any, Optional

# Default configuration file path
DEFAULT_CONFIG_FILE = "/etc/cads-mars-server.yaml"
CONFIG_FILE = os.getenv("MARS_CONFIG_FILE", DEFAULT_CONFIG_FILE)


def _load_yaml_config() -> dict[str, Any]:
    """
    Load configuration from YAML file if it exists.
    
    Returns:
        Dictionary with configuration values, or empty dict if file doesn't exist.
    """
    config_path = Path(CONFIG_FILE)
    
    if not config_path.exists():
        return {}
    
    try:
        import yaml
        with open(config_path) as f:
            config = yaml.safe_load(f) or {}
        return config
    except ImportError:
        # PyYAML not installed, skip file-based config
        return {}
    except Exception as e:
        import warnings
        warnings.warn(f"Failed to load config file {CONFIG_FILE}: {e}")
        return {}


# Load file-based configuration
_file_config = _load_yaml_config()


def _get_config(
    env_key: str,
    file_key: str,
    default: Any,
    cast_type: type = str,
) -> Any:
    """
    Get configuration value with proper precedence.
    
    Precedence: ENV > FILE > DEFAULT
    
    Args:
        env_key: Environment variable name (e.g., "MARS_PIPE_PORT")
        file_key: Key in YAML file (e.g., "pipe_port")
        default: Default value if not found
        cast_type: Type to cast the value to
        
    Returns:
        Configuration value with proper type
    """
    # 1. Check environment variable
    env_value = os.getenv(env_key)
    if env_value is not None:
        if cast_type == bool:
            return env_value.lower() in ("true", "1", "yes", "on")
        elif cast_type == Path:
            return Path(env_value)
        else:
            return cast_type(env_value)
    
    # 2. Check file config
    file_value = _file_config.get(file_key)
    if file_value is not None:
        if cast_type == Path:
            return Path(file_value)
        elif cast_type == bool:
            return bool(file_value)
        else:
            return cast_type(file_value)
    
    # 3. Use default
    return default


# ============================================================================
# Server Configuration
# ============================================================================

# Default server ports
DEFAULT_PIPE_PORT = _get_config("MARS_PIPE_PORT", "pipe_port", 9000, int)
DEFAULT_SHARES_PORT = _get_config("MARS_SHARES_PORT", "shares_port", 9001, int)

# Shared filesystem configuration (for shares/websocket mode)
SHARED_ROOT = _get_config("MARS_SHARED_ROOT", "shared_root", "/cache", Path)

# ============================================================================
# WebSocket Configuration
# ============================================================================

# Heartbeat interval to keep load balancers from closing idle connections
HEARTBEAT_INTERVAL = _get_config("MARS_HEARTBEAT_INTERVAL", "heartbeat_interval", 20, int)

# Timeout for websocket close operations
WS_CLOSE_TIMEOUT = _get_config("MARS_WS_CLOSE_TIMEOUT", "ws_close_timeout", 30, int)

# Ping interval for websocket connections (None to disable)
# Special handling for optional int
_ws_ping_env = os.getenv("MARS_WS_PING_INTERVAL")
_ws_ping_file = _file_config.get("ws_ping_interval")
if _ws_ping_env is not None:
    WS_PING_INTERVAL: Optional[int] = int(_ws_ping_env)
elif _ws_ping_file is not None:
    WS_PING_INTERVAL = int(_ws_ping_file)
else:
    WS_PING_INTERVAL = None

# ============================================================================
# Client Configuration
# ============================================================================

# Retry configuration for client operations
RETRY_DELAY = _get_config("MARS_RETRY_DELAY", "retry_delay", 2, int)
MAX_RETRIES = _get_config("MARS_MAX_RETRIES", "max_retries", 10, int)

# Request timeout for individual server attempts
REQUEST_TIMEOUT = _get_config("MARS_REQUEST_TIMEOUT", "request_timeout", 30, int)

# ============================================================================
# Logging Configuration
# ============================================================================

# Enable debug logging for websocket operations
DEBUG_MODE = _get_config("MARS_WS_DEBUG", "debug_mode", False, bool)

# ============================================================================
# Client Selection
# ============================================================================

# Use shares (websocket) client instead of pipe client
USE_SHARES = _get_config("MARS_USE_SHARES", "use_shares", False, bool)
