"""
Centralized configuration for MARS server and client components.

Configuration precedence (highest to lowest):
1. Environment variables (MARS_*)
2. Configuration file (YAML)
3. Built-in defaults

For systemd services, use /etc/cads-mars-server.yaml as primary configuration.
For Kubernetes pods, use environment variables.

Strictness: when MARS_CONFIG_FILE is set explicitly, the file MUST exist and
be readable (PyYAML installed, valid YAML) — any failure raises ConfigError
so services fail fast instead of silently running with built-in defaults.
When the default path (/etc/cads-mars-server.yaml) is used implicitly, a
missing file is fine, but an unreadable/unparseable one emits a warning.
"""

import os
import warnings
from pathlib import Path
from typing import Any, Optional

# Default configuration file path
DEFAULT_CONFIG_FILE = "/etc/cads-mars-server.yaml"
CONFIG_FILE = os.getenv("MARS_CONFIG_FILE", DEFAULT_CONFIG_FILE)

# True when the config file location was set explicitly via MARS_CONFIG_FILE.
# Explicit configuration is mandatory: failures to load it are fatal.
CONFIG_FILE_EXPLICIT = os.getenv("MARS_CONFIG_FILE") is not None


class ConfigError(RuntimeError):
    """Raised when an explicitly requested configuration cannot be loaded."""


# File keys are normalized to lowercase; these aliases map legacy key names
# (written by older cds-ansible templates / shared with ds-cache tooling)
# onto the canonical names used here.
_KEY_ALIASES = {
    "cache_root": "shared_root",
}

_TRUTHY = ("true", "1", "yes", "on")


def _fail_or_warn(message: str) -> None:
    """Raise if the config file was requested explicitly, warn otherwise."""
    if CONFIG_FILE_EXPLICIT:
        raise ConfigError(message)
    warnings.warn(message)


def _load_yaml_config() -> dict[str, Any]:
    """
    Load configuration from YAML file.

    Keys are normalized to lowercase and legacy aliases (e.g. CACHE_ROOT)
    are mapped to their canonical names (shared_root).

    Returns
    -------
        Dictionary with configuration values. Empty dict if the default
        config file doesn't exist or cannot be read (with a warning).

    Raises
    ------
        ConfigError
            If MARS_CONFIG_FILE was set explicitly and the file is missing,
            PyYAML is not installed, or the file cannot be parsed.
    """
    config_path = Path(CONFIG_FILE)

    if not config_path.exists():
        if CONFIG_FILE_EXPLICIT:
            raise ConfigError(
                f"Configuration file not found: MARS_CONFIG_FILE={CONFIG_FILE}"
            )
        return {}

    try:
        import yaml  # type: ignore[import-untyped]
    except ImportError:
        _fail_or_warn(
            f"PyYAML is not installed: configuration file {CONFIG_FILE} "
            "cannot be read and is IGNORED. Install PyYAML (pip install pyyaml)."
        )
        return {}

    try:
        with open(config_path) as f:
            raw = yaml.safe_load(f) or {}
    except Exception as e:
        _fail_or_warn(f"Failed to load config file {CONFIG_FILE}: {e}")
        return {}

    if not isinstance(raw, dict):
        _fail_or_warn(
            f"Config file {CONFIG_FILE} must contain a YAML mapping, "
            f"got {type(raw).__name__}: content IGNORED."
        )
        return {}

    normalized: dict[str, Any] = {}
    for key, value in raw.items():
        k = str(key).lower()
        normalized[_KEY_ALIASES.get(k, k)] = value
    return normalized


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

    Returns
    -------
        Configuration value with proper type
    """
    # 1. Check environment variable
    env_value = os.getenv(env_key)
    if env_value is not None:
        if cast_type is bool:
            return env_value.lower() in _TRUTHY
        elif cast_type is Path:
            return Path(env_value)
        else:
            return cast_type(env_value)

    # 2. Check file config
    file_value = _file_config.get(file_key)
    if file_value is not None:
        if cast_type is Path:
            return Path(file_value)
        elif cast_type is bool:
            # YAML strings like "false" must not become True
            if isinstance(file_value, str):
                return file_value.strip().lower() in _TRUTHY
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

# List of shared volume names under SHARED_ROOT (for stream server mode)
# Each volume is a subdirectory of SHARED_ROOT.  Requests are distributed
# across available volumes to avoid hot-spotting a single mount.
_shares_env = os.getenv("MARS_SHARES")
_shares_file = _file_config.get("shares")
if _shares_env is not None:
    SHARES: list[str] = [s.strip() for s in _shares_env.split(",") if s.strip()]
elif _shares_file is not None:
    SHARES = (
        list(_shares_file) if isinstance(_shares_file, list) else [str(_shares_file)]
    )
else:
    SHARES = []

# Sub-folder inside each share used for MARS output data
CACHE_FOLDER = _get_config("MARS_CACHE_FOLDER", "cache_folder", "mars", str)

# ============================================================================
# WebSocket Configuration
# ============================================================================

# Heartbeat interval to keep load balancers from closing idle connections
HEARTBEAT_INTERVAL = _get_config(
    "MARS_HEARTBEAT_INTERVAL", "heartbeat_interval", 20, int
)

# Timeout for websocket close operations
WS_CLOSE_TIMEOUT = _get_config("MARS_WS_CLOSE_TIMEOUT", "ws_close_timeout", 30, int)

# Maximum concurrent WebSocket connections (0 = unlimited)
MAX_CONCURRENT_CONNECTIONS = _get_config(
    "MARS_MAX_CONCURRENT_CONNECTIONS", "max_concurrent_connections", 0, int
)

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

# Filter MARS log output to reduce noise (client-side)
CLIENT_FILTER_LOGS = _get_config(
    "MARS_CLIENT_FILTER_LOGS", "client_filter_logs", True, bool
)

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
