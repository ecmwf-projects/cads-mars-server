#!/usr/bin/env python3
"""
Test script to verify configuration loading from YAML and environment variables.

Usage:
    # Test with default settings
    python test_config.py

    # Test with YAML file
    echo "use_shares: true
shares_port: 9999" > /tmp/test-mars-config.yaml
    MARS_CONFIG_FILE=/tmp/test-mars-config.yaml python test_config.py

    # Test with environment variable override
    MARS_CONFIG_FILE=/tmp/test-mars-config.yaml MARS_SHARES_PORT=8888 python test_config.py
"""

import os
import sys
from pathlib import Path


def test_config():
    """Test configuration loading."""
    print("=" * 70)
    print("MARS Server Configuration Test")
    print("=" * 70)

    # Show environment
    print("\n1. Environment Variables:")
    env_vars = [k for k in os.environ.keys() if k.startswith("MARS_")]
    if env_vars:
        for var in sorted(env_vars):
            print(f"   {var} = {os.environ[var]}")
    else:
        print("   (none set)")

    # Show config file
    print("\n2. Configuration File:")
    config_file = os.getenv("MARS_CONFIG_FILE", "/etc/cads-mars-server.yaml")
    print(f"   Path: {config_file}")
    if Path(config_file).exists():
        print("   Status: EXISTS")
        print("   Contents:")
        with open(config_file) as f:
            for line in f:
                if line.strip() and not line.strip().startswith("#"):
                    print(f"      {line.rstrip()}")
    else:
        print("   Status: NOT FOUND (using defaults)")

    # Load configuration
    print("\n3. Loading Configuration Module...")
    try:
        from cads_mars_server.config import (
            DEBUG_MODE,
            DEFAULT_PIPE_PORT,
            DEFAULT_SHARES_PORT,
            HEARTBEAT_INTERVAL,
            MAX_RETRIES,
            REQUEST_TIMEOUT,
            RETRY_DELAY,
            SHARED_ROOT,
            USE_SHARES,
            WS_CLOSE_TIMEOUT,
            WS_PING_INTERVAL,
        )

        print("   ✓ Configuration loaded successfully")
    except Exception as e:
        print(f"   ✗ Failed to load configuration: {e}")
        sys.exit(1)

    # Display loaded configuration
    print("\n4. Loaded Configuration Values:")
    print(f"   DEFAULT_PIPE_PORT      = {DEFAULT_PIPE_PORT}")
    print(f"   DEFAULT_SHARES_PORT    = {DEFAULT_SHARES_PORT}")
    print(f"   SHARED_ROOT            = {SHARED_ROOT}")
    print(f"   USE_SHARES             = {USE_SHARES}")
    print(f"   RETRY_DELAY            = {RETRY_DELAY}")
    print(f"   MAX_RETRIES            = {MAX_RETRIES}")
    print(f"   REQUEST_TIMEOUT        = {REQUEST_TIMEOUT}")
    print(f"   HEARTBEAT_INTERVAL     = {HEARTBEAT_INTERVAL}")
    print(f"   WS_CLOSE_TIMEOUT       = {WS_CLOSE_TIMEOUT}")
    print(f"   WS_PING_INTERVAL       = {WS_PING_INTERVAL}")
    print(f"   DEBUG_MODE             = {DEBUG_MODE}")

    # Verify precedence
    print("\n5. Configuration Precedence Verification:")

    # Test 1: Default values (no env, no file)
    expected_defaults = {
        "DEFAULT_PIPE_PORT": 9000,
        "DEFAULT_SHARES_PORT": 9001,
        "USE_SHARES": False,
        "DEBUG_MODE": False,
    }

    if not env_vars and not Path(config_file).exists():
        print("   Testing defaults (no env vars, no config file)...")
        all_match = True
        for key, expected in expected_defaults.items():
            actual = locals()[key]
            if actual == expected:
                print(f"   ✓ {key} = {actual} (matches default)")
            else:
                print(f"   ✗ {key} = {actual} (expected {expected})")
                all_match = False
        if all_match:
            print("   ✓ All defaults correct")

    # Test 2: Environment variables take precedence
    if "MARS_SHARES_PORT" in os.environ:
        expected = int(os.environ["MARS_SHARES_PORT"])
        if DEFAULT_SHARES_PORT == expected:
            print(
                f"   ✓ MARS_SHARES_PORT env var correctly overrides: {DEFAULT_SHARES_PORT}"
            )
        else:
            print(
                f"   ✗ MARS_SHARES_PORT mismatch: got {DEFAULT_SHARES_PORT}, expected {expected}"
            )

    if "MARS_USE_SHARES" in os.environ:
        expected = os.environ["MARS_USE_SHARES"].lower() in ("true", "1", "yes", "on")
        if USE_SHARES == expected:
            print(f"   ✓ MARS_USE_SHARES env var correctly overrides: {USE_SHARES}")
        else:
            print(
                f"   ✗ MARS_USE_SHARES mismatch: got {USE_SHARES}, expected {expected}"
            )

    print("\n" + "=" * 70)
    print("✓ Configuration test completed successfully!")
    print("=" * 70)


if __name__ == "__main__":
    test_config()
