# MARS Log Filtering

## Overview

The WebSocket client includes intelligent log filtering to reduce noise from MARS output while highlighting important information. You can use the default filtering or inject custom log handlers with advanced capabilities.

## Default Log Filtering

The default log parser (`MarsLogParser`) processes each line received from the server:

1. **Pattern Matching**: Identifies important lines (errors, warnings, progress, transfers)
1. **Deduplication**: Suppresses repeated messages after showing them a few times
1. **Formatting**: Adds visual markers (❌ ⚠️ ✓) for quick scanning

## Configuration

### Enable/Disable Filtering

```yaml
# In /etc/cads-mars-server.yaml
client_filter_logs: true  # Default: true
```

Or via environment variable:

```bash
export MARS_CLIENT_FILTER_LOGS=false  # Show all logs
```

### Programmatic Control

```python
from cads_mars_server.ws_client import mars_via_ws

# Use filtering (default)
result = await mars_via_ws(servers, requests, environ, target)

# Show all logs
result = await mars_via_ws(servers, requests, environ, target, filter_logs=False)
```

## Custom Log Handlers

For advanced use cases, you can inject a custom log handler that can:

- Parse logs with custom logic
- Raise exceptions to abort requests on specific conditions
- Send commands to the server (e.g., kill signal)
- Trigger external actions

### Custom Handler Interface

A log handler is an async function with this signature:

```python
async def custom_handler(line: str, ws: WebSocket, logger: Any) -> Optional[str]:
    """
    Process a MARS log line.

    Args:
        line: Raw log line from MARS
        ws: WebSocket connection (can send commands like {"cmd": "kill"})
        logger: Logger instance for custom logging

    Returns:
        Formatted string to display, or None to suppress

    Raises:
        Exception: Any exception will abort the MARS request
    """
    pass
```

### Example 1: Abort on Fatal Errors

```python
import json
from cads_adaptors.adaptors.mars import execute_mars


async def abort_on_fatal(line: str, ws, logger) -> Optional[str]:
    """Abort request if MARS encounters fatal errors."""
    if "FATAL ERROR" in line or "CRITICAL" in line:
        # Send kill command to server
        await ws.send(json.dumps({"cmd": "kill"}))
        # Abort by raising exception
        raise RuntimeError(f"MARS fatal error detected: {line}")

    # Show errors and warnings
    if any(keyword in line.upper() for keyword in ["ERROR", "WARNING"]):
        return f"⚠️  {line}"

    # Suppress other lines
    return None


# Use in adaptor code
result = execute_mars(
    request, context=context, target_dir=cache_path, log_handler=abort_on_fatal
)
```

### Example 2: Timeout Detection

```python
import json


async def timeout_detector(line: str, ws, logger) -> Optional[str]:
    """Detect and abort on timeout conditions."""

    # Check for explicit timeouts
    if "TIMEOUT" in line.upper() or "TIMED OUT" in line.upper():
        await ws.send(json.dumps({"cmd": "kill"}))
        raise TimeoutError(f"MARS request timed out: {line}")

    # Show important lines
    if any(kw in line.upper() for kw in ["ERROR", "WARNING", "COMPLETE", "%"]):
        return line

    return None


# Usage
result = execute_mars(request, context=context, log_handler=timeout_detector)
```

### Example 3: Combining with Default Handler

```python
from cads_mars_server.log_filter import create_default_log_handler

# Create default handler
default_handler = create_default_log_handler(filter_logs=True)


async def custom_with_default(line: str, ws, logger) -> Optional[str]:
    """Custom logic + default filtering."""

    # Custom handling for specific patterns
    if "TAPE MOUNT" in line:
        logger.info("Waiting for tape mount...")
        return f"💾 {line}"

    # Abort on data corruption
    if "CHECKSUM FAILED" in line:
        import json

        await ws.send(json.dumps({"cmd": "kill"}))
        raise ValueError("Data corruption detected")

    # Use default handler for everything else
    return await default_handler(line, ws, logger)


# Usage
result = execute_mars(request, context=context, log_handler=custom_with_default)
```

### Using in Adaptor Classes

```python
from cads_adaptors.adaptors.mars import MarsCdsAdaptor, execute_mars


class CustomMarsCdsAdaptor(MarsCdsAdaptor):
    async def my_log_handler(self, line: str, ws, logger) -> Optional[str]:
        """Custom log handler for this adaptor."""

        # Handle dataset-specific patterns
        if "DATA QUALITY CHECK" in line:
            if "FAILED" in line:
                raise ValueError(f"Data quality check failed: {line}")
            return f"✓ {line}"

        # Use default for other lines
        from cads_mars_server.log_filter import create_default_log_handler

        default = create_default_log_handler(filter_logs=True)
        return await default(line, ws, logger)

    def retrieve(self, request):
        result = execute_mars(
            request,
            context=self.context,
            target_dir=self.cache_tmp_path,
            log_handler=self.my_log_handler,  # Inject custom handler
        )
        return open(result, "rb")
```

## Example Output

### Before (Unfiltered)

```
Connecting to server
Session initiated
Checking credentials
Credentials validated
Starting retrieval process
Parsing request
Request parsed successfully
Allocating resources
Resources allocated
Initializing connection to tape library
Connection established
Positioning tape drive
Tape drive positioned
Reading block 1 of 500
Reading block 2 of 500
Reading block 3 of 500
...
Reading block 498 of 500
Reading block 499 of 500
Reading block 500 of 500
Validating data integrity
Data validation complete
Transferring 245.32 MB
Transfer complete
Closing connection
Connection closed
Clean up complete
```

### After (Filtered)

```
Connecting to server
✓ Credentials validated
✓ Connection established to tape library
Transferring 245.32 MB
✓ Transfer complete
```

### With Errors

```
Connecting to server
✓ Credentials validated
❌ ERROR: Tape drive timeout after 30s
⚠️  WARNING: Retrying with different drive
✓ Connection established to tape library
Transferring 245.32 MB
✓ Transfer complete
```

## What Gets Filtered

### Always Shown

- Lines containing: error, warning, failed, exception, timeout, fatal, critical
- Progress indicators: "retrieving X bytes", "transferred", "% complete"
- Important status: "complete", "success"

### Suppressed

- Verbose debug output
- Repeated messages (after 3 occurrences)
- Routine operational messages

### All Logs Still Stored

The `Result.message` field contains **all logs** (unfiltered). The filtering only affects what's displayed to the user in real-time.

## Benefits

1. **Faster Debugging**: Errors and warnings stand out immediately
1. **Reduced Noise**: Focus on what matters
1. **Better UX**: Clean, readable output for end users
1. **Full History**: All logs still available in `Result.message` for detailed analysis

## Notes

- Filtering happens **client-side only** - server still sends all logs
- No data is lost - filtering is display-only
- Can be disabled globally or per-request
- Parser maintains state to detect repeated messages within a single request
