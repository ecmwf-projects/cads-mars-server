"""
Example custom log handlers for MARS requests.

These examples demonstrate how to inject custom log parsing logic
into MARS retrievals to handle specific conditions.
"""
import json
from typing import Optional, Any


async def abort_on_fatal_error(line: str, ws: Any, logger: Any) -> Optional[str]:
    """
    Abort the MARS request if fatal errors are detected.
    
    This handler:
    - Sends kill signal to server on fatal errors
    - Raises exception to abort the request
    - Highlights all errors and warnings
    - Suppresses routine messages
    """
    line_upper = line.upper()
    
    # Fatal conditions - abort immediately
    if any(keyword in line_upper for keyword in ["FATAL", "CRITICAL", "CORRUPTED"]):
        # Kill the server process
        await ws.send(json.dumps({"cmd": "kill"}))
        # Abort the request
        raise RuntimeError(f"MARS fatal error: {line}")
    
    # Highlight errors and warnings
    if "ERROR" in line_upper:
        return f"❌ {line}"
    if "WARNING" in line_upper:
        return f"⚠️  {line}"
    
    # Show progress
    if "%" in line or "COMPLETE" in line_upper:
        return f"✓ {line}"
    
    # Suppress other lines
    return None


async def timeout_monitor(line: str, ws: Any, logger: Any) -> Optional[str]:
    """
    Monitor for timeout conditions and abort.
    
    Use this when MARS requests are known to hang on certain conditions.
    """
    line_upper = line.upper()
    
    # Detect timeout conditions
    timeout_keywords = [
        "TIMEOUT", "TIMED OUT", "NO RESPONSE",
        "WAITING FOR", "STUCK", "HUNG"
    ]
    
    if any(keyword in line_upper for keyword in timeout_keywords):
        logger.warning(f"Timeout detected: {line}")
        
        # Give it one more chance on first timeout
        if "RETRY" not in line_upper:
            await ws.send(json.dumps({"cmd": "kill"}))
            raise TimeoutError(f"Request timed out: {line}")
    
    # Show important status
    if any(kw in line_upper for kw in ["ERROR", "WARNING", "PROGRESS", "%"]):
        return line
    
    return None


class MetricsCollector:
    """
    Collect metrics from MARS log output.
    
    Usage:
        metrics = MetricsCollector()
        result = execute_mars(request, context=context, log_handler=metrics)
        print(f"Errors: {metrics.errors}, Bytes: {metrics.bytes_transferred}")
    """
    
    def __init__(self):
        self.errors = 0
        self.warnings = 0
        self.bytes_transferred = 0
        self.tape_mounts = 0
        self.start_time = None
    
    async def __call__(self, line: str, ws: Any, logger: Any) -> Optional[str]:
        """Process log line and collect metrics."""
        line_upper = line.upper()
        
        # Count errors and warnings
        if "ERROR" in line_upper:
            self.errors += 1
            return f"❌ [{self.errors}] {line}"
        
        if "WARNING" in line_upper:
            self.warnings += 1
            return f"⚠️  [{self.warnings}] {line}"
        
        # Track tape operations
        if "TAPE" in line_upper and ("MOUNT" in line_upper or "LOAD" in line_upper):
            self.tape_mounts += 1
            return f"💾 Tape mount #{self.tape_mounts}: {line}"
        
        # Extract transfer sizes
        if "TRANSFERRED" in line_upper or "RETRIEVED" in line_upper:
            import re
            # Parse sizes like "245.32 MB" or "1.5 GB"
            match = re.search(r'(\d+(?:\.\d+)?)\s*(MB|GB|KB)', line_upper)
            if match:
                size = float(match.group(1))
                unit = match.group(2)
                
                # Convert to bytes
                multiplier = {"KB": 1024, "MB": 1024**2, "GB": 1024**3}
                self.bytes_transferred += int(size * multiplier.get(unit, 1))
                
                return f"📊 {line} (Total: {self.bytes_transferred / 1024**2:.2f} MB)"
        
        # Show progress indicators
        if "%" in line or "COMPLETE" in line_upper:
            return line
        
        # Suppress routine messages
        return None
    
    def summary(self) -> str:
        """Get a summary of collected metrics."""
        return (
            f"Metrics Summary:\n"
            f"  Errors: {self.errors}\n"
            f"  Warnings: {self.warnings}\n"
            f"  Tape Mounts: {self.tape_mounts}\n"
            f"  Data Transferred: {self.bytes_transferred / 1024**2:.2f} MB"
        )


async def data_quality_checker(line: str, ws: Any, logger: Any) -> Optional[str]:
    """
    Check for data quality issues and abort on failures.
    
    Use this for datasets that require specific quality checks.
    """
    line_upper = line.upper()
    
    # Quality check patterns
    quality_issues = [
        "CHECKSUM", "CORRUPT", "INVALID", "MALFORMED",
        "INCOMPLETE", "MISSING REQUIRED"
    ]
    
    for issue in quality_issues:
        if issue in line_upper and any(fail in line_upper for fail in ["FAIL", "ERROR"]):
            # Send kill and abort
            await ws.send(json.dumps({"cmd": "kill"}))
            raise ValueError(f"Data quality check failed: {line}")
    
    # Show quality-related messages
    if any(kw in line_upper for kw in ["QUALITY", "VALIDATION", "VERIFY", "CHECK"]):
        if "PASS" in line_upper or "OK" in line_upper or "SUCCESS" in line_upper:
            return f"✓ {line}"
        else:
            return f"⚙️  {line}"
    
    # Show errors and progress
    if any(kw in line_upper for kw in ["ERROR", "WARNING", "%", "COMPLETE"]):
        return line
    
    return None


async def combined_handler(line: str, ws: Any, logger: Any) -> Optional[str]:
    """
    Combine multiple handlers - quality checks + default filtering.
    
    This demonstrates how to layer custom logic on top of default filtering.
    """
    from cads_mars_server.log_filter import create_default_log_handler
    
    line_upper = line.upper()
    
    # First, check for critical conditions
    if "FATAL" in line_upper or "CRITICAL" in line_upper:
        await ws.send(json.dumps({"cmd": "kill"}))
        raise RuntimeError(f"Critical error: {line}")
    
    # Custom handling for dataset-specific patterns
    if "TAPE" in line_upper:
        if "MOUNT" in line_upper:
            return f"💾 {line}"
        elif "ERROR" in line_upper:
            return f"💾❌ {line}"
    
    # Use default handler for everything else
    default = create_default_log_handler(filter_logs=True)
    return await default(line, ws, logger)


# Example usage in adaptor code:
"""
from cads_adaptors.adaptors.mars import execute_mars

# Simple abort on fatal
result = execute_mars(
    request,
    context=context,
    target_dir=cache_path,
    log_handler=abort_on_fatal_error
)

# Or with metrics collection
metrics = MetricsCollector()
result = execute_mars(
    request,
    context=context,
    target_dir=cache_path,
    log_handler=metrics
)
print(metrics.summary())
"""
