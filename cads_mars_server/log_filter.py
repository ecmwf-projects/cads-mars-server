"""
Simple log parser for MARS output to reduce noise and highlight important information.

Used by ws_client to filter logs received from the server.

Custom log handlers can be injected to:
- Parse and filter log lines differently
- Raise exceptions to abort the request
- Send commands to the server (e.g., kill signal)
"""
import re
from typing import Optional, List, Callable, Any, Protocol
from collections import deque


class LogHandlerProtocol(Protocol):
    """
    Protocol for custom log handlers that process MARS log output.
    
    Custom handlers can:
    - Parse and filter log lines
    - Raise exceptions to abort the request
    - Send commands to the server (e.g., kill signal)
    
    Args:
        line: The raw log line from MARS
        ws: The WebSocket connection (can be used to send kill commands)
        logger: Optional logger instance for custom logging
        
    Returns:
        Optional[str]: Formatted line to display, or None to suppress
        
    Raises:
        Exception: Any exception to abort the MARS request
        
    Example custom handler:
        async def my_handler(line: str, ws, logger) -> Optional[str]:
            if "FATAL ERROR" in line:
                # Send kill command and raise exception
                import json
                await ws.send(json.dumps({"cmd": "kill"}))
                raise RuntimeError(f"MARS fatal error: {line}")
            elif "WARNING" in line:
                return f"⚠️  {line}"
            return None  # Suppress other lines
    """
    async def __call__(self, line: str, ws: Any, logger: Any) -> Optional[str]:
        ...


# Type alias for backward compatibility
LogHandler = Callable[[str, Any, Any], Optional[str]]


class MarsLogParser:
    """
    Parse and filter MARS log output to show only relevant information.
    
    Reduces noise by:
    - Showing important patterns (errors, warnings, progress)
    - Hiding verbose debug output
    - Aggregating repeated messages
    """
    
    def __init__(
        self,
        show_all: bool = False,
        important_patterns: Optional[List[str]] = None,
        ignore_patterns: Optional[List[str]] = None
    ):
        """
        Initialize log parser.
        
        Args:
            show_all: If True, show all lines (no filtering)
            important_patterns: Regex patterns to always show
            ignore_patterns: Regex patterns to always hide
        """
        self.show_all = show_all
        
        # Default important patterns
        self.important_patterns = [
            re.compile(p, re.IGNORECASE) for p in (important_patterns or [
                r'error',
                r'warning',
                r'failed',
                r'exception',
                r'timeout',
                r'fatal',
                r'critical',
                r'retrieving.*bytes',
                r'file.*complete',
                r'transferred.*bytes',
                r'\d+%',  # Progress percentages
            ])
        ]
        
        # Default ignore patterns (verbose stuff)
        self.ignore_patterns = [
            re.compile(p, re.IGNORECASE) for p in (ignore_patterns or [
                # Add patterns for very verbose output if needed
            ])
        ]
        
        # Track repeated messages
        self.last_line = None
        self.repeat_count = 0
        self.max_repeats = 3  # Show first 3 occurrences, then suppress
        
    def should_show(self, line: str) -> bool:
        """
        Determine if a log line should be shown to the user.
        
        Args:
            line: The log line to evaluate
            
        Returns:
            True if line should be shown, False to suppress
        """
        if not line.strip():
            return False
            
        if self.show_all:
            return True
        
        # Always show if matches important pattern
        if any(p.search(line) for p in self.important_patterns):
            return True
        
        # Never show if matches ignore pattern
        if any(p.search(line) for p in self.ignore_patterns):
            return False
        
        # Default: show most lines (only filter very verbose stuff)
        return True
    
    def process_line(self, line: str) -> Optional[str]:
        """
        Process a log line and return what should be displayed.
        
        Handles:
        - Filtering based on patterns
        - De-duplicating repeated messages
        - Formatting
        
        Args:
            line: Raw log line from server
            
        Returns:
            Processed line to display, or None to suppress
        """
        if not self.should_show(line):
            return None
        
        # Handle repeated lines
        if line == self.last_line:
            self.repeat_count += 1
            if self.repeat_count > self.max_repeats:
                # Suppress - we've shown this enough
                return None
            elif self.repeat_count == self.max_repeats:
                # Show count and stop
                return f"... (message repeated {self.repeat_count} times, suppressing further repeats)"
        else:
            # New line, reset counter
            if self.repeat_count > self.max_repeats:
                # Show final count for previous suppressed line
                prev = self.last_line
                total = self.repeat_count
                self.last_line = line
                self.repeat_count = 1
                # Return both the summary and new line
                return f"... (previous message repeated {total} total times)\n{line}"
            
            self.last_line = line
            self.repeat_count = 1
        
        return line
    
    def format_for_display(self, line: str) -> str:
        """
        Format a line for display (add colors/emphasis if terminal supports it).
        
        Args:
            line: Line to format
            
        Returns:
            Formatted line
        """
        # Check for error/warning keywords and add markers
        line_lower = line.lower()
        
        if 'error' in line_lower or 'failed' in line_lower or 'fatal' in line_lower:
            return f"❌ {line}"
        elif 'warning' in line_lower or 'warn' in line_lower:
            return f"⚠️  {line}"
        elif 'success' in line_lower or 'complete' in line_lower:
            return f"✓ {line}"
        
        return line


# Convenience function for simple parsing
def parse_mars_log_line(
    line: str,
    show_all: bool = False,
    parser: Optional[MarsLogParser] = None
) -> Optional[str]:
    """
    Parse a single MARS log line.
    
    Args:
        line: The log line
        show_all: If True, show all lines
        parser: Optional parser instance to maintain state
        
    Returns:
        Processed line or None if should be suppressed
    """
    if parser is None:
        parser = MarsLogParser(show_all=show_all)
    
    processed = parser.process_line(line)
    if processed:
        return parser.format_for_display(processed)
    return None


def create_default_log_handler(filter_logs: bool = True) -> LogHandler:
    """
    Create the default MARS log handler using MarsLogParser.
    
    This is the default handler used when no custom log_handler is provided.
    It filters and formats MARS logs to reduce noise.
    
    Args:
        filter_logs: If True, filter and format logs. If False, show all logs.
        
    Returns:
        An async log handler function compatible with LogHandlerProtocol
        
    Example:
        # Use default filtering
        handler = create_default_log_handler(filter_logs=True)
        
        # Or in mars_via_ws:
        result = await mars_via_ws(servers, requests, environ, target,
                                    log_handler=create_default_log_handler())
    """
    parser = MarsLogParser(show_all=not filter_logs)
    
    async def handler(line: str, ws: Any, logger: Any) -> Optional[str]:
        """Default log handler using MarsLogParser."""
        if not filter_logs:
            return line
        
        display_line = parser.process_line(line)
        if display_line:
            return parser.format_for_display(display_line)
        return None
    
    return handler
