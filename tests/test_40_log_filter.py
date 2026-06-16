"""Unit tests for cads_mars_server.log_filter."""

import asyncio

from cads_mars_server.log_filter import MarsLogParser, create_default_log_handler


class TestMarsLogParser:
    def test_empty_line_hidden(self):
        p = MarsLogParser()
        assert not p.should_show("")
        assert not p.should_show("   ")

    def test_important_patterns_shown(self):
        p = MarsLogParser()
        assert p.should_show("ERROR: something went wrong")
        assert p.should_show("warning: disk full")
        assert p.should_show("FATAL crash")
        assert p.should_show("timeout reached")
        assert p.should_show("50%")

    def test_show_all_mode(self):
        p = MarsLogParser(show_all=True)
        assert p.should_show("some random debug line")
        assert p.should_show("DEBUG: irrelevant")

    def test_process_line_dedup(self):
        p = MarsLogParser()
        assert p.process_line("hello") == "hello"  # repeat_count=1
        assert p.process_line("hello") == "hello"  # repeat_count=2
        # Third call hits max_repeats=3 → suppression message
        result = p.process_line("hello")
        assert result is not None
        assert "repeated" in result
        # Fourth call → fully suppressed
        assert p.process_line("hello") is None

    def test_process_line_reset_on_new(self):
        p = MarsLogParser()
        p.process_line("line A")
        p.process_line("line A")
        result = p.process_line("line B")
        assert result == "line B"

    def test_format_for_display(self):
        p = MarsLogParser()
        assert p.format_for_display("some line") == "some line"

    def test_custom_important_patterns(self):
        p = MarsLogParser(important_patterns=[r"CUSTOM_PATTERN"])
        assert p.should_show("CUSTOM_PATTERN detected")

    def test_custom_ignore_patterns(self):
        p = MarsLogParser(ignore_patterns=[r"NOISY"])
        assert not p.should_show("NOISY debug output")


class TestCreateDefaultLogHandler:
    def test_filter_enabled(self):
        handler = create_default_log_handler(filter_logs=True)
        result = asyncio.run(handler("", None, None))
        assert result is None  # empty line filtered

    def test_filter_disabled(self):
        handler = create_default_log_handler(filter_logs=False)
        result = asyncio.run(handler("some line", None, None))
        assert result == "some line"

    def test_important_line_passes_filter(self):
        handler = create_default_log_handler(filter_logs=True)
        result = asyncio.run(handler("ERROR: something failed", None, None))
        assert result is not None
        assert "ERROR" in result
