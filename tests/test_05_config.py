"""Tests for config loading (cads_mars_server.config).

The config module is evaluated at import time, so each scenario runs in a
fresh subprocess with a controlled MARS_CONFIG_FILE / environment.
"""

import os
import subprocess
import sys
import textwrap

SNIPPET_PRINT = textwrap.dedent(
    """
    from cads_mars_server import config
    print("SHARES", config.SHARES)
    print("SHARED_ROOT", config.SHARED_ROOT)
    print("CACHE_FOLDER", config.CACHE_FOLDER)
    print("USE_SHARES", config.USE_SHARES)
    print("PIPE_PORT", config.DEFAULT_PIPE_PORT)
    """
)


def _run(snippet, config_text=None, tmp_path=None, extra_env=None, config_file=None):
    """Run *snippet* in a subprocess with a controlled config environment."""
    env = {k: v for k, v in os.environ.items() if not k.startswith("MARS_")}
    if config_text is not None:
        config_file = str(tmp_path / "config.yaml")
        with open(config_file, "w") as f:
            f.write(config_text)
    if config_file is not None:
        env["MARS_CONFIG_FILE"] = config_file
    if extra_env:
        env.update(extra_env)
    return subprocess.run(
        [sys.executable, "-c", snippet],
        env=env,
        capture_output=True,
        text=True,
    )


class TestYamlLoading:
    def test_lowercase_keys(self, tmp_path):
        result = _run(
            SNIPPET_PRINT,
            config_text="shared_root: /data\nshares:\n  - vol1\n  - vol2\ncache_folder: mars\n",
            tmp_path=tmp_path,
        )
        assert result.returncode == 0, result.stderr
        assert "SHARES ['vol1', 'vol2']" in result.stdout
        assert "SHARED_ROOT /data" in result.stdout

    def test_legacy_uppercase_keys(self, tmp_path):
        """Old cds-ansible templates wrote CACHE_ROOT / SHARES / CACHE_FOLDER."""
        result = _run(
            SNIPPET_PRINT,
            config_text="CACHE_ROOT: /data\nSHARES:\n  - dl1\nCACHE_FOLDER: marsx\n",
            tmp_path=tmp_path,
        )
        assert result.returncode == 0, result.stderr
        assert "SHARES ['dl1']" in result.stdout
        assert "SHARED_ROOT /data" in result.stdout
        assert "CACHE_FOLDER marsx" in result.stdout

    def test_port_from_file(self, tmp_path):
        result = _run(SNIPPET_PRINT, config_text="pipe_port: 9100\n", tmp_path=tmp_path)
        assert result.returncode == 0, result.stderr
        assert "PIPE_PORT 9100" in result.stdout

    def test_env_overrides_file(self, tmp_path):
        result = _run(
            SNIPPET_PRINT,
            config_text="shares:\n  - fromfile\n",
            tmp_path=tmp_path,
            extra_env={"MARS_SHARES": "fromenv1,fromenv2"},
        )
        assert result.returncode == 0, result.stderr
        assert "SHARES ['fromenv1', 'fromenv2']" in result.stdout

    def test_empty_file_uses_defaults(self, tmp_path):
        result = _run(SNIPPET_PRINT, config_text="", tmp_path=tmp_path)
        assert result.returncode == 0, result.stderr
        assert "SHARES []" in result.stdout
        assert "SHARED_ROOT /cache" in result.stdout
        assert "CACHE_FOLDER mars" in result.stdout


class TestBooleans:
    def test_yaml_bool_false(self, tmp_path):
        result = _run(
            SNIPPET_PRINT, config_text="use_shares: false\n", tmp_path=tmp_path
        )
        assert "USE_SHARES False" in result.stdout

    def test_string_false_is_false(self, tmp_path):
        result = _run(
            SNIPPET_PRINT, config_text='use_shares: "false"\n', tmp_path=tmp_path
        )
        assert "USE_SHARES False" in result.stdout

    def test_string_true_is_true(self, tmp_path):
        result = _run(
            SNIPPET_PRINT, config_text='use_shares: "yes"\n', tmp_path=tmp_path
        )
        assert "USE_SHARES True" in result.stdout


class TestExplicitConfigStrictness:
    def test_explicit_missing_file_fails(self, tmp_path):
        result = _run(SNIPPET_PRINT, config_file=str(tmp_path / "does-not-exist.yaml"))
        assert result.returncode != 0
        assert "ConfigError" in result.stderr
        assert "not found" in result.stderr

    def test_explicit_invalid_yaml_fails(self, tmp_path):
        result = _run(
            SNIPPET_PRINT,
            config_text="shares: [unclosed\n  broken :: yaml\n",
            tmp_path=tmp_path,
        )
        assert result.returncode != 0
        assert "ConfigError" in result.stderr

    def test_explicit_non_mapping_fails(self, tmp_path):
        result = _run(
            SNIPPET_PRINT, config_text="- just\n- a\n- list\n", tmp_path=tmp_path
        )
        assert result.returncode != 0
        assert "ConfigError" in result.stderr

    def test_implicit_missing_file_is_fine(self):
        # No MARS_CONFIG_FILE set; /etc/cads-mars-server.yaml may or may not
        # exist on the machine running the tests, so just assert import works.
        result = _run("import cads_mars_server.config")
        assert result.returncode == 0, result.stderr


class TestRetrieveSizeConfig:
    SNIPPET = textwrap.dedent(
        """
        from cads_mars_server import config
        print("MAX_RETRIEVE_SIZE", config.MAX_RETRIEVE_SIZE)
        print("MAX_RETRIEVE_SIZE_FLOOR", config.MAX_RETRIEVE_SIZE_FLOOR)
        """
    )

    def test_defaults(self, tmp_path):
        result = _run(self.SNIPPET, config_text="", tmp_path=tmp_path)
        assert result.returncode == 0, result.stderr
        assert "MAX_RETRIEVE_SIZE 161061273600" in result.stdout
        assert "MAX_RETRIEVE_SIZE_FLOOR 1073741824" in result.stdout

    def test_from_file(self, tmp_path):
        result = _run(
            self.SNIPPET,
            config_text="max_retrieve_size: 1000\nmax_retrieve_size_floor: 10\n",
            tmp_path=tmp_path,
        )
        assert result.returncode == 0, result.stderr
        assert "MAX_RETRIEVE_SIZE 1000" in result.stdout
        assert "MAX_RETRIEVE_SIZE_FLOOR 10" in result.stdout

    def test_env_overrides_file(self, tmp_path):
        result = _run(
            self.SNIPPET,
            config_text="max_retrieve_size: 1000\n",
            tmp_path=tmp_path,
            extra_env={"MARS_MAX_RETRIEVE_SIZE": "2000"},
        )
        assert result.returncode == 0, result.stderr
        assert "MAX_RETRIEVE_SIZE 2000" in result.stdout


class TestStreamServerFailFast:
    SNIPPET_SETUP = textwrap.dedent(
        """
        from cads_mars_server import server_cache_and_stream
        server = server_cache_and_stream.setup_server(
            "/usr/local/bin/mars", "127.0.0.1", 0, logdir="{logdir}"
        )
        print("STARTED with shares:", server.RequestHandlerClass.shares)
        """
    )

    def test_no_shares_with_explicit_config_fails(self, tmp_path):
        logdir = tmp_path / "logs"
        result = _run(
            self.SNIPPET_SETUP.format(logdir=logdir),
            config_text="shared_root: /cache\n",  # no shares key
            tmp_path=tmp_path,
        )
        assert result.returncode != 0
        assert "refusing to start" in result.stderr

    def test_shares_from_explicit_config_starts(self, tmp_path):
        logdir = tmp_path / "logs"
        result = _run(
            self.SNIPPET_SETUP.format(logdir=logdir),
            config_text="shared_root: /cache\nshares:\n  - vol1\n",
            tmp_path=tmp_path,
        )
        assert result.returncode == 0, result.stderr
        assert "STARTED with shares: ['vol1']" in result.stdout
