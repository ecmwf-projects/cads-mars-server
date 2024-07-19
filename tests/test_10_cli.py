import subprocess


def test_cli_help():
    captured = subprocess.run(["cads-mars-server", "--help"], stdout=subprocess.PIPE)
    assert not captured.returncode
    assert captured.stdout.decode().startswith("Usage: cads-mars-server")
    assert not captured.stderr
