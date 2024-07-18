import subprocess


def test_cli_help():
    captured = subprocess.run(["cads-mars-server"], stdout=subprocess.PIPE)
    assert not captured.returncode
    assert captured.stdout
    assert not captured.stderr
