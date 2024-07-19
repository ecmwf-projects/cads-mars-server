import cads_mars_server


def test_version() -> None:
    assert cads_mars_server.__version__ != "999"
