import yaml
import os

DEFAULT_CONFIG_FILE = '/etc/cads-mars-server.yaml'

DEFAULT_CONFIG = dict(
    CACHE_ROOT='/cache',
    SHARES=['download-dev-0001', 'download-dev-0002'],
    MEMCACHED=['localhost:11211'],
    MARS_CACHE_FOLDER='mars'
)

def get_config():
    if os.path.exists(DEFAULT_CONFIG_FILE):
        with open(DEFAULT_CONFIG_FILE, 'r') as _f:
            config = yaml.safe_load(DEFAULT_CONFIG_FILE)
    else:
        config = DEFAULT_CONFIG
    return config

