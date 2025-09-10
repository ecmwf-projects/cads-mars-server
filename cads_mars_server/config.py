import yaml
import os
import random

DEFAULT_CONFIG_FILE = '/etc/cads-mars-server.yaml'
MARS_CONFIG_FILE = os.getenv('MARS_CONFIG_FILE', DEFAULT_CONFIG_FILE)
DEFAULT_CONFIG = dict(
    CLUSTER='cci1',
    CACHE_ROOT='/',
    SHARES=['download-dev-0001', 'download-dev-0002'],
    MEMCACHED=[''],
    CACHE_FOLDER='mars',
    DOWNLOAD_SERVERS=[
        "https://dss-download-cci1.copernicus-climate.eu",
        "https://dss-download-cci2.copernicus-climate.eu"]
)


def get_config():
    if os.path.exists(MARS_CONFIG_FILE):
        with open(MARS_CONFIG_FILE, 'r') as _f:
            config = yaml.safe_load(_f)
    else:
        config = DEFAULT_CONFIG
    return config

def local_target(cache_object: dict) -> str:
    if cache_object.get('target'):
        _c = get_config()
        target = cache_object['target']
        _, _file = tuple(target.split(f'/{_c.get("CACHE_FOLDER","mars")}/'))
        _cache_root, share = _.split('/')[1:]
        out = target.replace(f'/{_cache_root}/', f"{_c['CACHE_ROOT']}")
        if os.path.exists(out):
            return out
        else:
            if 'DOWNLOAD_SERVERS' in cache_object and cache_object['DOWNLOAD_SERVERS']:
                return f'{random.sample(cache_object["DOWNLOAD_SERVERS"], 1)[0]}/{share}/{_c.get("CACHE_FOLDER","mars")}/{_file}'
            return f'{random.sample(_c["DOWNLOAD_SERVERS"], 1)[0]}/{share}/{_c.get("CACHE_FOLDER","mars")}/{_file}'
