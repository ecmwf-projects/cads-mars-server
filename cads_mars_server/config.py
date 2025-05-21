import yaml
import os
import random

DEFAULT_CONFIG_FILE = '/etc/cads-mars-server.yaml'
MARS_CONFIG_FILE = os.getenv('MARS_CONFIG_FILE', DEFAULT_CONFIG_FILE)
DEFAULT_CONFIG = dict(
    CACHE_ROOT='/',
    SHARES=['download-dev-0001', 'download-dev-0002'],
    MEMCACHED=['mars-worker-dev-1000:11211', 'mars-worker-dev-1000:11211',
               'mars-worker-dev-2000:11211', 'mars-worker-dev-2001:11211'],
    CACHE_FOLDER='mars',
    DOWNLOAD_SERVERS=[
        "https://download-cci1-0000.copernicus-climate.eu",
        "https://download-cci1-0001.copernicus-climate.eu",
        "https://download-cci1-0002.copernicus-climate.eu",
        "https://download-cci1-0003.copernicus-climate.eu",
        "https://download-cci1-0004.copernicus-climate.eu",
        "https://download-cci1-0005.copernicus-climate.eu",
        "https://download-cci1-0007.copernicus-climate.eu",
        "https://download-cci1-0008.copernicus-climate.eu",
        "https://download-cci1-0009.copernicus-climate.eu",
        "https://download-cci2-0000.copernicus-climate.eu",
        "https://download-cci2-0001.copernicus-climate.eu",
        "https://download-cci2-0002.copernicus-climate.eu",
        "https://download-cci2-0003.copernicus-climate.eu",
        "https://download-cci2-0004.copernicus-climate.eu",
        "https://download-cci2-0005.copernicus-climate.eu",
        "https://download-cci2-0007.copernicus-climate.eu",
        "https://download-cci2-0008.copernicus-climate.eu", 
        "https://download-cci2-0009.copernicus-climate.eu"]
)
CACHE_ROOT: '/'
SHARES:
- download-cci1-0000
- download-cci1-0001
- download-cci1-0002
- download-cci1-0003
- download-cci1-0004
- download-cci1-0005
- download-cci1-0007
- download-cci1-0008
- download-cci1-0009
MEMCACHED:
- mars-worker-dev-1000.shared.compute.cci1.ecmwf.int:11211
- mars-worker-dev-1001.shared.compute.cci1.ecmwf.int:11211
- mars-worker-dev-2000.shared.compute.cci2.ecmwf.int:11211
- mars-worker-dev-2001.shared.compute.cci2.ecmwf.int:11211
CACHE_FOLDER: mars
DOWNLOAD_SERVERS:


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
        _, _file = tuple(target.split(f'/{_c.get("cache_folder","mars")}/'))
        _cache_root, share = _.split('/')[1:]
        out = target.replace(f'/{_cache_root}/', f"{_c['CACHE_ROOT']}")
        if os.path.exists(out):
            return out
        else:
            return f'https://{random.sample(_c["DOWNLOAD_SERVERS"], 1)[0]}/{share}/{_file}'