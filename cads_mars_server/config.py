import yaml
import os

DEFAULT_CONFIG_FILE = '/etc/cads-mars-server.yaml'
MARS_CONFIG_FILE = os.getenv('MARS_CONFIG_FILE', DEFAULT_CONFIG_FILE)
DEFAULT_CONFIG = dict(
    CACHE_ROOT='/',
    SHARES=['download-dev-0001', 'download-dev-0002'],
    MEMCACHED=['mars-worker-dev-1000:11211', 'mars-worker-dev-1000:11211'],
    CACHE_FOLDER='mars'
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
        target = cache_object['target']
        _, _file = tuple(target.split('/mars/'))
        _cache_root, share = _.split('/')[1:]
        print(_cache_root, share)
        _c = get_config()
        return target.replace(f'/{_cache_root}', f"{_c['CACHE_ROOT']}")