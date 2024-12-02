import glob
from .config import get_config
import json
from pymemcache.client.hash import HashClient
import hashlib

VALID_STATUS = ['PENDING', 'RUNNING', 'COMPLETED', 'FAILED', 'CANCELLED']


def request_hash(request):
    _rq = request.copy()
    if 'target' in _rq:
        _rq.pop('target')
    return hashlib.md5(json.dumps(_rq, sort_keys=True).encode('utf-8')).hexdigest()


class WorkerCache:
    def __init__(self, client=None):
        self.config = get_config()
        self.client = client if client else HashClient(self.config['MEMCACHED'])

    def get(self, key):
        value = self.client.get(key)
        if value is not None:
            return json.loads(value)
        return None

    def set(self, key, value):
        return self.client.set(key, json.dumps(value))

    def delete(self, key):
        return self.client.delete(key)

    def delete_all(self):
        return self.client.delete_all()