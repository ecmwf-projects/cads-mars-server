import glob
import hashlib
import json
import os
import random

from pymemcache.client.hash import HashClient

from .config import get_config

VALID_STATUS = ["ACCEPTED", "RUNNING", "SUCCESSFUL", "FAILED", "DELETED"]


def request_hash(request):
    _rq = request.copy()
    if "target" in _rq:
        _rq.pop("target")
    return hashlib.md5(json.dumps(_rq, sort_keys=True).encode("utf-8")).hexdigest()


class WorkerCache:
    def __init__(self, client=None):
        self.config = get_config()
        self.client = client if client else HashClient(self.config["MEMCACHED"])

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

    def new_file(self, request):
        rq_hash = request_hash(request)
        share = random.choice(self.config["SHARES"])
        out_file = os.path.join(
            self.config["CACHE_ROOT"],
            share,
            self.config["CACHE_FOLDER"],
            f"{rq_hash}.grib",
        )
        return out_file


class CacheMaintainer:
    def __init__(self, cache: WorkerCache):
        self.cache = cache
        self.config = get_config()
        self.cache_root = self.config["CACHE_ROOT"]
        self.cache_folder = self.config["CACHE_FOLDER"]
        self.shares = self.config["SHARES"]
        self.cache_folders = [
            os.path.join(self.cache_root, share, self.cache_folder)
            for share in self.shares
        ]

    def clean(self):
        # clean all files that are not tracked by the WorkerCache
        files_to_delete = []
        for folder in self.cache_folders:
            files_to_delete += glob.glob(os.path.join(folder, "*.grib"))
        print(f"We found {len(files_to_delete)} files to be checked")
        self.hashes = {os.path.basename(f).split(".")[0]: f for f in files_to_delete}
        for hash in self.hashes:
            rq = self.cache.get(hash)
            if rq is None:
                print(
                    f"removing {self.hashes[hash]} becuse is not tracked by the cache"
                )
                os.remove(self.hashes[hash])
                continue

    def populate(self):
        # populate the WorkerCache with all files in the cache folders
        for folder in self.cache_folders:
            for f in glob.glob(os.path.join(folder, "*.grib")):
                hash = os.path.basename(f).split(".")[0]
                size = os.stat(f).st_size
                rq = dict(status="COMPLETED", size=size, target=f)
                if rq is None:
                    os.remove(f)
                    continue
                self.cache.set(hash, rq)
