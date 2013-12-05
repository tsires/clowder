__all__ = ['FILESYSTEMS', 'CHUNKSERVERS', 'CHUNKBINS', 'Cache', 'Selector']

import logging
import posixpath
import random

import msgpack

# Zookeeper Paths
FILESYSTEMS = posixpath.join('/', 'fs', 'trees')
CHUNK = posixpath.join('/', 'chunk')
CHUNKSERVERS = posixpath.join(CHUNK, 'servers/')
CHUNKBINS = posixpath.join(CHUNK, 'bins')

class Cache(object):
    __log = logging.getLogger('distfs.cache')
    def __init__(self, get):
        self._get = get
        self._cache = {}
        self._tries = 0
        self._misses = 0

    def get(self, path):
        path = path.rstrip('/')
        self._tries += 1
        try:
            return self._cache[path]
        except KeyError:
            self.__log.debug('Cache miss: %s', path)
            self._misses += 1
            o = self._get(path, self._expire)
            self._cache[path] = o
            return o

    def stats(self):
        return (self._tries - self._misses, self._misses)

    def _expire(self, event):
        path = event.path
        self.__log.debug('Cache expire: %s', path)
        del self._cache[path]


class Selector(object):
    'Allows selection of the proper ChunkServer to service a request.'
    __log = logging.getLogger('distfs.selector')
    def __init__(self, zk, **kwargs):
        super().__init__(**kwargs)
        self.zk = zk
        # Caches
        self._peer_cache = Cache(get=lambda p, w: msgpack.loads(zk.get(p, w)[0], encoding='utf-8'))
        self._children_cache = Cache(get=zk.get_children)
        self._get_peer = lambda p: self._peer_cache.get(posixpath.join(CHUNKSERVERS, p))
        self._get_children = self._children_cache.get
        self._get_bin = lambda b: self._children_cache.get(posixpath.join(CHUNKBINS, b))

    def get_random_peer(self, key=None, exclude=None):
        if key is None:
            candidates = self._get_children(CHUNKSERVERS)
        else:
            candidates = self._get_bin(key[:2])
        try:
            candidates.remove(exclude)
        except ValueError:
            pass
        if len(candidates):
            return [self._get_peer(random.choice(candidates))]
        else:
            return []

    def get_all_peers(self, key=None, exclude=None):
        if key is None:
            candidates = self._get_children(CHUNKSERVERS)
        else:
            candidates = self._get_bin(key[:2])
        try:
            candidates.remove(exclude)
        except ValueError:
            pass
        return [self._get_peer(p) for p in candidates]


# vim: sw=4 ts=4 expandtab
