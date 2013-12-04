__all__ = ['FILESYSTEMS', 'CHUNKSERVERS', 'CHUNKBINS', 'Cache']

import posixpath

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


# vim: sw=4 ts=4 expandtab
