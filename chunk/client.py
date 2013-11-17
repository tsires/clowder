""" Distributed Filesystem: Chunkserver Client API

Project for CSCI 6450

"""

from __future__ import with_statement, division, print_function, absolute_import, unicode_literals

import posixpath
from .common import *

class ChunkClient(object):
    pass


class DummyChunkClient(ChunkClient):
    def __init__(self, cache_path):
        self.cache_path = cache_path
        self.chunks = {}

    def put(key, data):
        self.chunks[key] = data
        with open(posixpath.join(self.cache_path, key), mode='wb') as f:
            f.write(data)

    def get(key):
        try:
            return self.chunks[key]
        except KeyError:
            with open(posixpath.join(self.cache_path, key), mode='rb') as f:
                data = f.read()
            self.chunks[key] = data
            return data


# vim: sw=4 ts=4 expandtab
