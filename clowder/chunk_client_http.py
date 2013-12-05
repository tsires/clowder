""" Distributed Filesystem: HTTP ChunkClient Implementation

Project for CSCI 6450

"""

from __future__ import with_statement, division, print_function, absolute_import, unicode_literals

import logging

import requests

from .common import *
from .exception import *
from .chunk_client import LocalChunkClient

class HTTPChunkClient(LocalChunkClient, Selector):
    'A ChunkClient that fetches missing chunks via HTTP.'
    __log = logging.getLogger('distfs.http_chunk_client')

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.session = requests.Session()

    def put(self, data, key=None):
        """ Store chunk data, returning its key.
        
        If key is not given, one will be generated according to the selected scheme.

        """
        key = super().put(data, key)

        try:
            host, port = self.get_random_peer(key=None)[0]
            r = self.session.post(url='http://%s:%s/%s' % (host, port, key), files={key: data})
            # TODO: Check return code
        except IndexError:
            pass

        return key

    def get(self, key):
        """ Get the chunk data for the given numeric key. """
        try:
            return super().get(key)
        except ChunkNotFoundError:
            try:
                host, port = self.get_random_peer(key=None)[0]
                r = self.session.get(url='http://%s:%s/%s' % (host, port, key))
                data = r.content
                super().put(data, key)
                return data
            except IndexError as e:
                raise ChunkNotFoundError(key) from e



# vim: sw=4 ts=4 expandtab
