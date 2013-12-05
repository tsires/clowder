""" Distributed Filesystem: HTTP ChunkServer Implementation

Project for CSCI 6450

"""

from __future__ import with_statement, division, print_function, absolute_import, unicode_literals

import logging
import posixpath

import flask
import msgpack
import requests

from .common import *
from .exception import *
from .chunk_client import LocalChunkClient

class HTTPChunkServer(LocalChunkClient, Selector):
    'A ChunkServer that serves chunks via HTTP.'
    __log = logging.getLogger('distfs.http_chunk_server')

    def __init__(self, addr, **kwargs):
        super().__init__(**kwargs)
        self.addr = addr
        self.app = flask.Flask('clowder')
        self.app.add_url_rule('/<key>', view_func=self.handle_get, methods=['GET'])
        self.app.add_url_rule('/<key>', view_func=self.handle_put, methods=['PUT'])
        self.app.add_url_rule('/<key>', view_func=self.handle_post, methods=['POST'])
        self.session = requests.Session()
        self.bins = set()
        self.id = None

    def run(self):
        self.id = posixpath.basename(self.zk.create(CHUNKSERVERS, msgpack.dumps(self.addr), ephemeral=True, makepath=True, sequence=True))
        host, port = self.addr
        self.app.run(host=host, port=port)

    def handle_get(self, key):
        """ Get the chunk data for the given key. """
        try:
            return self.get(key)
        except ChunkNotFoundError:
            flask.abort(404)

    def handle_put(self, key):
        """ Store chunk data, returning its key.
        
        If key is not given, one will be generated according to the selected scheme.

        """
        data = flask.request.files[key].read()
        return self.put(data, key)

    def handle_post(self, key):
        """ Store chunk data, returning its key.
        
        If key is not given, one will be generated according to the selected scheme.

        """
        data = flask.request.files[key].read()
        key = self.put(data, key)

        for host, port in self.get_all_peers(key=None, exclude=self.id):
            r = self.session.put(url='http://%s:%s/%s' % (host, port, key), files={key: data})
        # TODO: Check return code

        return key


# vim: sw=4 ts=4 expandtab
