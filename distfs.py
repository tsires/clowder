#!/usr/bin/env python
""" Distributed Filesystem: FUSE Implementation

Project for CSCI 6450

Usage: ./distfs <mountpoint> <host> <port>
"""

from __future__ import with_statement, division, print_function, absolute_import, unicode_literals

import logging
from sys import argv, exit
try:
    from time import monotonic
except ImportError:
    from time import time as monotonic
from functools import partial

from fuse import FUSE, FuseOSError, Operations, LoggingMixIn

import zmq

if not hasattr(__builtins__, 'bytes'):
    bytes = str

class DistFS(LoggingMixIn, Operations):
    'Remote filesystem. Passes all calls to a listening ZMQFS instance.'

    def __init__(self, host='localhost', port='1234'):
        context = zmq.Context()
        self.socket = context.socket(zmq.REQ)
        self.socket.connect('tcp://%s:%s' % (host, port))

    def _remote_call(self, op, *args):
        self.socket.send_pyobj([op, args])
        errno, ret = self.socket.recv_pyobj()
        if errno is not None:
            raise FuseOSError(errno)
        print('[%13.3f] %s: %r -> %r' % (monotonic(), op, args, ret))
        return ret

    def chmod(self, path, mode):
        return self._remote_call('chmod', path, mode)

    def chown(self, path, uid, gid):
        return self._remote_call('chown', path, uid, gid)

    def create(self, path, mode):
        return self._remote_call('create', path, mode)

    def getattr(self, path, fh=None):
        return self._remote_call('getattr', path, fh)

    def getxattr(self, path, name, position=0):
        return self._remote_call('getxattr', path, name, position)

    def listxattr(self, path):
        return self._remote_call('listxattr', path)

    def mkdir(self, path, mode):
        return self._remote_call('mkdir', path, mode)

    def open(self, path, flags):
        return self._remote_call('open', path, flags)

    def read(self, path, size, offset, fh):
        return self._remote_call('read', path, size, offset, fh)

    def readdir(self, path, fh):
        return self._remote_call('readdir', path, fh)

    def readlink(self, path):
        return self._remote_call('readlink', path)

    def removexattr(self, path, name):
        return self._remote_call('removexattr', path, name)

    def rename(self, old, new):
        return self._remote_call('rename', old, new)

    def rmdir(self, path):
        return self._remote_call('rmdir', path)

    def setxattr(self, path, name, value, options, position=0):
        return self._remote_call('setxattr', path, name, value, options, position)

    def statfs(self, path):
        return self._remote_call('statfs', path)

    def symlink(self, target, source):
        return self._remote_call('symlink', target, source)

    def truncate(self, path, length, fh=None):
        return self._remote_call('truncate', path, length, fh)

    def unlink(self, path):
        return self._remote_call('unlink', path)

    def utimens(self, path, times=None):
        return self._remote_call('utimens', path, times)

    def write(self, path, data, offset, fh):
        return self._remote_call('write', path, data, offset, fh)


if __name__ == '__main__':
    if len(argv) != 4:
        print('usage: %s <mountpoint> <host> <port>' % argv[0])
        exit(1)

    logging.getLogger().setLevel(logging.DEBUG)
    fuse = FUSE(DistFS(host=argv[2], port=argv[3]), argv[1], foreground=True)
