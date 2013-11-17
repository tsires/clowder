#!/usr/bin/env python3
""" Distributed Filesystem: FUSE Implementation

Project for CSCI 6450

Usage: ./distfs.py <root_name> <mountpoint>
"""

from __future__ import with_statement, division, print_function, absolute_import, unicode_literals

import posixpath
from errno import EEXIST, ENOENT, ENOTEMPTY
from stat import S_IFDIR, S_IFLNK, S_IFREG

from time import time

try:
    from time import monotonic
except ImportError:
    from time import time as monotonic

import msgpack
from fuse import FUSE, FuseOSError, Operations, LoggingMixIn
from kazoo.client import KazooClient
from kazoo.exceptions import NodeExistsError, NoNodeError, NotEmptyError
from kazoo.protocol.states import EventType

from chunk.client import DummyChunkClient

class DistFS(LoggingMixIn, Operations):
    'Distributed filesystem. Queries Zookeeper for directory contents and metadata.'

    FILESYSTEMS = posixpath.join('/', 'fs', 'trees')

    def __init__(self, zk, chunk_client, fs_root):
        self._meta_cache = {}
        self._children_cache = {}
        self._cache_tries = 0
        self._cache_misses = 0
        self.zk = zk
        self.chunk_client = chunk_client
        self.fs_root = fs_root
        # Internal FD counter
        self.fd = 0

    def bootstrap(self):
        path = self._zk_path('/')
        now = time()
        meta = dict(attrs=dict(
                st_mode=(S_IFDIR | 0o755),
                st_nlink=2,
                st_size=0,
                st_ctime=now,
                st_mtime=now,
                st_atime=now,
                ))
        try:
            self.zk.create(path, msgpack.dumps(meta), makepath=True)
        except NodeExistsError as e:
            pass

    def _op_stub(self, op, *args):
        print('[%13.3f] [STUB] %s: %r' % (monotonic(), op, args))

    def _zk_path(self, path):
        return posixpath.join(self.FILESYSTEMS, self.fs_root, path.lstrip('/'))

    def _get_meta(self, path):
        self._cache_tries += 1
        try:
            return self._meta_cache[path]
        except KeyError:
            self._cache_misses += 1
            meta = msgpack.loads(self.zk.get(path, watch=self._cache_expire)[0], encoding='utf-8')
            self._meta_cache[path] = meta
            return meta

    def _get_children(self, path):
        self._cache_tries += 1
        try:
            return self._children_cache[path]
        except KeyError:
            self._cache_misses += 1
            children = self.zk.get_children(path, watch=self._cache_expire)
            self._children_cache[path] = children
            return children

    def _cache_expire(self, event):
        if event.type == EventType.CHILD:
            del self._children_cache[event.path]
        else:
            del self._meta_cache[event.path]

    def chmod(self, path, mode):
        path = self._zk_path(path)
        return self._op_stub('chmod', path, mode)

    def chown(self, path, uid, gid):
        path = self._zk_path(path)
        return self._op_stub('chown', path, uid, gid)

    def create(self, path, mode):
        path = self._zk_path(path)
        now = time()
        meta = dict(data=[],attrs=dict(
                st_mode=(S_IFREG | mode),
                st_nlink=1,
                st_size=0,
                st_ctime=now,
                st_mtime=now,
                st_atime=now,
                ))
        try:
            self.zk.create(path, msgpack.dumps(meta))
        except NodeExistsError as e:
            raise FuseOSError(EEXIST) from e
        except NoNodeError as e:
            raise FuseOSError(ENOENT) from e
        self.fd += 1
        return self.fd

    def getattr(self, path, fh=None):
        path = self._zk_path(path)
        try:
            meta = self._get_meta(path)
        except NoNodeError as e:
            raise FuseOSError(ENOENT) from e
        return meta['attrs']

    def mkdir(self, path, mode):
        path = self._zk_path(path)
        parent = posixpath.dirname(posixpath.normpath(path))
        now = time()
        meta = dict(attrs=dict(
                st_mode=(S_IFDIR | mode),
                st_nlink=2,
                st_size=0,
                st_ctime=now,
                st_mtime=now,
                st_atime=now,
                ))
        try:
            parent_meta = self._get_meta(parent)
            parent_meta['attrs']['st_nlink'] += 1
            trans = self.zk.transaction()
            trans.set_data(parent, msgpack.dumps(parent_meta))
            trans.create(path, msgpack.dumps(meta))
            trans.commit()
        except NodeExistsError as e:
            raise FuseOSError(EEXIST) from e
        except NoNodeError as e:
            raise FuseOSError(ENOENT) from e

    def open(self, path, flags):
        self.fd += 1
        return self.fd

    def read(self, path, size, offset, fh):
        path = self._zk_path(path)
        try:
            self._get_meta(path)
        except NoNodeError as e:
            raise FuseOSError(ENOENT) from e
        # FIXME: replace stub with actual implementation
        return b''

    def readdir(self, path, fh):
        path = self._zk_path(path)
        try:
            files = self._get_children(path)
        except NoNodeError as e:
            raise FuseOSError(ENOENT) from e
        return ['.', '..'] + files

    def readlink(self, path):
        path = self._zk_path(path)
        try:
            self._get_meta(path)
        except NoNodeError as e:
            raise FuseOSError(ENOENT) from e
        # FIXME: replace stub with actual implementation
        return b''

    def rename(self, oldpath, newpath):
        oldpath = self._zk_path(oldpath)
        newpath = self._zk_path(newpath)
        try:
            # FIXME: This will not relocate an entire directory tree
            meta = self._get_meta(path)
            trans = self.zk.transaction()
            trans.create(newpath, msgpack.dumps(meta))
            trans.delete(oldpath)
            trans.commit()
        except NodeExistsError as e:
            raise FuseOSError(EEXIST) from e
        except NoNodeError as e:
            raise FuseOSError(ENOENT) from e
        except NotEmptyError as e:
            raise FuseOSError(ENOTEMPTY) from e

    def rmdir(self, path):
        path = self._zk_path(path)
        parent = posixpath.dirname(path)
        try:
            parent_meta = self._get_meta(parent)
            parent_meta['attrs']['st_nlink'] -= 1
            trans = self.zk.transaction()
            trans.set_data(parent, msgpack.dumps(parent_meta))
            trans.delete(path)
            trans.commit()
        except NoNodeError as e:
            raise FuseOSError(ENOENT) from e
        except NotEmptyError as e:
            raise FuseOSError(ENOTEMPTY) from e

    def statfs(self, path):
        # TODO: get this fs metadata from Zookeeper too?
        return dict(f_bsize=64*1024, f_blocks=4096, f_bavail=2048)

    def symlink(self, oldpath, newpath):
        newpath = self._zk_path(newpath)
        now = time()
        meta = dict(target=oldpath,attrs=dict(
                st_mode=(S_IFLNK | 0o777),
                st_nlink=1,
                st_size=0,
                ))
        try:
            self.zk.create(path, msgpack.dumps(meta))
        except NodeExistsError as e:
            raise FuseOSError(EEXIST) from e
        except NoNodeError as e:
            raise FuseOSError(ENOENT) from e
        # FIXME: replace stub with actual implementation

    def truncate(self, path, length, fh=None):
        path = self._zk_path(path)
        try:
            self._get_meta(path)
        except NoNodeError as e:
            raise FuseOSError(ENOENT) from e
        # FIXME: replace stub with actual implementation

    def unlink(self, path):
        path = self._zk_path(path)
        try:
            self.zk.delete(path)
        except NoNodeError as e:
            raise FuseOSError(ENOENT) from e
        except NotEmptyError as e:
            raise FuseOSError(ENOTEMPTY) from e

    def utimens(self, path, times=None):
        path = self._zk_path(path)
        now = time()
        atime, mtime = times if times else (now, now)
        try:
            meta = self._get_meta(path)
            meta['attrs'].update(st_atime=atime, st_mtime=mtime)
            self.zk.set(path, msgpack.dumps(meta))
        except NoNodeError as e:
            raise FuseOSError(ENOENT) from e

    def write(self, path, data, offset, fh):
        path = self._zk_path(path)
        try:
            self._get_meta(path)
        except NoNodeError as e:
            raise FuseOSError(ENOENT) from e
        # FIXME: replace stub with actual implementation
        return len(data)


def main(argv):
    if len(argv) != 3:
        print('usage: %s <root_name> <mountpoint>' % argv[0])
        return 1

    # Zookeeper
    zk_hosts = [('127.0.0.1', '2181')]
    hosts = ','.join(':'.join(host) for host in zk_hosts) 
    zk = KazooClient(hosts=hosts)
    zk.start()

    # ChunkClient
    cc = DummyChunkClient('/tmp/chunkcache')

    # DistFS
    distfs = DistFS(zk=zk, chunk_client=cc, fs_root=argv[1])
    distfs.bootstrap()

    fuse = FUSE(distfs, argv[2], foreground=True, nothreads=True)
    zk.stop()
    print("Cache hits: %d; misses: %d" % (distfs._cache_tries - distfs._cache_misses, distfs._cache_misses))
    return 0

if __name__ == '__main__':
    from sys import argv, exit
    import logging
    logging.basicConfig(level=logging.DEBUG)
    exit(main(argv))

# vim: sw=4 ts=4 expandtab
