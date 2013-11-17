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

class DistFS(LoggingMixIn, Operations):
    'Distributed filesystem. Queries Zookeeper for directory contents and metadata.'

    FILESYSTEMS = posixpath.join('/', 'fs', 'trees')

    def __init__(self, fs_root, zk_hosts=None):
        self._meta_cache = {}
        self._children_cache = {}
        self._cache_tries = 0
        self._cache_misses = 0
        if zk_hosts is None:
            zk_hosts = [('127.0.0.1', '2181')]
        hosts = ','.join(':'.join(host) for host in zk_hosts) 
        self.zk = KazooClient(hosts=hosts)
        self.fs_root = fs_root
        # Internal FD counter
        self.fd = 0
        # Startup
        self.zk.start()
        self.bootstrap()

    def bootstrap(self):
        path = self._zk_path('/')
        now = time()
        meta = dict(
                st_mode=(S_IFDIR | 0o755),
                st_nlink=2,
                st_size=0,
                st_ctime=now,
                st_mtime=now,
                st_atime=now,
                )
        try:
            self.zk.create(path, msgpack.dumps(meta), makepath=True)
        except NodeExistsError as e:
            pass

    def _op_stub(self, op, *args):
        print('[%13.3f] [STUB] %s: %r' % (monotonic(), op, args))

    def _zk_path(self, path):
        return posixpath.normpath(posixpath.join(self.FILESYSTEMS, self.fs_root, posixpath.relpath(path, '/')))

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
        meta = dict(
                st_mode=(S_IFREG | mode),
                st_nlink=1,
                st_size=0,
                st_ctime=now,
                st_mtime=now,
                st_atime=now,
                )
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
        return meta

    def getxattr(self, path, name, position=0):
        attrs = self.getattr(path).get('attrs', {})
        try:
            return attrs[name]
        except KeyError:
            # FIXME: Should return ENOATTR?
            return b''

        return self._op_stub('getxattr', path, name, position)

    def listxattr(self, path):
        attrs = self.getattr(path).get('attrs', {})
        return attrs.keys()

    def mkdir(self, path, mode):
        path = self._zk_path(path)
        parent = posixpath.dirname(posixpath.normpath(path))
        now = time()
        meta = dict(
                st_mode=(S_IFDIR | mode),
                st_nlink=2,
                st_size=0,
                st_ctime=now,
                st_mtime=now,
                st_atime=now,
                )
        try:
            parent_meta = self._get_meta(parent)
            parent_meta['st_nlink'] += 1
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

    def removexattr(self, path, name):
        path = self._zk_path(path)
        try:
            meta = self._get_meta(path)
            attrs = meta.get('attrs', {})
            try:
                del attrs[name]
            except KeyError:
                # FIXME: Should return ENOATTR?
                pass
            self.zk.set(path, msgpack.dumps(meta))
        except NoNodeError as e:
            raise FuseOSError(ENOENT) from e

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
            parent_meta['st_nlink'] -= 1
            trans = self.zk.transaction()
            trans.set_data(parent, msgpack.dumps(parent_meta))
            trans.delete(path)
            trans.commit()
        except NoNodeError as e:
            raise FuseOSError(ENOENT) from e
        except NotEmptyError as e:
            raise FuseOSError(ENOTEMPTY) from e

    def setxattr(self, path, name, value, options, position=0):
        path = self._zk_path(path)
        try:
            meta = self._get_meta(path)
            attrs = meta.setdefault('attrs', {})
            attrs[name] = value
            self.zk.set(path, msgpack.dumps(meta))
        except NoNodeError as e:
            raise FuseOSError(ENOENT) from e

    def statfs(self, path):
        # TODO: get this fs metadata from Zookeeper too?
        return dict(f_bsize=64*1024, f_blocks=4096, f_bavail=2048)

    def symlink(self, oldpath, newpath):
        newpath = self._zk_path(newpath)
        now = time()
        meta = dict(
                st_mode=(S_IFLNK | 0o777),
                st_nlink=1,
                st_size=0,
                )
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
            meta.update(st_atime=atime, st_mtime=mtime)
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


if __name__ == '__main__':
    from sys import argv, exit
    import logging
    if len(argv) != 3:
        print('usage: %s <root_name> <mountpoint>' % argv[0])
        exit(1)

    logging.basicConfig(level=logging.DEBUG)
    distfs = DistFS(fs_root=argv[1])
    fuse = FUSE(distfs, argv[2], foreground=True)
    print("Cache hits: %d; misses: %d" % (distfs._cache_tries - distfs._cache_misses, distfs._cache_misses))


# vim: sw=4 ts=4 expandtab
