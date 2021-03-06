#!/usr/bin/env python3
""" Distributed Filesystem: FUSE Implementation

Project for CSCI 6450

Usage: ./distfs.py <root_name> <mountpoint>
"""

from __future__ import with_statement, division, print_function, absolute_import, unicode_literals

import os
import logging
import posixpath
from functools import reduce
from errno import EEXIST, ENOENT, ENOTEMPTY, EPERM
from stat import S_IFDIR, S_IFLNK, S_IFREG, S_ISDIR, S_ISREG
from time import time
import argparse

import msgpack
from fuse import FuseOSError, Operations, LoggingMixIn
from kazoo.exceptions import NodeExistsError, NoNodeError, NotEmptyError, RuntimeInconsistency, RolledBackError
from kazoo.protocol.states import EventType

from .common import *
from .exception import *
from .chunk_client import LocalChunkClient

class File(dict):
    __log = logging.getLogger('distfs.cache')
    def __init__(self, data, znode=None):
        super().__init__(data)
        self._znode = znode
        self._dirty = False

    def dumps(self):
        return msgpack.dumps(self, encoding='utf-8')

    @classmethod
    def loads(cls, data):
        return cls(msgpack.loads(data[0], encoding='utf-8'), data[1])

    def __del__(self):
        if self._dirty:
            self.__log.warn('Wiping out modified data')


class BufferedWrite(object):
    def __init__(self, path, fh):
        self._path = path
        self._fh = fh
        self._writes = []
        self.count = 0

    def write(self, data, offset):
        if len(self._writes) and self._writes[-1][1] == offset:
            self._writes[-1][1]+=len(data)
            self._writes[-1][2].append(data)
        else:
            self._writes.append([offset, offset+len(data), [data]])
        self.count += len(data)
        return len(data)

    def flush(self, func):
        for w in self._writes:
            func(self._path, b''.join(w[2]), w[0], self._fh)
        self._writes = []
        self.count = 0
        return 0


class ClowderFS(LoggingMixIn, Operations):
    'Distributed filesystem. Queries Zookeeper for directory contents and metadata.'

    FILESYSTEMS = FILESYSTEMS
    CHUNK_SIZE = 64*1024
    __log = logging.getLogger('distfs')

    def __init__(self, zk, chunk_client, fs_root):
        self.zk = zk
        self.chunk_client = chunk_client
        self.fs_root = fs_root
        # Caches
        self._meta_cache = Cache(get=lambda p, w: File.loads(zk.get(p, w)))
        self._children_cache = Cache(get=zk.get_children)
        # FIXME: placeholders until finished refactoring
        self._get_meta = self._meta_cache.get
        self._get_children = self._children_cache.get
        # Internal FD counter
        self._open_files = {}
        self.fd = 0
        # UID and GID
        self.uid = os.getuid()
        self.gid = os.getgid()

    @classmethod
    def mkfs(cls, zk, fs_root, chunk_size):
        now = time()
        root_meta = File(dict(
            fs=dict(
                f_bsize=chunk_size,
                ),
            attrs=dict(
                st_mode=(S_IFDIR | 0o755),
                st_nlink=2,
                st_size=0,
                st_ctime=now,
                st_mtime=now,
                st_atime=now,
                )))
        try:
            zk.create(fs_root, root_meta.dumps(), makepath=True)
            cls.__log.info('Created root directory at %s', fs_root)
        except NodeExistsError as e:
            cls.__log.error('Filesystem already exists at %s', fs_root)
            raise FSAlreadyExistsError(fs_root) from e

    def destroy(self, path):
        self.__log.debug("Metadata cache hits: %d; misses: %d", *(self._meta_cache.stats()))
        self.__log.debug("Children cache hits: %d; misses: %d", *(self._children_cache.stats()))

    def _op_stub(self, op, *args):
        self.__log.debug('[STUB] %s: %r', op, args)

    def chmod(self, path, mode):
        try:
            meta = self._get_meta(path)
            meta['attrs'].update(st_mode=mode)
            meta._dirty = False
            self.zk.set(path, meta.dumps())
        except NoNodeError as e:
            raise FuseOSError(ENOENT) from e

    def chown(self, path, uid, gid):
        # FIXME: Should this be EACCESS?
        raise FuseOSError(EPERM)

    def create(self, path, mode):
        now = time()
        new_meta = File(dict(chunks=[],attrs=dict(
                st_mode=(S_IFREG | mode),
                st_nlink=1,
                st_size=0,
                st_ctime=now,
                st_mtime=now,
                st_atime=now,
                )))
        try:
            self.zk.create(path, new_meta.dumps())
        except NodeExistsError as e:
            raise FuseOSError(EEXIST) from e
        except NoNodeError as e:
            raise FuseOSError(ENOENT) from e
        self.fd += 1
        self._open_files[self.fd] = BufferedWrite(path, self.fd)
        return self.fd

    def getattr(self, path, fh=None):
        # TODO: get some info from the znode too
        try:
            return dict(self._get_meta(path)['attrs'], st_uid=self.uid, st_gid=self.gid)
        except NoNodeError as e:
            raise FuseOSError(ENOENT) from e

    def mkdir(self, path, mode):
        parent = posixpath.dirname(path)
        now = time()
        new_meta = File(dict(attrs=dict(
                st_mode=(S_IFDIR | mode),
                st_nlink=2,
                st_size=0,
                st_ctime=now,
                st_mtime=now,
                st_atime=now,
                )))
        try:
            parent_meta = self._get_meta(parent)
            parent_meta['attrs']['st_nlink'] += 1
            trans = self.zk.transaction()
            trans.set_data(parent, parent_meta.dumps())
            trans.create(path, new_meta.dumps())
            for r in filter(lambda e: isinstance(e, Exception) and not isinstance(e, (RuntimeInconsistency, RolledBackError)), trans.commit()):
                raise r
        except NodeExistsError as e:
            raise FuseOSError(EEXIST) from e
        except NoNodeError as e:
            raise FuseOSError(ENOENT) from e

    def open(self, path, flags):
        self.fd += 1
        self._open_files[self.fd] = BufferedWrite(path, self.fd)
        return self.fd

    def release(self, path, fh):
        self._open_files[fh].flush(self._write)
        del self._open_files[fh]
        return 0

    def read(self, path, size, offset, fh):
        self._open_files[fh].flush(self._write)
        try:
            meta = self._get_meta(path)
            return self.chunk_client.read_chunks(meta['chunks'], offset, offset+size)
        except NoNodeError as e:
            raise FuseOSError(ENOENT) from e

    def readdir(self, path, fh):
        # TODO: include stat objects?
        try:
            return ['.', '..'] + self._get_children(path)
        except NoNodeError as e:
            raise FuseOSError(ENOENT) from e

    def readlink(self, path):
        try:
            return self._get_meta(path)['target']
        except NoNodeError as e:
            raise FuseOSError(ENOENT) from e

    def rename(self, oldpath, newpath):
        # FIXME: recurse over all elements in directory
        try:
            meta = self._get_meta(oldpath)
            trans = self.zk.transaction()
            trans.create(newpath, msgpack.dumps(meta))
            trans.delete(oldpath)
            for r in filter(lambda e: isinstance(e, Exception) and not isinstance(e, (RuntimeInconsistency, RolledBackError)), trans.commit()):
                raise r
        except NodeExistsError as e:
            raise FuseOSError(EEXIST) from e
        except NoNodeError as e:
            raise FuseOSError(ENOENT) from e
        except NotEmptyError as e:
            raise FuseOSError(ENOTEMPTY) from e

    def rmdir(self, path):
        parent = posixpath.dirname(path)
        try:
            parent_meta = self._get_meta(parent)
            parent_meta['attrs']['st_nlink'] -= 1
            trans = self.zk.transaction()
            trans.delete(path)
            trans.set_data(parent, parent_meta.dumps())
            for r in filter(lambda e: isinstance(e, Exception) and not isinstance(e, (RuntimeInconsistency, RolledBackError)), trans.commit()):
                raise r
        except NoNodeError as e:
            raise FuseOSError(ENOENT) from e
        except NotEmptyError as e:
            raise FuseOSError(ENOTEMPTY) from e

    def statfs(self, path):
        # Ignores path
        try:
            return self._get_meta(self.fs_root)['fs']
        except NoNodeError as e:
            raise FuseOSError(ENOENT) from e

    def symlink(self, oldpath, newpath):
        now = time()
        new_meta = File(dict(target=oldpath,attrs=dict(
                st_mode=(S_IFLNK | 0o777),
                st_nlink=1,
                st_size=len(oldpath),
                st_ctime=now,
                st_mtime=now,
                st_atime=now,
                )))
        try:
            self.zk.create(newpath, new_meta.dumps())
        except NodeExistsError as e:
            raise FuseOSError(EEXIST) from e
        except NoNodeError as e:
            raise FuseOSError(ENOENT) from e

    def truncate(self, path, length, fh=None):
        if fh is not None:
            self._open_files[fh].flush(self._write)
        try:
            meta = self._get_meta(path)
            meta['attrs'].update(st_size=length)
            meta['chunks'] = self.chunk_client.truncate_chunks(meta['chunks'], length)
            meta._dirty = False
            self.zk.set(path, meta.dumps())
        except NoNodeError as e:
            raise FuseOSError(ENOENT) from e

    def unlink(self, path):
        try:
            self.zk.delete(path)
        except NoNodeError as e:
            raise FuseOSError(ENOENT) from e
        except NotEmptyError as e:
            raise FuseOSError(ENOTEMPTY) from e

    def utimens(self, path, times=None):
        now = time()
        atime, mtime = times if times else (now, now)
        try:
            meta = self._get_meta(path)
            meta['attrs'].update(st_atime=atime, st_mtime=mtime)
            meta._dirty = False
            self.zk.set(path, meta.dumps())
        except NoNodeError as e:
            raise FuseOSError(ENOENT) from e

    def write(self, path, data, offset, fh):
        try:
            meta = self._get_meta(path)
            if offset+len(data) > meta['attrs']['st_size']:
                meta['attrs'].update(st_size=offset+len(data))
                meta._dirty = True
        except NoNodeError as e:
            raise FuseOSError(ENOENT) from e
        open_file = self._open_files[fh]
        l = open_file.write(data, offset)
        if open_file.count > 16*1024*1024:
            open_file.flush(self._write)
        return l

    def flush(self, path, fh):
        return self._open_files[fh].flush(self._write)

    def _write(self, path, data, offset, fh):
        self.__log.debug('Applying buffered write: %s %r(%d) %d, %d', path, data[:10], len(data), offset, fh)
        # Internal write
        try:
            meta = self._get_meta(path)
            if offset+len(data) > meta['attrs']['st_size']:
                meta['attrs'].update(st_size=offset+len(data))
            meta['chunks'] = self.chunk_client.write_chunks(meta['chunks'], data, offset)
            meta._dirty = False
            self.zk.set(path, meta.dumps())
        except NoNodeError as e:
            raise FuseOSError(ENOENT) from e
        return len(data)

    def _get_used_chunks(self, root=None):
        if root is None:
            root = self.fs_root
        meta = self._get_meta(root)
        if S_ISREG(meta['attrs']['st_mode']):
            return set(meta['chunks'])
        elif S_ISDIR(meta['attrs']['st_mode']):
            children = (posixpath.join(root, child) for child in self._get_children(root))
            return reduce(set.union, (self._get_used_chunks(root=path) for path in children))
        else:
            return set()


# vim: sw=4 ts=4 expandtab
