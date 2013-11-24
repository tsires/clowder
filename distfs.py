#!/usr/bin/env python3
""" Distributed Filesystem: FUSE Implementation

Project for CSCI 6450

Usage: ./distfs.py <root_name> <mountpoint>
"""

from __future__ import with_statement, division, print_function, absolute_import, unicode_literals

import logging
import posixpath
from errno import EEXIST, ENOENT, ENOTEMPTY
from stat import S_IFDIR, S_IFLNK, S_IFREG
from time import time

import msgpack
from fuse import FUSE, FuseOSError, Operations, LoggingMixIn
from kazoo.client import KazooClient
from kazoo.exceptions import NodeExistsError, NoNodeError, NotEmptyError, RuntimeInconsistency, RolledBackError
from kazoo.protocol.states import EventType

from chunk.client import LocalChunkClient

class File(dict):
    def __init__(self, data, znode=None):
        super().__init__(data)
        self._znode = znode

    def dumps(self):
        return msgpack.dumps(self, encoding='utf-8')

    @classmethod
    def loads(cls, data):
        return cls(msgpack.loads(data[0], encoding='utf-8'), data[1])


class Cache(object):
    __log = logging.getLogger('distfs.cache')
    def __init__(self, get):
        self._get = get
        self._cache = {}
        self._tries = 0
        self._misses = 0

    def get(self, path):
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


class BufferedWrite(object):
    def __init__(self, path, fh):
        self._path = path
        self._fh = fh
        self._writes = []

    def write(self, data, offset):
        if len(self._writes) and self._writes[-1][1] == offset:
            self._writes[-1][1]+=len(data)
            self._writes[-1][2].append(data)
        else:
            self._writes.append([offset, offset+len(data), [data]])
        return len(data)

    def flush(self, func):
        for w in self._writes:
            func(self._path, b''.join(w[2]), w[0], self._fh)
        self._writes = []
        return 0

class DistFS(LoggingMixIn, Operations):
    'Distributed filesystem. Queries Zookeeper for directory contents and metadata.'

    FILESYSTEMS = posixpath.join('/', 'fs', 'trees')
    CHUNK_SIZE = 4*1024
    __log = logging.getLogger('distfs')

    def __init__(self, zk, chunk_client, fs_root):
        self.zk = zk
        self.chunk_client = chunk_client
        self.fs_root = posixpath.join(self.FILESYSTEMS, fs_root)
        # Caches
        self._meta_cache = Cache(get=lambda p, w: File.loads(zk.get(p, w)))
        self._children_cache = Cache(get=zk.get_children)
        # FIXME: placeholders until finished refactoring
        self._get_meta = self._meta_cache.get
        self._get_children = self._children_cache.get
        # Internal FD counter
        self._open_files = {}
        self.fd = 0

    def bootstrap(self):
        now = time()
        root_meta = File(dict(
            fs=dict(
                f_bsize=self.CHUNK_SIZE,
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
            self.zk.create(self.fs_root, root_meta.dumps(), makepath=True)
            self.__log.info('Created root directory at %s', self.fs_root)
        except NodeExistsError as e:
            self.__log.info('Mounted existing root directory at %s', self.fs_root)

    def destroy(self, path):
        self.__log.debug("Metadata cache hits: %d; misses: %d", *(self._meta_cache.stats()))
        self.__log.debug("Children cache hits: %d; misses: %d", *(self._children_cache.stats()))

    def _op_stub(self, op, *args):
        self.__log.debug('[STUB] %s: %r', op, args)

    def _zk_path(self, path):
        return posixpath.join(self.fs_root, path.lstrip('/')).rstrip('/')

    def chmod(self, path, mode):
        path = self._zk_path(path)
        try:
            meta = self._get_meta(path)
            meta['attrs'].update(st_mode=mode)
            self.zk.set(path, meta.dumps())
        except NoNodeError as e:
            raise FuseOSError(ENOENT) from e

    def chown(self, path, uid, gid):
        # FIXME: maybe this should be a single-user fs?
        path = self._zk_path(path)
        try:
            meta = self._get_meta(path)
            meta['attrs'].update(st_uid=uid, st_gid=gid)
            self.zk.set(path, meta.dumps())
        except NoNodeError as e:
            raise FuseOSError(ENOENT) from e

    def create(self, path, mode):
        path = self._zk_path(path)
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
        path = self._zk_path(path)
        try:
            return self._get_meta(path)['attrs']
        except NoNodeError as e:
            raise FuseOSError(ENOENT) from e

    def mkdir(self, path, mode):
        path = self._zk_path(path)
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
        path = self._zk_path(path)
        self.fd += 1
        self._open_files[self.fd] = BufferedWrite(path, self.fd)
        return self.fd

    def release(self, path, fh):
        del self._open_files[fh]
        return 0

    def read(self, path, size, offset, fh):
        path = self._zk_path(path)
        try:
            meta = self._get_meta(path)
            return self.chunk_client.read_chunks(meta['chunks'], offset, offset+size)
        except NoNodeError as e:
            raise FuseOSError(ENOENT) from e

    def readdir(self, path, fh):
        # TODO: include stat objects?
        path = self._zk_path(path)
        try:
            return ['.', '..'] + self._get_children(path)
        except NoNodeError as e:
            raise FuseOSError(ENOENT) from e

    def readlink(self, path):
        path = self._zk_path(path)
        try:
            return self._get_meta(path)['target']
        except NoNodeError as e:
            raise FuseOSError(ENOENT) from e

    def rename(self, oldpath, newpath):
        # FIXME: recurse over all elements in directory
        oldpath = self._zk_path(oldpath)
        newpath = self._zk_path(newpath)
        try:
            meta = self._get_meta(path)
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
        path = self._zk_path(path)
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
        newpath = self._zk_path(newpath)
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
        path = self._zk_path(path)
        try:
            meta = self._get_meta(path)
            meta['attrs'].update(st_size=length)
            meta['chunks'] = self.chunk_client.truncate_chunks(meta['chunks'], length)
            self.zk.set(path, meta.dumps())
        except NoNodeError as e:
            raise FuseOSError(ENOENT) from e

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
            self.zk.set(path, meta.dumps())
        except NoNodeError as e:
            raise FuseOSError(ENOENT) from e

    def write(self, path, data, offset, fh):
        return self._open_files[fh].write(data, offset)

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
            self.zk.set(path, meta.dumps())
        except NoNodeError as e:
            raise FuseOSError(ENOENT) from e
        return len(data)


def main(args):
    # Zookeeper
    zk_hosts = [('127.0.0.1', '2181')]
    hosts = ','.join(':'.join(host) for host in zk_hosts) 
    zk = KazooClient(hosts=hosts)
    zk.start()

    # ChunkClient
    cc = LocalChunkClient('/tmp/chunkcache')

    # DistFS
    distfs = DistFS(zk=zk, chunk_client=cc, fs_root=args.source)
    distfs.bootstrap()

    fuse = FUSE(distfs, args.directory, foreground=args.foreground)
    zk.stop()
    return 0

if __name__ == '__main__':
    from sys import exit
    import logging
    import argparse

    parser = argparse.ArgumentParser(description='Mount a Clowder filesystem tree.')
    parser.add_argument('-v', '--verbose', action='count')
    parser.add_argument('-q', '--quiet', action='count')
    group = parser.add_mutually_exclusive_group()
    group.add_argument('-f', '--foreground', action='store_true')
    group.add_argument('-b', '--background', action='store_false')
    parser.add_argument('source')
    parser.add_argument('directory')
    parser.set_defaults(foreground=True, verbose=0, quiet=0)

    args = parser.parse_args()

    verbosity = args.verbose - args.quiet
    log_level = logging.WARN - verbosity*10

    logging.basicConfig(level=log_level)
    logging.getLogger('kazoo.client').setLevel(log_level + 20)
    exit(main(args))

# vim: sw=4 ts=4 expandtab
