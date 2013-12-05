__all__ = ['mkfs', 'mount', 'chunk']

import posixpath
import argparse
import logging

from kazoo.client import KazooClient
from fuse import FUSE

from .fs import ClowderFS
from .chunk_client_http import HTTPChunkClient
from .chunk_server_http import HTTPChunkServer
from .common import *

# Set up parsers
base_parser = argparse.ArgumentParser(add_help=False)
base_parser.add_argument('-v', '--verbose', action='count')
base_parser.add_argument('-q', '--quiet', action='count')
base_parser.add_argument('-s', '--server', action='append', dest='servers')
base_parser.set_defaults(verbose=0, quiet=0, servers=[])

mount_parser = argparse.ArgumentParser(parents=[base_parser], description='Mount a Clowder filesystem tree.')
group = mount_parser.add_mutually_exclusive_group()
group.add_argument('-f', '--foreground', action='store_true')
group.add_argument('-b', '--background', action='store_false', dest='foreground')
mount_parser.add_argument('-c', '--chunk-cache', dest='chunk_cache')
mount_parser.add_argument('-H', '--hash-data', action='store_true', dest='hash_data')
mount_parser.add_argument('-d', '--debug', action='store_true')
mount_parser.add_argument('source')
mount_parser.add_argument('directory')
mount_parser.set_defaults(foreground=True, chunk_cache='/tmp/chunkcache', hash_data=False, debug=False)

mkfs_parser = argparse.ArgumentParser(parents=[base_parser], description='Create a new Clowder filesystem tree.')
mkfs_parser.add_argument('name')
mkfs_parser.add_argument('-b', '--block-size', dest='chunk_size')
mkfs_parser.set_defaults(chunk_size=64*1024)

chunk_parser = argparse.ArgumentParser(parents=[base_parser], description='Launch a Clowder chunk server.')
chunk_parser.add_argument('-c', '--chunk-cache', dest='chunk_cache')
chunk_parser.add_argument('-H', '--hash-data', action='store_true', dest='hash_data')
chunk_parser.add_argument('-d', '--debug', action='store_true')
chunk_parser.add_argument('-b', '--bind-address', action='store', dest='host')
chunk_parser.add_argument('-p', '--bind-port', action='store', dest='port')
chunk_parser.set_defaults(chunk_cache='/tmp/chunkcache', hash_data=False, debug=False, host='127.0.0.1', port='33333')


def mount(args=None):
    args = mount_parser.parse_args(args)

    # Debug
    foreground = args.foreground or args.debug
    nothreads = args.debug
    # Log verbosity
    verbosity = args.verbose - args.quiet
    if args.debug:
        log_level = logging.DEBUG - verbosity*10
    else:
        log_level = logging.WARN - verbosity*10

    logging.basicConfig(level=log_level)
    logging.getLogger('kazoo.client').setLevel(log_level + 20)

    # ZK Path of filesystem root
    zk_root = posixpath.join(FILESYSTEMS, args.source)

    # Zookeeper servers
    if len(args.servers):
        zk_hosts = ','.join(args.servers)
    else:
        zk_hosts = '127.0.0.1:2181'

    # Zookeeper client
    zk = KazooClient(hosts=zk_hosts)

    zk.start()

    # ChunkClient
    cc = HTTPChunkClient(zk=zk, cache_path=args.chunk_cache, hash_data=args.hash_data)

    # ClowderFS
    distfs = ClowderFS(zk=zk, chunk_client=cc, fs_root=zk_root)

    # FUSE
    fuse = FUSE(distfs, args.directory, foreground=foreground, nothreads=nothreads, fsname=args.source, subtype='clowder', big_writes=True, modules='subdir', subdir=zk_root)

    # Cleanup
    zk.stop()

def mkfs(args=None):
    args = mkfs_parser.parse_args(args)

    # Log verbosity
    verbosity = args.verbose - args.quiet
    log_level = logging.WARN - verbosity*10

    logging.basicConfig(level=log_level)
    logging.getLogger('kazoo.client').setLevel(log_level + 20)

    # ZK Path of filesystem root
    zk_root = posixpath.join(FILESYSTEMS, args.name)

    # Zookeeper
    if len(args.servers):
        zk_hosts = ','.join(args.servers)
    else:
        zk_hosts = '127.0.0.1:2181'
    zk = KazooClient(hosts=zk_hosts)

    zk.start()

    # Run
    ClowderFS.mkfs(zk=zk, fs_root=zk_root, chunk_size=args.chunk_size)

    # Cleanup
    zk.stop()

def chunk(args=None):
    args = chunk_parser.parse_args(args)

    # Log verbosity
    verbosity = args.verbose - args.quiet
    if args.debug:
        log_level = logging.DEBUG - verbosity*10
    else:
        log_level = logging.WARN - verbosity*10

    logging.basicConfig(level=log_level)
    logging.getLogger('kazoo.client').setLevel(log_level + 20)

    # Zookeeper servers
    if len(args.servers):
        zk_hosts = ','.join(args.servers)
    else:
        zk_hosts = '127.0.0.1:2181'

    # Zookeeper client
    zk = KazooClient(hosts=zk_hosts)

    zk.start()

    # ChunkServer
    cs = HTTPChunkServer(zk=zk, addr=(args.host,args.port), cache_path=args.chunk_cache, hash_data=args.hash_data)
    cs.run()

    # Cleanup
    zk.stop()


# vim: sw=4 ts=4 expandtab

