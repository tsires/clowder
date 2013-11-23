""" Distributed Filesystem: Chunkserver Client API

Project for CSCI 6450

"""

from __future__ import with_statement, division, print_function, absolute_import, unicode_literals

import logging
import posixpath

import mmh3

from .common import *

def chunk_hash(data):
    return '%016x%016x' % mmh3.hash64(data)

ZERO = ''

class ChunkClient(object):
    """Abstract chunk client. Provides some high-level convenience methods.

    Subclasses should implement the following:
        put(data) -> key
        get(key)  -> data

    The key, 0, is a special case: it represents a completely zero-filled chunk.

    """
    __log = logging.getLogger('distfs.chunk_client')

    def __init__(self, chunk_size=64*1024):
        self._chunk_size = chunk_size

    @property
    def CHUNK_SIZE(self):
        return self._chunk_size

    #def put(self, data):
    #    """ Store chunk data, returning its key. """
    #    pass

    #def get(self, key):
    #    """ Get the chunk data for the given numeric key. """
    #    pass

    # Convenience methods

    # Mutate one chunk and return the key of the result

    def truncate(self, key, length):
        self.__log.debug('truncate(key=%r, length=%r)', key, length)
        orig_data = self.get(key)
        if length > len(orig_data):
            return self.put(orig_data + bytes(length-len(orig_data)))
        elif length < len(orig_data):
            return self.put(orig_data[:length])
        else:
            return key

    def write_into(self, key, data, offset=0):
        self.__log.debug('write_into(key=%r, data=%r, offset=%r)', key, data, offset)
        orig_data = self.get(key)
        if offset <= len(orig_data):
            return self.put(b''.join([orig_data[:offset], data, orig_data[offset+len(data):]]))
        else:
            return self.put(b''.join([orig_data, bytes(offset-len(orig_data)), data]))

    # Mutate a sequence of chunks and return the modified sequence

    def truncate_chunks(self, keys, length):
        self.__log.debug('truncate_chunks(keys=%r, length=%r)', keys, length)
        if length == 0:
            del keys[:]
            return keys
        last_chunk, last_chunk_length = divmod(length, self.CHUNK_SIZE)
        if last_chunk_length == 0:
            last_chunk -= 1
        if last_chunk < len(keys):
            # New length is shorter or same number of chunks
            if last_chunk_length > 0:
                keys[last_chunk] = self.truncate(keys[last_chunk], last_chunk_length)
            del keys[last_chunk+1:]
        else:
            # New length is longer
            keys[-1] = self.truncate(keys[-1], self.CHUNK_SIZE)
            keys.extend([ZERO]*(last_chunk-len(keys)))
            if last_chunk_length > 0:
                keys.append(self.put(bytes(last_chunk_length)))
        return keys

    def write_chunks(self, keys, data, offset=0):
        self.__log.debug('write_chunks(keys=%r, data=%r, offset=%r)', keys, data, offset)
        if len(data) == 0:
            return keys
        first_chunk, first_offset = divmod(offset, self.CHUNK_SIZE)
        last_chunk, last_offset = divmod(offset+len(data)-1, self.CHUNK_SIZE)
        last_offset += 1

        if len(keys) <= last_chunk:
            # Write extends beyond current length; truncate to start and append
            keys = self.truncate_chunks(keys, offset)

        fragments = [data[max(0, s):s+self.CHUNK_SIZE] for s in range(-first_offset,len(data),self.CHUNK_SIZE)]
        if last_chunk == first_chunk:
            # Write one chunk
            if len(keys) == first_chunk:
                keys.append(self.put(fragments[0]))
            elif first_offset == 0 and last_offset == self.CHUNK_SIZE:
                keys[first_chunk] = self.put(fragments[0])
            else:
                keys[first_chunk] = self.write_into(keys[first_chunk], fragments[0], first_offset)
        else:
            # Write first (possibly partial) chunk
            if first_offset == 0:
                keys[first_chunk:first_chunk+1] = [self.put(fragments[0])]
            else:
                keys[first_chunk] = self.write_into(keys[first_chunk], fragments[0], first_offset)
            # Write whole chunks
            keys[first_chunk+1:last_chunk] = (self.put(f) for f in fragments[1:-1])
            if len(keys) > last_chunk:
                # Write last partial chunk within existing chunk
                keys[last_chunk] = self.write_into(keys[last_chunk], fragments[-1])
            else:
                # Write extends beyond existing chunks
                keys[last_chunk:] = [self.put(fragments[-1])]

        return keys

    def read_chunks(self, keys, start=None, end=None):
        """ Get and concatenate the chunk data for each key in keys.

        If given, start and/or end should be offsets in bytes.

        """
        self.__log.debug('read_chunks(keys=%r, start=%r, end=%r)', keys, start, end)
        if start is None:
            start = 0
        if end is None:
            end = len(keys) * self.CHUNK_SIZE
        first_chunk, first_offset = divmod(start, self.CHUNK_SIZE)
        last_chunk, last_offset = divmod(end-1, self.CHUNK_SIZE)
        last_offset += 1

        if len(keys) <= first_chunk:
            return b''
        elif len(keys) <= last_chunk:
            last_chunk = len(keys)-1
            last_offset = None

        chunks = []
        # Start with whole chunks
        chunks.extend(self.get(k) for k in keys[first_chunk:last_chunk+1])

        # Slice out portions of the first and last chunks
        if first_chunk == last_chunk:
            chunks[0] = chunks[0][first_offset:last_offset]
        else:
            chunks[0] = chunks[0][first_offset:]
            chunks[-1] = chunks[-1][:last_offset]

        return b''.join(chunks)


class LocalChunkClient(ChunkClient):
    """ Simple, file-backed local chunk cache. """

    def __init__(self, cache_path, **kwargs):
        super().__init__(**kwargs)
        self.cache_path = cache_path
        self.chunks = {}
        self.chunks[ZERO] = bytes(self.CHUNK_SIZE)

    def put(self, data):
        """ Store chunk data, returning its key. """
        key = chunk_hash(data)
        self.chunks[key] = data
        with open(posixpath.join(self.cache_path, key), mode='wb') as f:
            f.write(data)
        return key

    def get(self, key):
        """ Get the chunk data for the given numeric key. """
        try:
            return self.chunks[key]
        except KeyError:
            try:
                with open(posixpath.join(self.cache_path, key), mode='rb') as f:
                    data = f.read()
                self.chunks[key] = data
                return data
            except FileNotFoundError as e:
                raise ChunkNotFoundError(key) from e




# vim: sw=4 ts=4 expandtab
