""" Distributed Filesystem: Chunkserver Client API

Project for CSCI 6450

"""

from __future__ import with_statement, division, print_function, absolute_import, unicode_literals

import os
import logging
from base64 import b16encode
from uuid import uuid1
import concurrent.futures

import mmh3

from .common import *
from .exception import *

def chunk_hash(data):
    return b16encode(mmh3.hash_bytes(data)).decode('ascii')

def chunk_uuid(data):
    return b16encode(mmh3.hash_bytes(uuid1().bytes)).decode('ascii')

ZERO = ''

class ChunkClient(object):
    """Abstract chunk client. Provides some high-level convenience methods.

    Subclasses should implement the following:
        put(data) -> key
        get(key)  -> data

    The key, 0, is a special case: it represents a completely zero-filled chunk.

    """
    __log = logging.getLogger('distfs.chunk_client')

    def __init__(self, chunk_size=64*1024, **kwargs):
        super().__init__(**kwargs)
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
        'Mutate and return a list of up to one chunk by truncating it'
        self.__log.debug('truncate(key=%r, length=%r)', key, length)

        if length == 0:
            # No chunk here
            return []
        elif length == self.CHUNK_SIZE and len(key) == 0:
            # Optimize for this edge case
            # NOTE: this isn't strictly necessary, but avoids hashing a bunch of zeros again
            return [ZERO]

        try:
            orig_data = self.get(key[0])
        except IndexError:
            orig_data = b''

        if length > len(orig_data):
            # Zero-pad this data
            return [self.put(b''.join([orig_data, bytes(length-len(orig_data))]))]
        elif length < len(orig_data):
            # Shorten this data
            return [self.put(orig_data[:length])]
        else:
            # No change in length
            # NOTE: this is fine even if len(key) was zero
            return key


    def write_into(self, key, data, offset=0):
        'Mutate and return a list of up to one chunk by writing data into it'
        self.__log.debug('write_into(key=%r, data=%r(%d), offset=%r)', key, data[:16], len(data), offset)

        if len(data) == 0:
            # No change to data
            # NOTE: Not sure if this happens, but avoids some edge cases
            return key
        elif len(data) == self.CHUNK_SIZE:
            # Completely replacing data
            return [self.put(data)]

        try:
            orig_data = self.get(key[0])
        except IndexError:
            orig_data = b''

        if offset <= len(orig_data):
            return [self.put(b''.join([orig_data[:offset], data, orig_data[offset+len(data):]]))]
        else:
            # NOTE: This can still happen because truncate_chunks only
            # gets called if truncate is necessary at the chunk level
            return [self.put(b''.join([orig_data, bytes(offset-len(orig_data)), data]))]


    # Mutate a sequence of chunks and return the modified sequence

    def truncate_chunks(self, keys, length):
        'Mutate and return a list of chunks by truncating them'
        self.__log.debug('truncate_chunks(keys=%r, length=%r)', keys, length)

        if length == 0:
            # Remove all chunks
            del keys[:]
            return keys

        # Determine the last partial chunk
        last_chunk, last_chunk_length = divmod(length, self.CHUNK_SIZE)

        # Extend to new length if longer
        if last_chunk >= max(1, len(keys)):
            # Extend the current last chunk if necessary
            keys[-1:] = self.truncate(keys[-1:], self.CHUNK_SIZE)
            # Sparse fill complete chunks, if any
            keys.extend([ZERO]*(last_chunk-len(keys)))

        # Truncate the new last chunk
        keys[last_chunk:] = self.truncate(keys[last_chunk:last_chunk+1], last_chunk_length)

        return keys


    def write_chunks(self, keys, data, offset=0):
        'Mutate and return a list of chunks by writing data into them'
        self.__log.debug('write_chunks(keys=%r, data=%r(%d), offset=%r)', keys, data[:16], len(data), offset)

        # Determine the first chunk and last chunks to be modified
        # as well as the offsets within those chunks
        # NOTE: len(data) == 0 --> first_chunk == last_chunk-1
        first_chunk, first_offset = divmod(offset, self.CHUNK_SIZE)
        last_chunk, last_offset = divmod(offset+len(data)-1, self.CHUNK_SIZE)
        last_offset += 1

        # If offset > file length, will need to truncate to offset
        # We don't know actual file length in bytes, but we do know the number of chunks
        # NOTE: len(data) == 0 --> first_chunk == last_chunk-1; hence, we use the greater of the two
        if len(keys) <= max(first_chunk, last_chunk):
            # Write extends beyond current length; truncate to start and append
            keys = self.truncate_chunks(keys, offset)

        # NOTE: Not sure if this ever actually happens, but handling it lets us safely ignore the edge case
        if len(data) == 0:
            return keys

        # Split data into fragments to be written into each chunk
        fragments = [data[max(0, s):s+self.CHUNK_SIZE] for s in range(-first_offset,len(data),self.CHUNK_SIZE)]

        with concurrent.futures.ThreadPoolExecutor(max_workers=8) as executor:
            # Write first (possibly partial) chunk
            keys[first_chunk:first_chunk+1] = self.write_into(keys[first_chunk:first_chunk+1], fragments[0], first_offset)
            if last_chunk > first_chunk:
                # Write whole chunks
                keys[first_chunk+1:last_chunk] = list(executor.map(self.put, fragments[1:-1]))
                # Write last (possibly partial) chunk
                keys[last_chunk:last_chunk+1] = self.write_into(keys[last_chunk:last_chunk+1], fragments[-1])

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
            last_offset = self.CHUNK_SIZE

        with concurrent.futures.ThreadPoolExecutor(max_workers=8) as executor:
            # Start with whole chunks
            chunks = list(executor.map(self.get, keys[first_chunk:last_chunk+1]))

        # Slice out portions of the first and last chunks
        if last_offset < self.CHUNK_SIZE:
            chunks[-1] = chunks[-1][:last_offset]
        if first_offset > 0:
            chunks[0] = chunks[0][first_offset:]

        return b''.join(chunks)


class LocalChunkClient(ChunkClient):
    """ Simple, file-backed local chunk cache. """

    def __init__(self, cache_path, hash_data=False, **kwargs):
        super().__init__(**kwargs)
        self.cache_path = cache_path
        if hash_data:
            self.chunk_hash = chunk_hash
        else:
            self.chunk_hash = chunk_uuid
        self.ZERO_CHUNK = bytes(self.CHUNK_SIZE)

    def put(self, data, key=None):
        """ Store chunk data, returning its key.
        
        If key is not given, one will be generated according to the selected scheme.

        """
        if not key:
            key = self.chunk_hash(data)
        chunk_path = os.path.join(self.cache_path, key)
        if not os.path.exists(chunk_path):
            with open(chunk_path, mode='wb') as f:
                f.write(data)
        return key

    def get(self, key):
        """ Get the chunk data for the given numeric key. """
        if not key:
            return self.ZERO_CHUNK
        try:
            with open(os.path.join(self.cache_path, key), mode='rb') as f:
                return f.read()
        except IOError as e:
            raise ChunkNotFoundError(key) from e

    def garbage_collect(self, keys):
        """ Purge all chunks from cache that are not in the given set of keys. """
        for key in (set(os.listdir(self.cache_path)) - keys):
            os.remove(os.path.join(self.cache_path, key))


# vim: sw=4 ts=4 expandtab
