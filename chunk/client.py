""" Distributed Filesystem: Chunkserver Client API

Project for CSCI 6450

"""

from __future__ import with_statement, division, print_function, absolute_import, unicode_literals

import posixpath
import hashlib
from .common import *

class ChunkClient(object):
    """Abstract chunk client. Provides some high-level convenience methods.

    Subclasses should implement the following:
        put(data) -> key
        get(key)  -> data

    The key, 0, is a special case: it represents a completely zero-filled chunk.

    """
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
        orig_data = self.get(key)
        if length > len(orig_data):
            return self.put(orig_data + bytes(length-len(orig_data)))
        elif length < len(orig_data):
            return self.put(orig_data[:length])
        else:
            return key

    def write_into(self, key, data, offset=0):
        orig_data = self.get(key)
        if offset <= len(orig_data):
            return self.put(b''.join([orig_data[:offset], data, orig_data[offset+len(data):]]))
        else:
            return self.put(b''.join([orig_data, bytes(offset-len(orig_data)), data]))

    # Mutate a sequence of chunks and return the modified sequence

    def truncate_chunks(self, keys, length):
        last_chunk, last_chunk_length = divmod(length-1, self.CHUNK_SIZE)
        last_chunk_length += 1
        if last_chunk < len(keys):
            # New length is shorter
            keys[last_chunk] = self.truncate(keys[last_chunk], last_chunk_length)
            del keys[last_chunk+1:]
        else:
            # New length is longer
            keys[-1] = self.truncate(keys[-1], self.CHUNK_SIZE)
            keys.extend([0]*(last_chunk-len(keys)))
            keys.append(self.truncate(0, last_chunk_length))
        return keys

    def write_chunks(self, keys, data, offset=0):
        first_chunk, first_offset = divmod(offset, self.CHUNK_SIZE)
        last_chunk, last_offset = divmod(offset+len(data)-1, self.CHUNK_SIZE)
        last_offset += 1

        if len(keys) <= last_chunk:
            # Write extends beyond current length; truncate to start and append
            keys = self.truncate_chunks(keys, offset)

        fragments = [data[max(0, s):s+self.CHUNK_SIZE] for s in range(-first_offset,len(data),self.CHUNK_SIZE)]
        keys[first_chunk] = self.write_into(keys[first_chunk], fragments[0], first_offset)
        if last_chunk > first_chunk:
            # Whole chunks
            keys[first_chunk+1:last_chunk] = (self.put(f) for f in fragments[1:-1])
            if len(keys) > last_chunk:
                # Write last partial chunk within existing chunk
                keys[last_chunk] = self.write_into(keys[last_chunk], fragments[-1])
            else:
                # Write extends beyond existing chunks
                keys[last_chunk:] = [self.put(fragments[-1])]

        return keys

    def get_chunks(self, keys, start=None, end=None):
        """ Get and concatenate the chunk data for each key in keys.

        If given, start and/or end should be offsets in bytes.

        """
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
            chunks[0] = chunks[first_offset:last_offset]
        else:
            chunks[0] = chunks[first_offset:]
            chunks[-1] = chunks[:last_offset]

        return b''.join(chunks)


class LocalChunkClient(ChunkClient):
    """ Simple, file-backed local chunk cache. """

    def __init__(self, cache_path, **kwargs):
        super().__init__(**kwargs)
        self.cache_path = cache_path
        self.chunks = {}
        self._zero_chunk = bytes(self.CHUNK_SIZE)
        self._zero_chunk_key = int(hashlib.sha1(self.ZERO_CHUNK).hexdigest(), 16)
        self.chunks[0] = self.chunks[self._zero_chunk_key] = self._zero_chunk

    @property
    def ZERO_CHUNK(self):
        return self._zero_chunk

    @property
    def ZERO_CHUNK_KEY(self):
        return self._zero_chunk_key

    def put(self, data):
        """ Store chunk data, returning its key. """
        file_name = hashlib.sha1(data).hexdigest()
        key = int(fname, 16)
        if key == self.ZERO_CHUNK_KEY:
            # No need to store
            return 0
        self.chunks[key] = data
        with open(posixpath.join(self.cache_path, file_name), mode='wb') as f:
            f.write(data)
        return int(key, 16)

    def get(self, key):
        """ Get the chunk data for the given numeric key. """
        try:
            return self.chunks[key]
        except KeyError:
            with open(posixpath.join(self.cache_path, '%040x' % (key,)), mode='rb') as f:
                data = f.read()
            self.chunks[key] = data
            return data




# vim: sw=4 ts=4 expandtab
