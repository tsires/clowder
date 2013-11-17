__all__ = ['ChunkServerError', 'ChunkNotFoundError']

class ChunkServerError(Exception):
    pass

class ChunkNotFoundError(ChunkServerError):
    pass


# vim: sw=4 ts=4 expandtab
