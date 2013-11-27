__all__ = ['ClowderFSError', 'FSAlreadyExistsError', 'ChunkServerError', 'ChunkNotFoundError']

class ClowderFSError(Exception):
    pass

class FSAlreadyExistsError(ClowderFSError):
    pass

class ChunkServerError(ClowderFSError):
    pass

class ChunkNotFoundError(ChunkServerError):
    pass

# vim: sw=4 ts=4 expandtab
