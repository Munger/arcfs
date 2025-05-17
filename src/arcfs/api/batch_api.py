"""
ARCFS Batch API

Summary:
    Public interface for batching multiple file/archive operations in ArchiveFS.
    Allows users to group operations into a session that is committed atomically
    for performance and consistency.

Author: Tim Hosking
Contact: https://github.com/Munger
License: MIT
"""

from ..core.batch_session import BatchSession

class BatchAPI:
    """
    Public API for batching operations in ArchiveFS.
    Exposed as fs.batch.

    Usage patterns:
        # Context manager (recommended):
        with fs.batch as batch:
            batch.write('archive.zip/foo.txt', 'data')
            batch.append('archive.zip/foo.txt', 'more', binary=False)
            batch.mkdir('archive.zip/bar')
            batch.open('archive.zip/foo.txt', 'w')
        # All changes are committed at the end of the block.

        # Manual session:
        batch = fs.batch
        batch.write('archive.zip/foo.txt', 'data')
        batch.commit()

    Available operations (proxied from BatchSession):
        - write(path, data, binary=False): Write data to a file in an archive.
        - append(path, data, binary=False): Append data to a file in an archive.
        - mkdir(path, create_parents=False): Create a directory in an archive.
        - open(path, mode='r', encoding='utf-8'): Open a file in an archive.
        - commit(): Commit all pending changes (auto-called at context exit).

    All methods not explicitly defined here are dynamically proxied to BatchSession.
    See arcfs.batch_session.BatchSession for full details and additional methods.
    """
    def __init__(self, archive_fs):
        self._fs = archive_fs
        self._session = None

    def __enter__(self):
        self._session = BatchSession(self._fs)
        return self._session

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self._session:
            self._session.commit()
            self._session = None

    def __getattr__(self, name):
        # Proxy methods to a new session if not in a context
        if self._session is not None:
            return getattr(self._session, name)
        # Create a temporary session for one-off calls
        temp_session = BatchSession(self._fs)
        return getattr(temp_session, name)

    def commit(self):
        if self._session:
            self._session.commit()
        else:
            raise RuntimeError("No active batch session to commit.")
