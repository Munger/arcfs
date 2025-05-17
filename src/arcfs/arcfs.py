"""
ArcFS: Transparent Archive File System

A Python library that allows working with archives as if they were directories,
providing transparent access to files within archives.

This module combines all the operation implementations into a complete ArchiveFS class.

Author: Tim Hosking
Contact: https://github.com/Munger
License: MIT
"""

import arcfs.handlers
from typing import Dict, List, Optional, Union, BinaryIO, TextIO, Any, Iterator, Tuple
import os
import contextlib

from .core.path_resolver import PathResolver, PathInfo
from .core.archive_handlers import get_handler_for_path, get_supported_formats
from .core.stream_provider import StreamProvider
from .core.utils import is_archive_format
from .core.stream_operations import StreamOperations

# API classes for public operations
from .api.files_api import FilesAPI
from .api.dirs_api import DirsAPI

from .api.config_api import ConfigAPI
from .api.batch_api import BatchAPI

__version__ = '0.1.0'


class ArchiveFS:
    """
    Main entry point for the Archive File System.
    Provides a clean, namespaced API for working with files, directories, and archives.

    Attributes:
        files: File operations (open, read, write, etc.)
        dirs: Directory operations (mkdir, rmdir, list, etc.)
        archives: Archive operations (create, extract, compress, etc.)
        batch: Batch/transactional operations
        config: Configuration
    """
    def __init__(self):
        from .core.path_resolver import PathResolver
        from .core.stream_provider import StreamProvider
        from .api.files_api import FilesAPI
        from .api.dirs_api import DirsAPI
        from .api.config_api import ConfigAPI
        from .api.batch_api import BatchAPI
        self._path_resolver = PathResolver()
        self._stream_provider = StreamProvider()
        self.files = FilesAPI(self)
        self.dirs = DirsAPI(self)
        self.config = ConfigAPI()
        self.batch = BatchAPI(self)

    # TODO: This method should be accessed via the appropriate API (e.g., files, dirs, batch, etc.)
    """
    fs.batch: Public API for batching operations.
    Usage:
        with fs.batch as batch:
            batch.write(...)
            batch.mkdir(...)
        # Or, manual commit:
        batch = fs.batch
        batch.write(...)
        batch.commit()
    """
    #     Returns:
    #         A file-like object with standard read/write methods
    #     """
    #     path_info = self._path_resolver.resolve(path)
    #     
    #     # Handle write modes which may need to create parent directories/archives
    # TODO: This method should be accessed via the appropriate API (e.g., files, batch, etc.)
    # def read(self, path: str, binary: bool = False) -> Union[str, bytes]:
    #     """
    #     Read entire file contents.
    #     
    #     Args:
    #         path: Path to the file
    #         binary: If True, return bytes instead of string
    #         
    #     Returns:
    #         File contents as string or bytes
    #     """
    #     mode = 'rb' if binary else 'r'
    #     with self.open(path, mode) as f:
    #         return f.read()
    
    # TODO: This method should be accessed via the appropriate API (e.g., files, batch, etc.)
    # def write(self, path: str, data: Union[str, bytes], binary: bool = False) -> None:
    #     """
    #     Write data to a file or archive entry.
    #     
    #     Args:
    #         path: Path to the file
    #         data: Data to write
    #         binary: If True, write bytes instead of string
    #     """
    #     # Choose the correct mode
    #     mode = 'wb' if binary else 'w'
    #     
    #     # Handle type conversion explicitly
    #     if binary:
    #         # If binary mode, ensure data is bytes
    #         if isinstance(data, str):
    #             data = data.encode('utf-8')
    #     else:
    #         # If text mode, ensure data is str
    #         if isinstance(data, bytes):
    #             data = data.decode('utf-8')
    #     
    #     # Write the data
    #     with self.open(path, mode) as f:
    #         f.write(data)
    
    # TODO: This method should be accessed via the appropriate API (e.g., files, batch, etc.)
    # def append(self, path: str, data: Union[str, bytes], binary: bool = False) -> None:
    #     """
    #     Append data to existing file or archive entry.
    #     
    #     Args:
    #         path: Path to the file
    #         data: Data to append
    #         binary: If True, append bytes instead of string
    #     """
    #     # Choose the correct mode
    #     mode = 'ab' if binary else 'a'
    #     
    #     # Handle type conversion explicitly
    #     if binary:
    #         # If binary mode, ensure data is bytes
    #         if isinstance(data, str):
    #             data = data.encode('utf-8')
    #     else:
    #         # If text mode, ensure data is str
    #         if isinstance(data, bytes):
    #             data = data.decode('utf-8')
    #     
    #     # Append the data
    #     with self.open(path, mode) as f:
    #         f.write(data)

    # TODO: This method should be accessed via the appropriate API (e.g., files, dirs, etc.)
    # def exists(self, path: str) -> bool:
    #     """
    #     Check if a file or directory exists at the given path.
    #     """
    #     # Try both FilesAPI and DirsAPI exists methods
    #     try:
    #         if self.files.exists(path):
    #             return True
    #     except Exception:
    #         pass
    #     try:
    #         if self.dirs.exists(path):
    #             return True
    #     except Exception:
    #         pass
    #     return False
    
    @contextlib.contextmanager
    def batch_session(self):
        """
        Return a session object for grouping operations.
        
        Returns:
            Session object with the same interface as ArchiveFS
        """
        from .api.batch_api import BatchAPI
        session = BatchSession(self)
        try:
            yield session
        finally:
            session.commit()
    
