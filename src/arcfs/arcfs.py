"""
ArcFS: Transparent Archive File System

A Python library that allows working with archives as if they were directories,
providing transparent access to files within archives.

This module combines all the operation implementations into a complete ArchiveFS class.

Author: Tim Hosking
Contact: https://github.com/Munger
License: MIT
"""

from typing import Dict, List, Optional, Union, BinaryIO, TextIO, Any, Iterator, Tuple
import os
import contextlib

from .path_resolver import PathResolver, PathInfo
from .archive_handlers import get_handler_for_path, get_supported_formats
from .stream_provider import StreamProvider
from .utils import is_archive_format
from .file_operations import FileOperations
from .directory_operations import DirectoryOperations
from .stream_operations import StreamOperations
from .archive_operations import ArchiveOperations
from .batch_session import BatchSession

__version__ = '0.1.0'


class ArchiveFS(FileOperations, DirectoryOperations, StreamOperations, ArchiveOperations):
    """
    Main class for the Archive File System.
    Provides file operations that transparently handle archives.
    """
    
    def __init__(self) -> None:
        """Initialize the ArchiveFS with default configuration."""
        # Initialize components
        self._path_resolver = PathResolver()
        self._stream_provider = StreamProvider()
        self._max_buffer_size: Optional[int] = 50 * 1024 * 1024  # 50MB default
        self._temp_dir: Optional[str] = None
        
    def configure(self, max_buffer_size: Optional[int] = None, 
                 temp_dir: Optional[str] = None) -> None:
        """
        Configure global library settings.
        
        Args:
            max_buffer_size: Maximum size for in-memory buffers in bytes
            temp_dir: Directory for temporary files (if needed)
        """
        if max_buffer_size is not None:
            self._max_buffer_size = max_buffer_size
        if temp_dir is not None:
            self._temp_dir = temp_dir
            os.makedirs(temp_dir, exist_ok=True)
    
    def open(self, path: str, mode: str = 'r') -> Union[BinaryIO, TextIO]:
        """
        Open a file or archive entry with the specified mode.
        
        Args:
            path: Path to the file, including any archive components
            mode: File mode ('r', 'w', 'a', 'rb', 'wb', etc.)
            
        Returns:
            A file-like object with standard read/write methods
        """
        path_info = self._path_resolver.resolve(path)
        
        # Handle write modes which may need to create parent directories/archives
        if any(m in mode for m in ('w', 'a', 'x')):
            # Ensure parent directories exist
            if path_info.physical_path:
                parent_dir = os.path.dirname(path_info.physical_path)
                if parent_dir:
                    os.makedirs(parent_dir, exist_ok=True)
            
            # For archives, we may need to create them if they don't exist
            if path_info.archive_components and 'w' in mode:
                self._ensure_archives_exist(path_info)
        
        # Get the appropriate stream from the stream provider
        return self._stream_provider.get_stream(path_info, mode)
    
    def read(self, path: str, binary: bool = False) -> Union[str, bytes]:
        """
        Read entire file contents.
        
        Args:
            path: Path to the file
            binary: If True, return bytes instead of string
            
        Returns:
            File contents as string or bytes
        """
        mode = 'rb' if binary else 'r'
        with self.open(path, mode) as f:
            return f.read()
    
    def write(self, path: str, data: Union[str, bytes], binary: bool = False) -> None:
        """
        Write data to a file or archive entry.
        
        Args:
            path: Path to the file
            data: Data to write
            binary: If True, write bytes instead of string
        """
        mode = 'wb' if binary else 'w'
        with self.open(path, mode) as f:
            f.write(data)
    
    def append(self, path: str, data: Union[str, bytes], binary: bool = False) -> None:
        """
        Append data to existing file or archive entry.
        
        Args:
            path: Path to the file
            data: Data to append
            binary: If True, append bytes instead of string
        """
        mode = 'ab' if binary else 'a'
        with self.open(path, mode) as f:
            f.write(data)
    
    @contextlib.contextmanager
    def batch_session(self):
        """
        Return a session object for grouping operations.
        
        Returns:
            Session object with the same interface as ArchiveFS
        """
        from .batch_session import BatchSession
        session = BatchSession(self)
        try:
            yield session
        finally:
            session.commit()
    
    def _ensure_archives_exist(self, path_info: PathInfo, include_last: bool = True) -> None:
        """
        Ensure all archives in the path exist, creating them if necessary.
        
        Args:
            path_info: Resolved path information
            include_last: Whether to include the last component in archive creation
        """
        # No archive components, nothing to do
        if not path_info.archive_components:
            return
            
        # Build up the path components
        current_path = path_info.physical_path
        
        # Ensure the physical path exists if it's an archive
        if is_archive_format(current_path) and not os.path.exists(current_path):
            self.create_archive(current_path)
        
        # Process each archive component except the last one
        components = path_info.archive_components[:-1] if not include_last else path_info.archive_components
        for i, component in enumerate(components):
            # Build the path to this component
            if current_path:
                new_path = f"{current_path}/{component}"
            else:
                new_path = component
            
            # Check if it's an archive format
            if is_archive_format(new_path) and not self.exists(new_path):
                self.create_archive(new_path)
            
            # Update the current path
            current_path = new_path
