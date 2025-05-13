"""
Stream provider for the Archive File System.
Handles the creation of appropriate streams for different types of paths.

Author: Tim Hosking
Contact: https://github.com/Munger
License: MIT
"""

import os
import io
from typing import Union, BinaryIO, TextIO, Any, Dict, Optional
from contextlib import contextmanager

from .path_resolver import PathInfo
from .utils import is_archive_format
from .archive_handlers import get_handler_for_path, ArchiveHandler


class StreamProvider:
    """
    Provides appropriate streams for different types of paths.
    Handles the creation of file objects for regular files, archive entries, and compressed files.
    """
    
    def __init__(self):
        """Initialize the stream provider."""
        self._opened_archives: Dict[str, ArchiveHandler] = {}
    
    def get_stream(self, path_info: PathInfo, mode: str) -> Union[BinaryIO, TextIO]:
        """
        Get an appropriate stream for the given path info and mode.
        
        Args:
            path_info: Resolved path information
            mode: File mode ('r', 'w', 'a', 'rb', 'wb', etc.)
            
        Returns:
            A file-like object appropriate for the path type
        """
        # Determine if we're dealing with text or binary mode
        is_binary = 'b' in mode
        
        # If there are no archive components, it's a regular file
        if not path_info.archive_components:
            # Make sure the parent directory exists if writing
            if 'w' in mode or 'a' in mode:
                parent_dir = os.path.dirname(path_info.physical_path)
                if parent_dir:
                    os.makedirs(parent_dir, exist_ok=True)
                    
            # Open the file with the requested mode
            if is_binary:
                return open(path_info.physical_path, mode)
            else:
                return open(path_info.physical_path, mode, encoding='utf-8')
        
        # We're dealing with an archive entry
        try:
            with self.get_archive_handler(path_info) as handler:
                # Get a binary stream for the entry
                binary_stream = handler.open_entry(path_info.get_entry_path(), mode)
                
                # If binary mode was requested, return the raw stream
                if is_binary:
                    return binary_stream
                
                # Otherwise, wrap it in a text stream
                return io.TextIOWrapper(binary_stream, encoding='utf-8')
        except ValueError as e:
            # Special case: if we have a regular file but the path looks like an archive
            if "No handler available for archive" in str(e) and 'r' in mode:
                if os.path.isfile(path_info.physical_path):
                    # It's a regular file, just open it
                    if is_binary:
                        return open(path_info.physical_path, mode)
                    else:
                        return open(path_info.physical_path, mode, encoding='utf-8')
            
            # Otherwise, re-raise the error
            raise
    
    @contextmanager
    def get_archive_handler(self, path_info: PathInfo) -> ArchiveHandler:
        """
        Get a handler for an archive.
        
        Args:
            path_info: Path information for the archive
            
        Returns:
            An archive handler appropriate for the archive type
            
        Raises:
            ValueError: If no handler is available for the archive type
            FileNotFoundError: If the archive doesn't exist in read mode
        """
        # Get the physical path to the archive
        archive_path = path_info.physical_path
        
        # Determine the appropriate mode based on the existence of the archive
        mode = 'r'  # Default to read mode
        if not os.path.exists(archive_path) and path_info.archive_components:
            # Archive doesn't exist but we're trying to access an entry inside it
            # This implies we need to create the archive
            mode = 'w'
            
            # Make sure the parent directory exists
            parent_dir = os.path.dirname(archive_path)
            if parent_dir:
                os.makedirs(parent_dir, exist_ok=True)
        
        # Get the appropriate handler class for the archive
        handler_cls = get_handler_for_path(archive_path)
        if not handler_cls:
            raise ValueError(f"No handler available for archive: {archive_path}")
        
        # Create and yield the handler
        handler = handler_cls(archive_path, mode)
        try:
            yield handler
        finally:
            handler.close()