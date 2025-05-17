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
from arcfs.api.config_api import ConfigAPI
from arcfs.api.dirs_api import DirsAPI


class StreamProvider:
    """
    Provides appropriate streams for different types of paths.
    Handles the creation of file objects for regular files, archive entries, and compressed files.
    """
    
    def __init__(self):
        """Initialize the stream provider."""
        self._opened_archives: Dict[str, ArchiveHandler] = {}
    
    def get_stream(self, path_info: PathInfo, mode: str, encoding: str = 'utf-8') -> Union[BinaryIO, TextIO]:
        """
        Get an appropriate stream for the given path info and mode.
        
        Args:
            path_info: Resolved path information
            mode: File mode ('r', 'w', 'a', 'rb', 'wb', etc.)
            encoding: Text encoding to use (for text modes)
            
        Returns:
            A file-like object appropriate for the path type
        """
        # Determine if we're dealing with text or binary mode
        is_binary = 'b' in mode
        
        # If there are no archive components, it's a regular file
        if not path_info.archive_components:
            physical_path = path_info.physical_path
            if 'w' in mode or 'a' in mode:
                parent_dir = os.path.dirname(physical_path)
                if parent_dir:
                    DirsAPI().mkdir(parent_dir, create_parents=True)
            handler_cls = get_handler_for_path(physical_path)
            if handler_cls and os.path.exists(physical_path) and not is_archive_format(physical_path):
                with handler_cls(physical_path, mode) as handler:
                    binary_stream = handler.open_entry("", mode)
                    return binary_stream if is_binary else io.TextIOWrapper(binary_stream, encoding=encoding)
            if is_binary:
                return open(physical_path, mode)
            return open(physical_path, mode, encoding=encoding)
        
        # We're dealing with an archive entry
        try:
            with self.get_archive_handler(path_info, mode) as handler:
                entry_path = path_info.get_entry_path()
                handler_mode = mode
                if not is_binary:
                    mode_map = {'r': 'rb', 'w': 'wb', 'a': 'ab'}
                    handler_mode = next((v for k, v in mode_map.items() if k in mode), None)
                    if handler_mode is None:
                        debug_print(f"Unsupported mode: {mode}", level=1)
                        raise ValueError(f"Unsupported mode: {mode}")
                binary_stream = handler.open_entry(entry_path, handler_mode)
                return binary_stream if is_binary else io.TextIOWrapper(binary_stream, encoding=encoding)
        except Exception as e:
            debug_print(f"Exception in StreamProvider.get_stream: {e}", level=1)
            raise IOError(f"Error accessing archive entry: {e}")
    
    @contextmanager
    def get_archive_handler(self, path_info: PathInfo, mode: str = 'r') -> ArchiveHandler:
        """
        Get a handler for an archive.
        
        Args:
            path_info: Path information for the archive
            mode: Access mode for the archive
            
        Returns:
            An archive handler appropriate for the archive type
            
        Raises:
            ValueError: If no handler is available for the archive type
            FileNotFoundError: If the archive doesn't exist in read mode
        """
        # Get the physical path to the archive
        archive_path = path_info.physical_path
        
        # Determine the appropriate mode based on the existence of the archive
        handler_mode = mode
        if not os.path.exists(archive_path) and path_info.archive_components:
            if 'r' in mode and not any(m in mode for m in ('w', 'a', 'x', '+')):
                raise FileNotFoundError(f"Archive does not exist: {archive_path}")
            handler_mode = handler_mode.replace('r', 'w') if 'r' in handler_mode and not any(m in handler_mode for m in ('w', 'a', 'x', '+')) else handler_mode
            parent_dir = os.path.dirname(archive_path)
            if parent_dir:
                DirsAPI().mkdir(parent_dir, create_parents=True)
        
        # Get the appropriate handler class for the archive
        handler_cls = get_handler_for_path(archive_path)
        if not handler_cls:
            from arcfs.core.logging import debug_print
            debug_print(f"No handler available for archive: {archive_path}", level=1)
            raise ValueError(f"No handler available for archive: {archive_path}")
        
        # Create and yield the handler
        handler = handler_cls(archive_path, handler_mode)
        try:
            yield handler
        finally:
            handler.close()