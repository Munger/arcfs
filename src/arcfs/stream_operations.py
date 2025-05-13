"""
Stream operations for the Archive File System.

A Python library that allows working with archives as if they were directories,
providing transparent access to files within archives.

Author: Tim Hosking
Contact: https://github.com/Munger
License: MIT
"""

from typing import Union, BinaryIO, TextIO


class StreamOperations:
    """
    Implementation of stream operations for ArchiveFS.
    This class is not meant to be used directly, but through ArchiveFS.
    """
    
    def open_stream(self, path: str, mode: str = 'r') -> Union[BinaryIO, TextIO]:
        """
        Return a file-like object optimized for streaming.
        
        Args:
            path: Path to the file
            mode: File mode ('r', 'w', 'a', 'rb', 'wb', etc.)
            
        Returns:
            Stream object with file-like interface
        """
        # Currently, this is the same as open() since all operations are streaming-based
        return self.open(path, mode)
    
    def pipe(self, src_path: str, dst_path: str, buffer_size: int = 8192) -> None:
        """
        Stream data from source to destination.
        
        Args:
            src_path: Source path
            dst_path: Destination path
            buffer_size: Size of buffer for streaming (in bytes)
        """
        # Open the source and destination files
        try:
            # First try as regular files
            if os.path.isfile(src_path):
                # Source is a regular file
                with open(src_path, 'rb') as src:
                    if os.path.exists(dst_path) and not os.path.isdir(dst_path):
                        # Destination is also a regular file
                        with open(dst_path, 'wb') as dst:
                            self._pipe_streams(src, dst, buffer_size)
                        return
                    
                    # Destination might be an archive path
                    with self.open(dst_path, 'wb') as dst:
                        self._pipe_streams(src, dst, buffer_size)
                return
                
            # Source might be an archive path
            with self.open(src_path, 'rb') as src:
                with self.open(dst_path, 'wb') as dst:
                    self._pipe_streams(src, dst, buffer_size)
                    
        except (FileNotFoundError, ValueError) as e:
            # Fallback - ensure parent directories exist
            src_dir = os.path.dirname(src_path)
            dst_dir = os.path.dirname(dst_path)
            
            if src_dir and not os.path.exists(src_dir):
                os.makedirs(src_dir, exist_ok=True)
            if dst_dir and not os.path.exists(dst_dir):
                os.makedirs(dst_dir, exist_ok=True)
                
            # Try again with regular file operations
            with open(src_path, 'rb') as src, open(dst_path, 'wb') as dst:
                self._pipe_streams(src, dst, buffer_size)
    
    def _pipe_streams(self, src, dst, buffer_size: int = 8192) -> None:
        """Helper function to pipe data between two file-like objects."""
        while True:
            chunk = src.read(buffer_size)
            if not chunk:
                break
            dst.write(chunk)

# Required for fallback case in pipe()
import os