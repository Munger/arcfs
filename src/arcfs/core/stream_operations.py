"""
Stream operations for the Archive File System.

A Python library that allows working with archives as if they were directories,
providing transparent access to files within archives.

Author: Tim Hosking
Contact: https://github.com/Munger
License: MIT
"""

import os
import shutil
from .utils import is_archive_format
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
        try:
            # Check if both paths exist and are regular files
            if os.path.isfile(src_path) and os.path.exists(os.path.dirname(dst_path)) and not self.is_archive_path(dst_path):
                # Make sure the destination directory exists
                dst_dir = os.path.dirname(dst_path)
                if dst_dir:
                    from .directory_operations import DirectoryOperations
                    try:
                        from arcfs.api.config_api import ConfigAPI
                        ConfigAPI().set('debug_level', 2)  # or use logging as appropriate(f"Creating directory: {dst_dir}", level=2)
                    except ImportError:
                        print(f"[DEBUG] Creating directory: {dst_dir}")
                    DirectoryOperations().mkdir(dst_dir, create_parents=True)
                
                # Use shutil.copyfile for regular files (much more efficient)
                shutil.copyfile(src_path, dst_path)
                try:
                    from arcfs.api.config_api import ConfigAPI
                    ConfigAPI().set('debug_level', 2)  # or use logging as appropriate(f"Copied file from {src_path} to {dst_path}", level=2)
                except ImportError:
                    print(f"[DEBUG] Copied file from {src_path} to {dst_path}")
                return
                
            # Otherwise, use our stream-based approach
            with self.open(src_path, 'rb') as src, self.open(dst_path, 'wb') as dst:
                shutil.copyfileobj(src, dst, buffer_size)
        
        except Exception as e:
            raise IOError(f"Error piping data from '{src_path}' to '{dst_path}': {e}")
    
    def pipe_string(self, data: str, dst_path: str) -> None:
        """
        Stream string data to a destination file.
        
        Args:
            data: String data to write
            dst_path: Destination path
        """
        try:
            with self.open(dst_path, 'w') as dst:
                dst.write(data)
        except Exception as e:
            raise IOError(f"Error writing string data to '{dst_path}': {e}")
    
    def pipe_bytes(self, data: bytes, dst_path: str) -> None:
        """
        Stream binary data to a destination file.
        
        Args:
            data: Binary data to write
            dst_path: Destination path
        """
        try:
            with self.open(dst_path, 'wb') as dst:
                dst.write(data)
        except Exception as e:
            raise IOError(f"Error writing binary data to '{dst_path}': {e}")
    
    def cat(self, path: str, encoding: str = 'utf-8') -> str:
        """
        Read and return the contents of a file as a string.
        Similar to read() but optimized for text files.
        
        Args:
            path: Path to the file
            encoding: Text encoding to use
            
        Returns:
            File contents as a string
        """
        try:
            with self.open(path, 'r', encoding=encoding) as f:
                return f.read()
        except Exception as e:
            raise IOError(f"Error reading file '{path}': {e}")