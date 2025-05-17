"""
GZIP handler for the Archive File System.
Provides access to GZIP compressed files as single-file archives.

Author: Tim Hosking
Contact: https://github.com/Munger
License: MIT
"""

import io
import gzip
import tempfile

from typing import Dict, List, Optional, BinaryIO, Any, Set

from arcfs.api.config_api import ConfigAPI
from arcfs.core.base_handler import ArchiveHandler

class GzipStream:
    """
    Stream wrapper for GZIP files.
    Provides a file-like interface for reading and writing GZIP compressed files.
    All file operations are performed via the ARCFS file API and are agnostic to how files are buffered or stored.
    """
    def __init__(self, path: str, mode: str, buffer_threshold: Optional[int] = None):
        """
        Initialize the GZIP stream.

        Args:
            path: Path to the GZIP file
            mode: Access mode
            buffer_threshold: Optional in-memory buffer threshold (overrides global default)
        """
        self.path = path
        self.mode = mode
        # Use config or config API for buffer threshold
        if buffer_threshold is not None:
            self._buffer_threshold = buffer_threshold
        else:
            try:
                self._buffer_threshold = getattr(ConfigAPI, 'get_buffer_threshold', lambda: 64*1024)()
            except Exception:
                self._buffer_threshold = 64 * 1024
        self._closed = False
        self._write_mode = 'w' in mode or 'a' in mode
        if not self._write_mode:
            try:
                gzip_mode = mode if 'b' in mode else mode + 'b'
                with gzip.open(path, gzip_mode) as f:
                    data = f.read()
                # Use a temporary file for buffer operations, as per ARCFS handler rules
                self._buffer = self.handler.fs.files.open(path=None, mode='r+b', buffering=-1, encoding=None)
                self._buffer.write(data)
                self._buffer.seek(0)
            except Exception as e:
                raise IOError(f"Exception in GzipHandler: Error opening GZIP file: {e}")
        else:
            # Use a hybrid buffer for writing
            self._buffer = self.handler.fs.files.open(path=None, mode='w+b', buffering=-1, encoding=None)

    def write(self, b):
        if self._closed:
            raise ValueError("I/O operation on closed file.")
        return self._buffer.write(b)

    def read(self, size=-1):
        if self._closed:
            raise ValueError("I/O operation on closed file.")
        self._buffer.seek(0)
        return self._buffer.read(size)

    def seek(self, offset, whence=io.SEEK_SET):
        if self._closed:
            raise ValueError("I/O operation on closed file.")
        return self._buffer.seek(offset, whence)

    def tell(self):
        if self._closed:
            raise ValueError("I/O operation on closed file.")
        return self._buffer.tell()

    @property
    def closed(self):
        return self._closed

    def close(self):
        if self._closed:
            return
        if self._write_mode:
            # Write member_stream to file
            try:
                self._buffer.seek(0)
                with gzip.open(self.path, 'wb') as f:
                    f.write(self._buffer.read())
            except Exception as e:
                raise IOError(f"Exception in GzipHandler: Error writing to GZIP file: {e}")
        self._buffer.close()
        self._closed = True

    # Context manager support
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()


class GzipConfig:
    _overrides = {}

    @classmethod
    def set(cls, key, value):
        cls._overrides[key] = value

    @classmethod
    def get(cls, key):
        if key in cls._overrides:
            return cls._overrides[key]
        from arcfs.core.global_config import GlobalConfig
        return GlobalConfig.get(key)

    @classmethod
    def reset(cls, key=None):
        if key is None:
            cls._overrides.clear()
        else:
            cls._overrides.pop(key, None)


class GzipHandler(ArchiveHandler):
    config = GzipConfig

    # --- Required abstract methods for ArchiveHandler ---
    def stream_exists(self, arc_path: str) -> bool:
        return self.file_exists(arc_path)

    def get_stream_info(self, arc_path: str):
        return self.get_file_info(arc_path)

    def list_streams(self):
        return self.list_files()

    def open_stream(self, arc_path: str, mode: str = 'r'):
        return self.open_file(arc_path, mode)

    def remove_stream(self, arc_path: str):
        return self.remove_file(arc_path)

    def __init__(self, path: str, mode: str = 'r', fs=None):
        if fs is None:
            raise ValueError("GzipHandler requires an ArchiveFS instance via the 'fs' argument.")
        self.fs = fs
        self.path = path
        self.mode = mode
        self.base_name = self.fs.dirs.basename(path)
        self._open()
    """
    Handler for GZIP compressed files, treating them as single-file archives.
    """
    
    def __init__(self, path: str, mode: str = 'r'):
        """
        Initialize the GZIP handler.
        
        Args:
            path: Path to the GZIP file
            mode: Access mode
        """
        self.path = path
        self.mode = mode
        self.base_name = self.fs.dirs.basename(path)
        self._open()
    
    def _open(self) -> None:
        """Open the GZIP file."""
        # For GZIP files, we don't need to keep the file open
        # We'll open it when needed
        pass
    
    def close(self) -> None:
        """Close the GZIP file."""
        # Nothing to do for GZIP files
        pass
    
    def list_files(self) -> List[Dict[str, Any]]:
        """
        List all files in the GZIP.
        
        Returns:
            List of dictionaries with file information
        """
        # GZIP files only have one member
        if not self.fs.files.exists(self.path):
            return []
            
        try:
            # Get file stats
            stat = self.fs.files.stat(self.path)
            
            # Create member object for the single file
            member = {
                'path': self.base_name,
                'size': stat.st_size,  # Compressed size, not original size
                'modified': stat.st_mtime,
                'is_dir': False
            }
            
            return [member]
        except Exception as e:
            raise IOError(f"Exception in GzipHandler: Error listing GZIP files: {e}")
    
    def list_dir(self, path: str) -> List[str]:
        """
        List contents of a directory in the GZIP.
        
        Args:
            path: Directory path within the GZIP
            
        Returns:
            List of file names in the directory
        """
        # GZIP files don't have directories
        if path:
            return []
            
        # Root directory contains just the single file
        if self.fs.files.exists(self.path):
            return [self.base_name]
        return []
    
    def open_file(self, path: str, mode: str = 'r') -> BinaryIO:
        """
        Open a file for reading or writing.
        
        Args:
            path: File path within the GZIP
            mode: Access mode
            
        Returns:
            File-like object for the file
        """
        # Check if the path matches the base name
        if path and path != self.base_name:
            raise FileNotFoundError(f"File not found in GZIP: {path}")
            
        # Open the GZIP file
        return GzipStream(self.path, mode)
    
    def get_file_info(self, path: str) -> Optional[Dict[str, Any]]:
        """
        Get information about a file.
        
        Args:
            path: File path within the GZIP
            
        Returns:
            Dictionary with file information, or None if file doesn't exist
        """
        # Check if the file exists
        if not self.fs.files.exists(self.path):
            return None
            
        try:
            # Root directory
            if not path:
                return {
                    'size': 0,
                    'modified': self.fs.dirs.getmtime(self.path),
                    'is_dir': True,
                    'path': ''
                }
                
            # Check if the path matches the base name
            if path != self.base_name:
                return None
                
            # Get file stats
            stat = self.fs.files.stat(self.path)
            
            # Return info for the single file
            return {
                'size': stat.st_size,  # Compressed size, not original size
                'modified': stat.st_mtime,
                'is_dir': False,
                'path': path
            }
        except Exception as e:
            raise IOError(f"Exception in GzipHandler: Error getting GZIP file info: {e}")
    
    def file_exists(self, path: str) -> bool:
        """
        Check if a file exists in the GZIP.
        
        Args:
            path: File path within the GZIP
            
        Returns:
            True if the file exists, False otherwise
        """
        # Root directory always exists if the file exists
        if not path:
            return self.fs.files.exists(self.path)
            
        # Check if the path matches the base name and the file exists
        return path == self.base_name and self.fs.files.exists(self.path)
    
    def create_dir(self, path: str) -> None:
        """
        Create a directory in the GZIP.
        
        Args:
            path: Directory path to create
        """
        # GZIP files don't support directories
        raise NotImplementedError("GZIP files do not support directories")
    
    def remove_file(self, path: str) -> None:
        """
        Remove a file from the GZIP.
        
        Args:
            path: File path to remove
        """
        # Check if the path matches the base name
        if path != self.base_name:
            raise FileNotFoundError(f"File not found in GZIP: {path}")
            
        # For a GZIP file, removing the file means removing the whole file
        if self.fs.files.exists(self.path):
            try:
                self.fs.files.remove(self.path)
            except Exception as e:
                raise IOError(f"Exception in GzipHandler: Error removing GZIP file: {e}")
    
    @classmethod
    def create_empty(cls, path: str, fs=None) -> None:
        """
        Create a new empty GZIP file.
        
        Args:
            path: Path where the GZIP should be created
        """
        if fs is None:
            raise ValueError("ArchiveFS instance (fs) must be provided to create_empty.")
        try:
            # Create parent directory if it doesn't exist
            parent_dir = fs.dirs.dirname(path)
            if parent_dir and not fs.dirs.exists(parent_dir):
                fs.dirs.mkdir(parent_dir, create_parents=True)
            
            # Create an empty GZIP file
            with gzip.open(path, 'wb'):
                pass  # Just create the file
        except Exception as e:
            raise IOError(f"Exception in GzipHandler: Error creating empty GZIP file: {e}")
    
    @classmethod
    def get_supported_extensions(cls) -> Set[str]:
        """
        Get the file extensions supported by this handler.
        
        Returns:
            Set of supported extensions (with leading dot)
        """
        return {'.gz'}

    def __getitem__(self, key):
        return getattr(self, key)

    def __setitem__(self, key, value):
        return setattr(self, key, value)

    def __getattr__(self, key):
        if key in self.__dict__:
            return self.__dict__[key]
        raise AttributeError(key)

    def __setattr__(self, key, value):
        super().__setattr__(key, value)