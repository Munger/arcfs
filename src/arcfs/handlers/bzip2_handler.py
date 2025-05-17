"""
BZIP2 handler for the Archive File System.
Provides access to BZIP2 compressed files as single-file archives.

Author: Tim Hosking
Contact: https://github.com/Munger
License: MIT
"""

import bz2
from os import SEEK_SET

from typing import Dict, List, Optional, BinaryIO, Any, Set

from arcfs.api.config_api import ConfigAPI
from arcfs.core.base_handler import ArchiveHandler, Any

class Bzip2Stream:
    """
    Stream wrapper for BZIP2 files.
    Provides a file-like interface for reading and writing BZIP2 compressed files.
    All file operations are performed via the ARCFS file API and are agnostic to how files are buffered or stored.
    """
    def __init__(self, path: str, mode: str, buffer_threshold: Optional[int] = None, handler=None):
        self.path = path
        self.mode = mode
        self.handler = handler
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
                bz2_mode = mode if 'b' in mode else mode + 'b'
                with bz2.open(path, bz2_mode) as f:
                    data = f.read()
                # Use handler.fs.files for buffer management
                self._buffer = self.handler.fs.files.open(path=None, mode='r+b', buffering=-1, encoding=None)
                self._buffer.write(data)
                self._buffer.seek(0)
            except Exception as e:
                raise IOError(f"Error opening BZIP2 file: {e}")
        else:
            # Use handler.fs.files for buffer management
            self._buffer = self.handler.fs.files.open(path=None, mode='w+b', buffering=-1, encoding=None)

    def write(self, b):
        if self._closed:
            raise ValueError("I/O operation on closed file.")
        return self._buffer.write(b)

    def read(self, size=-1):
        if self._closed:
            raise ValueError("I/O operation on closed file.")
        return self._buffer.read(size)

    def seek(self, offset, whence=SEEK_SET):
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
            # Write buffer to file
            try:
                with self.fs.files.open(path=self.path, mode='wb', buffering=-1, encoding=None) as f:
                    f.write(self._buffer.read())
            except Exception as e:
                raise IOError(f"Error writing to BZIP2 file: {e}")
        self._buffer.close()
        self._closed = True

    # Context manager support
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()


class Bzip2Config:
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


class Bzip2Handler(ArchiveHandler):
    config = Bzip2Config

    def __init__(self, path: str, mode: str = 'r', fs=None):
        if fs is None:
            raise ValueError("Bzip2Handler requires an ArchiveFS instance via the 'fs' argument.")
        self.fs = fs
        self.path = path
        self.mode = mode
        self.base_name = get_base_name(self.fs.dirs.basename(path))
        self._open()
    """
    Handler for BZIP2 compressed files, treating them as single-file archives.
    """
    
    def __init__(self, path: str, mode: str = 'r', fs=None):
        """
        Initialize the BZIP2 handler.

        Args:
            path: Path to the BZIP2 file
            mode: Access mode
            fs: ArchiveFS instance (required)
        """
        if fs is None:
            raise ValueError("Bzip2Handler requires an ArchiveFS instance via the 'fs' argument.")
        self.fs = fs
        self.path = path
        self.mode = mode
        self.base_name = get_base_name(self.fs.dirs.basename(path))
        self._open()
    
    def _open(self) -> None:
        """Open the BZIP2 file."""
        # For BZIP2 files, we don't need to keep the file open
        # We'll open it when needed
        pass
    
    def close(self) -> None:
        """Close the BZIP2 file."""
        # Nothing to do for BZIP2 files
        pass
    
    def list_streams(self) -> List[Any]:
        """
        List all streams in the BZIP2.
        
        Returns:
            List of Any objects
        """
        # BZIP2 files only have one stream
        if not self.fs.files.exists(self.path):
            return []
            
        try:
            # Get file stats
            stat = self.fs.files.stat(self.path)
            
            # Create stream object for the single file
            stream = Any(
                path=self.base_name,
                size=stat.st_size,  # Compressed size, not original size
                modified=stat.st_mtime,
                is_dir=False
            )
            
            return [stream]
        except Exception as e:
            raise IOError(f"Error listing BZIP2 streams: {e}")
    
    def list_dir(self, path: str) -> List[str]:
        """
        List contents of a directory in the BZIP2.
        
        Args:
            path: Directory path within the BZIP2
            
        Returns:
            List of stream names in the directory
        """
        # BZIP2 files don't have directories
        if path:
            return []
            
        # Root directory contains just the single file
        if self.fs.files.exists(self.path):
            return [self.base_name]
        return []
    
    def open_stream(self, path: str, mode: str = 'r') -> BinaryIO:
        """
        Open an stream for reading or writing.
        
        Args:
            path: Stream path within the BZIP2
            mode: Access mode
            
        Returns:
            File-like object for the stream
        """
        # Check if the path matches the base name
        if path and path != self.base_name:
            raise FileNotFoundError(f"Stream not found in BZIP2: {path}")
            
        # Open the BZIP2 file
        return Bzip2Stream(self.path, mode, handler=self, fs=self.fs)
    
    def get_stream_info(self, path: str) -> Optional[Dict[str, Any]]:
        """
        Get information about an stream.
        
        Args:
            path: Stream path within the BZIP2
            
        Returns:
            Dictionary with stream information, or None if stream doesn't exist
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
            raise IOError(f"Error getting BZIP2 stream info: {e}")
    
    def stream_exists(self, path: str) -> bool:
        """
        Check if an stream exists in the BZIP2.
        
        Args:
            path: Stream path within the BZIP2
            
        Returns:
            True if the stream exists, False otherwise
        """
        # Root directory always exists if the file exists
        if not path:
            return self.fs.files.exists(self.path)
            
        # Check if the path matches the base name and the file exists
        return path == self.base_name and self.fs.files.exists(self.path)
    
    def create_dir(self, path: str) -> None:
        """
        Create a directory in the BZIP2.
        
        Args:
            path: Directory path to create
        """
        # BZIP2 files don't support directories
        raise NotImplementedError("BZIP2 files do not support directories")
    
    def remove_stream(self, path: str) -> None:
        """
        Remove an stream from the BZIP2.
        
        Args:
            path: Stream path to remove
        """
        # Check if the path matches the base name
        if path != self.base_name:
            raise FileNotFoundError(f"Stream not found in BZIP2: {path}")
            
        # For a BZIP2 file, removing the stream means removing the whole file
        if self.fs.files.exists(self.path):
            try:
                self.fs.files.remove(self.path)
            except Exception as e:
                raise IOError(f"Error removing BZIP2 file: {e}")
    
    @classmethod
    def create_empty(cls, path: str, fs=None) -> None:
        """
        Create a new empty BZIP2 file.
        Args:
            path: Path where the BZIP2 should be created
            fs: ArchiveFS instance (for API access)
        """
        import bz2
        try:
            parent_dir = fs.dirs.dirname(path)
            if parent_dir and not fs.dirs.exists(parent_dir):
                fs.dirs.mkdir(parent_dir, create_parents=True)
            with bz2.open(path, 'wb'):
                pass  # Just create the file
        except Exception as e:
            raise IOError(f"Error creating empty BZIP2 file: {e}")
    
    @classmethod
    def get_supported_extensions(cls) -> Set[str]:
        """
        Get the file extensions supported by this handler.
        
        Returns:
            Set of supported extensions (with leading dot)
        """
        return {'.bz2'}
        if self._buffer_size is not None:
            return self._buffer_size
        try:
            return getattr(ConfigAPI, 'get_buffer_threshold', lambda: 64*1024)()
        except Exception:
            return 64 * 1024

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