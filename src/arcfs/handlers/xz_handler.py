"""
XZ handler for the Archive File System.
Provides access to XZ compressed files as single-file archives.

Author: Tim Hosking
Contact: https://github.com/Munger
License: MIT
"""

import os
import io
import lzma
from typing import Dict, List, Optional, BinaryIO, Any, Set

from ..archive_handlers import ArchiveHandler, ArchiveEntry
from ..utils import get_base_name


class XzEntryStream(io.BufferedIOBase):
    """
    Stream wrapper for XZ files.
    Handles reading and writing to XZ compressed files.
    """
    
    def __init__(self, path: str, mode: str):
        """
        Initialize an XZ entry stream.
        
        Args:
            path: Path to the XZ file
            mode: Access mode
        """
        self.path = path
        self.mode = mode
        self.closed = False
        
        # Convert mode to lzma mode
        lzma_mode = mode
        if 'b' not in lzma_mode:
            lzma_mode += 'b'  # Make sure we're in binary mode
        
        # Open the lzma file
        self.lzma_file = lzma.open(path, lzma_mode)
    
    def read(self, size: int = -1) -> bytes:
        """Read from the stream."""
        return self.lzma_file.read(size)
    
    def write(self, data: bytes) -> int:
        """Write to the stream."""
        return self.lzma_file.write(data)
    
    def close(self):
        """Close the stream."""
        if self.closed:
            return
            
        self.lzma_file.close()
        self.closed = True
    
    def readable(self) -> bool:
        """Check if the stream is readable."""
        return 'r' in self.mode
    
    def writable(self) -> bool:
        """Check if the stream is writable."""
        return 'w' in self.mode or 'a' in self.mode
    
    def seekable(self) -> bool:
        """Check if the stream is seekable."""
        return False  # XZ streams are not seekable in general


class XzHandler(ArchiveHandler):
    """
    Handler for XZ compressed files, treating them as single-file archives.
    """
    
    def __init__(self, path: str, mode: str = 'r'):
        """
        Initialize the XZ handler.
        
        Args:
            path: Path to the XZ file
            mode: Access mode
        """
        self.path = path
        self.mode = mode
        self.base_name = get_base_name(os.path.basename(path))
        self._open()
    
    def _open(self) -> None:
        """Open the XZ file."""
        # For XZ files, we don't need to keep the file open
        # We'll open it when needed
        pass
    
    def close(self) -> None:
        """Close the XZ file."""
        # Nothing to do for XZ files
        pass
    
    def list_entries(self) -> List[ArchiveEntry]:
        """
        List all entries in the XZ.
        
        Returns:
            List of ArchiveEntry objects
        """
        # XZ files only have one entry
        if not os.path.exists(self.path):
            return []
            
        # Get file stats
        stat = os.stat(self.path)
        
        # Create entry object for the single file
        entry = ArchiveEntry(
            path=self.base_name,
            size=stat.st_size,  # Compressed size, not original size
            modified=stat.st_mtime,
            is_dir=False
        )
        
        return [entry]
    
    def list_dir(self, path: str) -> List[str]:
        """
        List contents of a directory in the XZ.
        
        Args:
            path: Directory path within the XZ
            
        Returns:
            List of entry names in the directory
        """
        # XZ files don't have directories
        if path:
            return []
            
        # Root directory contains just the single file
        return [self.base_name]
    
    def open_entry(self, path: str, mode: str = 'r') -> BinaryIO:
        """
        Open an entry for reading or writing.
        
        Args:
            path: Entry path within the XZ
            mode: Access mode
            
        Returns:
            File-like object for the entry
        """
        # Check if the path matches the base name
        if path and path != self.base_name:
            raise FileNotFoundError(f"Entry not found in XZ: {path}")
            
        # Open the XZ file
        return XzEntryStream(self.path, mode)
    
    def get_entry_info(self, path: str) -> Optional[Dict[str, Any]]:
        """
        Get information about an entry.
        
        Args:
            path: Entry path within the XZ
            
        Returns:
            Dictionary with entry information, or None if entry doesn't exist
        """
        # Check if the file exists
        if not os.path.exists(self.path):
            return None
            
        # Root directory
        if not path:
            return {
                'size': 0,
                'modified': os.path.getmtime(self.path),
                'is_dir': True,
                'path': ''
            }
            
        # Check if the path matches the base name
        if path != self.base_name:
            return None
            
        # Get file stats
        stat = os.stat(self.path)
        
        # Return info for the single file
        return {
            'size': stat.st_size,  # Compressed size, not original size
            'modified': stat.st_mtime,
            'is_dir': False,
            'path': path
        }
    
    def entry_exists(self, path: str) -> bool:
        """
        Check if an entry exists in the XZ.
        
        Args:
            path: Entry path within the XZ
            
        Returns:
            True if the entry exists, False otherwise
        """
        # Root directory always exists if the file exists
        if not path:
            return os.path.exists(self.path)
            
        # Check if the path matches the base name and the file exists
        return path == self.base_name and os.path.exists(self.path)
    
    def create_dir(self, path: str) -> None:
        """
        Create a directory in the XZ.
        
        Args:
            path: Directory path to create
        """
        # XZ files don't support directories
        raise NotImplementedError("XZ files do not support directories")
    
    def remove_entry(self, path: str) -> None:
        """
        Remove an entry from the XZ.
        
        Args:
            path: Entry path to remove
        """
        # Check if the path matches the base name
        if path != self.base_name:
            raise FileNotFoundError(f"Entry not found in XZ: {path}")
            
        # For an XZ file, removing the entry means removing the whole file
        if os.path.exists(self.path):
            os.remove(self.path)
    
    @classmethod
    def create_empty(cls, path: str) -> None:
        """
        Create a new empty XZ file.
        
        Args:
            path: Path where the XZ should be created
        """
        # Create parent directory if it doesn't exist
        parent_dir = os.path.dirname(path)
        if parent_dir and not os.path.exists(parent_dir):
            os.makedirs(parent_dir, exist_ok=True)
        
        # Create an empty XZ file
        with lzma.open(path, 'wb'):
            pass  # Just create the file
    
    @classmethod
    def get_supported_extensions(cls) -> Set[str]:
        """
        Get the file extensions supported by this handler.
        
        Returns:
            Set of supported extensions (with leading dot)
        """
        return {'.xz'}