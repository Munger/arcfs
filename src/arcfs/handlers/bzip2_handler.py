"""
BZIP2 handler for the Archive File System.
Provides access to BZIP2 compressed files as single-file archives.

Author: Tim Hosking
Contact: https://github.com/Munger
License: MIT
"""

import os
import io
import bz2
from typing import Dict, List, Optional, BinaryIO, Any, Set

from ..archive_handlers import ArchiveHandler, ArchiveEntry
from ..utils import get_base_name


class Bzip2EntryStream(io.BufferedIOBase):
    """
    Stream wrapper for BZIP2 files.
    Handles reading and writing to BZIP2 compressed files.
    """
    
    def __init__(self, path: str, mode: str):
        """
        Initialize a BZIP2 entry stream.
        
        Args:
            path: Path to the BZIP2 file
            mode: Access mode
        """
        self.path = path
        self.mode = mode
        self.closed = False
        
        # Convert mode to bz2 mode
        bz2_mode = mode
        if 'b' not in bz2_mode:
            bz2_mode += 'b'  # Make sure we're in binary mode
        
        # Open the bz2 file
        self.bz2_file = bz2.open(path, bz2_mode)
    
    def read(self, size: int = -1) -> bytes:
        """Read from the stream."""
        return self.bz2_file.read(size)
    
    def write(self, data: bytes) -> int:
        """Write to the stream."""
        return self.bz2_file.write(data)
    
    def close(self):
        """Close the stream."""
        if self.closed:
            return
            
        self.bz2_file.close()
        self.closed = True
    
    def readable(self) -> bool:
        """Check if the stream is readable."""
        return 'r' in self.mode
    
    def writable(self) -> bool:
        """Check if the stream is writable."""
        return 'w' in self.mode or 'a' in self.mode
    
    def seekable(self) -> bool:
        """Check if the stream is seekable."""
        return False  # BZIP2 streams are not seekable in general


class Bzip2Handler(ArchiveHandler):
    """
    Handler for BZIP2 compressed files, treating them as single-file archives.
    """
    
    def __init__(self, path: str, mode: str = 'r'):
        """
        Initialize the BZIP2 handler.
        
        Args:
            path: Path to the BZIP2 file
            mode: Access mode
        """
        self.path = path
        self.mode = mode
        self.base_name = get_base_name(os.path.basename(path))
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
    
    def list_entries(self) -> List[ArchiveEntry]:
        """
        List all entries in the BZIP2.
        
        Returns:
            List of ArchiveEntry objects
        """
        # BZIP2 files only have one entry
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
        List contents of a directory in the BZIP2.
        
        Args:
            path: Directory path within the BZIP2
            
        Returns:
            List of entry names in the directory
        """
        # BZIP2 files don't have directories
        if path:
            return []
            
        # Root directory contains just the single file
        return [self.base_name]
    
    def open_entry(self, path: str, mode: str = 'r') -> BinaryIO:
        """
        Open an entry for reading or writing.
        
        Args:
            path: Entry path within the BZIP2
            mode: Access mode
            
        Returns:
            File-like object for the entry
        """
        # Check if the path matches the base name
        if path and path != self.base_name:
            raise FileNotFoundError(f"Entry not found in BZIP2: {path}")
            
        # Open the BZIP2 file
        return Bzip2EntryStream(self.path, mode)
    
    def get_entry_info(self, path: str) -> Optional[Dict[str, Any]]:
        """
        Get information about an entry.
        
        Args:
            path: Entry path within the BZIP2
            
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
        Check if an entry exists in the BZIP2.
        
        Args:
            path: Entry path within the BZIP2
            
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
        Create a directory in the BZIP2.
        
        Args:
            path: Directory path to create
        """
        # BZIP2 files don't support directories
        raise NotImplementedError("BZIP2 files do not support directories")
    
    def remove_entry(self, path: str) -> None:
        """
        Remove an entry from the BZIP2.
        
        Args:
            path: Entry path to remove
        """
        # Check if the path matches the base name
        if path != self.base_name:
            raise FileNotFoundError(f"Entry not found in BZIP2: {path}")
            
        # For a BZIP2 file, removing the entry means removing the whole file
        if os.path.exists(self.path):
            os.remove(self.path)
    
    @classmethod
    def create_empty(cls, path: str) -> None:
        """
        Create a new empty BZIP2 file.
        
        Args:
            path: Path where the BZIP2 should be created
        """
        # Create parent directory if it doesn't exist
        parent_dir = os.path.dirname(path)
        if parent_dir and not os.path.exists(parent_dir):
            os.makedirs(parent_dir, exist_ok=True)
        
        # Create an empty BZIP2 file
        with bz2.open(path, 'wb'):
            pass  # Just create the file
    
    @classmethod
    def get_supported_extensions(cls) -> Set[str]:
        """
        Get the file extensions supported by this handler.
        
        Returns:
            Set of supported extensions (with leading dot)
        """
        return {'.bz2'}