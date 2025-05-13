"""
Base handler for archive types.
Defines the interfaces that all archive handlers must implement.

Author: Tim Hosking
Contact: https://github.com/Munger
License: MIT
"""

from typing import Dict, Optional, List, Any, BinaryIO, Type, NamedTuple, Set
from abc import ABC, abstractmethod


class ArchiveEntry(NamedTuple):
    """Information about an entry in an archive."""
    path: str
    size: int
    modified: float
    is_dir: bool


class ArchiveHandler(ABC):
    """
    Base class for archive format handlers.
    Defines the interface that all archive handlers must implement.
    """
    
    def __init__(self, path: str, mode: str = 'r'):
        """
        Initialize the archive handler.
        
        Args:
            path: Path to the archive
            mode: Access mode ('r' for read, 'w' for write, 'a' for append)
        """
        self.path = path
        self.mode = mode
        self._open()
    
    @abstractmethod
    def _open(self) -> None:
        """Open the archive for access."""
        pass
    
    @abstractmethod
    def close(self) -> None:
        """Close the archive, releasing any resources."""
        pass
    
    def __enter__(self):
        """Enter context manager."""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Exit context manager, closing the archive."""
        self.close()
    
    @abstractmethod
    def list_entries(self) -> List[ArchiveEntry]:
        """
        List all entries in the archive.
        
        Returns:
            List of ArchiveEntry objects
        """
        pass
    
    @abstractmethod
    def list_dir(self, path: str) -> List[str]:
        """
        List contents of a directory in the archive.
        
        Args:
            path: Directory path within the archive
            
        Returns:
            List of entry names in the directory
        """
        pass
    
    @abstractmethod
    def open_entry(self, path: str, mode: str = 'r') -> BinaryIO:
        """
        Open an entry for reading or writing.
        
        Args:
            path: Entry path within the archive
            mode: Access mode
            
        Returns:
            File-like object for the entry
        """
        pass
    
    @abstractmethod
    def get_entry_info(self, path: str) -> Optional[Dict[str, Any]]:
        """
        Get information about an entry.
        
        Args:
            path: Entry path within the archive
            
        Returns:
            Dictionary with entry information, or None if entry doesn't exist
        """
        pass
    
    @abstractmethod
    def entry_exists(self, path: str) -> bool:
        """
        Check if an entry exists in the archive.
        
        Args:
            path: Entry path within the archive
            
        Returns:
            True if the entry exists, False otherwise
        """
        pass
    
    @abstractmethod
    def create_dir(self, path: str) -> None:
        """
        Create a directory in the archive.
        
        Args:
            path: Directory path to create
        """
        pass
    
    @abstractmethod
    def remove_entry(self, path: str) -> None:
        """
        Remove an entry from the archive.
        
        Args:
            path: Entry path to remove
        """
        pass
    
    @classmethod
    @abstractmethod
    def create_empty(cls, path: str) -> None:
        """
        Create a new empty archive.
        
        Args:
            path: Path where the archive should be created
        """
        pass
    
    @classmethod
    @abstractmethod
    def get_supported_extensions(cls) -> Set[str]:
        """
        Get the file extensions supported by this handler.
        
        Returns:
            Set of supported extensions (with leading dot)
        """
        pass
