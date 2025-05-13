"""
Batch session handling for the Archive File System.
Provides a way to group multiple operations on archives for better efficiency.

Author: Tim Hosking
Contact: https://github.com/Munger
License: MIT
"""

from typing import Dict, Set, Any, Optional
import os
from collections import defaultdict

from .path_resolver import PathResolver
from .utils import is_archive_format


class BatchSession:
    """
    Session object for grouping operations on archives.
    Allows multiple operations on the same archive to be batched together
    for better performance, rebuilding the archive only once per session.
    """
    
    def __init__(self, archive_fs):
        """
        Initialize a batch session.
        
        Args:
            archive_fs: The parent ArchiveFS instance
        """
        self._fs = archive_fs
        self._path_resolver = PathResolver()
        self._modified_archives: Set[str] = set()
        self._archive_operations: Dict[str, Dict[str, Any]] = defaultdict(dict)
    
    def __getattr__(self, name: str) -> Any:
        """
        Proxy attribute access to the parent ArchiveFS instance.
        
        Args:
            name: Attribute name
            
        Returns:
            The attribute from the parent ArchiveFS
        """
        # Get the attribute from the parent ArchiveFS
        attr = getattr(self._fs, name)
        
        # If it's a method, wrap it to track modifications
        if callable(attr):
            def wrapped_method(*args, **kwargs):
                # Call the original method
                result = attr(*args, **kwargs)
                
                # Track if an archive was modified
                if name in ('write', 'append', 'remove', 'mkdir', 'rmdir'):
                    # The first argument is typically the path
                    if args and isinstance(args[0], str):
                        self._track_modification(args[0])
                
                return result
            
            return wrapped_method
        
        # Otherwise, just return the attribute
        return attr
    
    def _track_modification(self, path: str) -> None:
        """
        Track that an archive was modified.
        
        Args:
            path: Path that was modified
        """
        # Resolve the path to get the archive components
        path_info = self._path_resolver.resolve(path)
        
        # If the path has archive components, track the parent archive
        if path_info.archive_components and path_info.physical_path:
            self._modified_archives.add(path_info.physical_path)
    
    def commit(self) -> None:
        """
        Commit all pending changes.
        This method is called automatically when the session is exited.
        """
        # Currently, there's no special handling needed for commit,
        # as all operations are applied immediately.
        # In a more advanced implementation, this could buffer changes
        # and apply them all at once for better performance.
        pass
    
    def open(self, path, mode='r'):
        """
        Open a file with tracking of modifications.
        
        Args:
            path: Path to the file
            mode: File mode
            
        Returns:
            A file object
        """
        # If opening for writing, track the modification
        if any(m in mode for m in ('w', 'a', 'x', '+')):
            self._track_modification(path)
            
        # Delegate to the parent FS
        return self._fs.open(path, mode)
    
    def write(self, path, data, binary=False):
        """
        Write data to a file, tracking the modification.
        
        Args:
            path: Path to the file
            data: Data to write
            binary: Whether data is binary
        """
        # Track the modification
        self._track_modification(path)
        
        # Delegate to the parent FS
        return self._fs.write(path, data, binary)
    
    def append(self, path, data, binary=False):
        """
        Append data to a file, tracking the modification.
        
        Args:
            path: Path to the file
            data: Data to append
            binary: Whether data is binary
        """
        # Track the modification
        self._track_modification(path)
        
        # Delegate to the parent FS
        return self._fs.append(path, data, binary)