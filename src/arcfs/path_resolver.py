"""
Path resolution functionality for the Archive File System.
Handles parsing and analyzing paths to identify archive components.

Author: Tim Hosking
Contact: https://github.com/Munger
License: MIT
"""

from typing import List, Optional, NamedTuple
import os
from pathlib import Path

from .utils import is_archive_format


class PathInfo(NamedTuple):
    """Information about a resolved path with archive components."""
    original_path: str
    physical_path: str
    archive_components: List[str]
    
    def get_entry_path(self) -> str:
        """
        Get the entry path within the archive.
        
        Returns:
            Path string relative to the archive root
        """
        if not self.archive_components:
            return ""
        return '/'.join(self.archive_components)


class PathResolver:
    """
    Resolves paths containing archive components.
    Parses paths like 'archive.tar.gz/dir/file.txt' to identify
    the physical path and virtual components within archives.
    """
    
    def resolve(self, path: str) -> PathInfo:
        """
        Resolve a path containing archive components.
        
        Args:
            path: Path string that may contain archive components
            
        Returns:
            PathInfo with the resolved components
        """
        if not path:
            raise ValueError("Path cannot be empty")
        
        # Normalize path separators
        path = path.replace('\\', '/')
        
        # Split the path into components
        components = path.split('/')
        
        # Find the physical path (everything up to the first archive)
        physical_path = ""
        archive_start_index = 0
        
        # Handle absolute paths
        if path.startswith('/'):
            physical_path = "/"
            
        # Find the first archive component
        current_path = ""
        for i, component in enumerate(components):
            if not component:  # Skip empty components
                continue
                
            # Update the current path
            if current_path:
                current_path = f"{current_path}/{component}"
            else:
                current_path = component
                
            # Check if this component is an archive
            if is_archive_format(component):
                # We found the start of archive components
                archive_start_index = i
                physical_path = current_path
                break
                
            # If we reach the end without finding an archive, it's a regular path
            if i == len(components) - 1:
                physical_path = path
                
        # Extract archive components
        archive_components = components[archive_start_index + 1:] if archive_start_index < len(components) else []
        
        return PathInfo(
            original_path=path,
            physical_path=physical_path,
            archive_components=archive_components
        )
    
    def get_parent_archive(self, path_info: PathInfo) -> Optional[PathInfo]:
        """
        Get information about the parent archive containing the given path.
        
        Args:
            path_info: Path information to analyze
            
        Returns:
            PathInfo for the parent archive, or None if not in an archive
        """
        if not path_info.archive_components:
            return None
            
        # The physical path is the parent archive
        return PathInfo(
            original_path=path_info.physical_path,
            physical_path=path_info.physical_path,
            archive_components=[]
        )
    
    def join(self, base: str, *paths: str) -> str:
        """
        Join paths, handling archive components correctly.
        
        Args:
            base: Base path
            *paths: Additional path components
            
        Returns:
            Joined path string
        """
        # Start with the base path
        result = base
        
        # Add each additional path component
        for path in paths:
            # Normalize path
            path = path.replace('\\', '/')
            
            # If path is absolute, it replaces the result
            if path.startswith('/'):
                result = path
                continue
                
            # Otherwise, append with a separator
            if result and not result.endswith('/'):
                result += '/'
                
            result += path
            
        return result