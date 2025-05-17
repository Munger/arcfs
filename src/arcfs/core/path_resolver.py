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
        archive_start_index = len(components)  # Default to end of path
        
        # Handle absolute paths
        if path.startswith('/'):
            physical_path = "/"
        
        # Build up the path until we find an archive
        current_path = ""
        found_archive = False
        
        for i, component in enumerate(components):
            if not component:  # Skip empty components
                continue
            
            # Update the current path
            if current_path:
                current_path = f"{current_path}/{component}"
            else:
                current_path = component
            
            # Only consider a component as an archive if:
            # 1. It has a recognized archive extension
            # 2. We haven't already found an archive
            if not found_archive and is_archive_format(component):
                # We found the start of archive components
                archive_start_index = i
                physical_path = current_path
                found_archive = True
        
        # If we didn't find an archive, the physical path is the entire path
        if not found_archive:
            physical_path = path

        # CRITICAL: Always ensure physical_path is absolute if the input path was absolute.
        # If you ever return a relative physical_path for an absolute input, you will cause
        # catastrophic bugs where paths are resolved relative to the project root instead of the filesystem root.
        # DO NOT REMOVE THIS CHECK unless you are 100% certain all code paths preserve absolute-ness!
        if path.startswith('/') and not physical_path.startswith('/'):
            physical_path = '/' + physical_path

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
        # Ensure we preserve the absolute path if present
        physical_path = path_info.physical_path
        
        # Check if this is an absolute path that's missing the leading slash
        if path_info.original_path.startswith('/') and not physical_path.startswith('/'):
            physical_path = '/' + physical_path
        
        return PathInfo(
            original_path=path_info.physical_path,
            physical_path=physical_path,
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
            if path.startswith('/'):
                result = path
                continue
            if result and not result.endswith('/'):
                result += '/'
            result += path
            
        return result