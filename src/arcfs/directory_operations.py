"""
Directory operations for the Archive File System.

A Python library that allows working with archives as if they were directories,
providing transparent access to files within archives.

Author: Tim Hosking
Contact: https://github.com/Munger
License: MIT
"""

from typing import List, Iterator, Tuple, Dict, Any
import os
import re
from fnmatch import fnmatch

from .utils import is_archive_format


class DirectoryOperations:
    """
    Implementation of directory operations for ArchiveFS.
    This class is not meant to be used directly, but through ArchiveFS.
    """
    
    def mkdir(self, path: str, create_parents: bool = False) -> None:
        """
        Create a directory or virtual directory within an archive.
        
        Args:
            path: Path to create
            create_parents: If True, create parent directories if they don't exist
        """
        # If the path exists as a file, we can't create a directory there
        if os.path.isfile(path):
            raise FileExistsError(f"Cannot create directory '{path}': File exists")
            
        # First try as a regular directory
        if '/' not in path or not any(ext in path for ext in ['.zip', '.tar', '.gz', '.bz2', '.xz']):
            try:
                if create_parents:
                    os.makedirs(path, exist_ok=True)
                else:
                    os.mkdir(path)
                return
            except (FileNotFoundError, NotADirectoryError):
                # Fall back to our virtual file system
                pass
        
        # Handle it as a virtual path
        path_info = self._path_resolver.resolve(path)
        
        # If creating parents, ensure all parent archives exist
        if create_parents and path_info.archive_components:
            self._ensure_archives_exist(path_info, include_last=False)
        
        # If it's a physical path with no archive components
        if not path_info.archive_components:
            if create_parents:
                os.makedirs(path_info.physical_path, exist_ok=True)
            else:
                os.mkdir(path_info.physical_path)
            return
        
        # Otherwise, we need to create a directory in the archive
        parent_path_info = self._path_resolver.get_parent_archive(path_info)
        if not parent_path_info:
            raise ValueError(f"Cannot create directory in non-existent archive: '{path}'")
            
        # Ensure parent archive exists
        if not os.path.exists(parent_path_info.physical_path):
            if create_parents:
                # Create the archive
                parent_dir = os.path.dirname(parent_path_info.physical_path)
                if parent_dir:
                    os.makedirs(parent_dir, exist_ok=True)
                self.create_archive(parent_path_info.physical_path)
            else:
                raise FileNotFoundError(f"No such file or directory: '{parent_path_info.physical_path}'")
        
        # Create directory in archive
        with self._stream_provider.get_archive_handler(parent_path_info) as handler:
            handler.create_dir(path_info.get_entry_path())
    
    def rmdir(self, path: str, recursive: bool = False) -> None:
        """
        Remove a directory.
        
        Args:
            path: Path to remove
            recursive: If True, recursively remove all contents
        """
        # Check if it's a regular directory first
        if os.path.isdir(path):
            if recursive:
                import shutil
                shutil.rmtree(path)
            else:
                os.rmdir(path)
            return
        
        # If not, use our virtual file system
        if not self.is_dir(path):
            raise NotADirectoryError(f"Not a directory: '{path}'")
        
        # Check if directory is empty when recursive is False
        if not recursive and len(self.list_dir(path)) > 0:
            raise OSError(f"Directory not empty: '{path}'")
        
        # If recursive, remove all contents first
        if recursive:
            for item in self.list_dir(path):
                item_path = f"{path}/{item}"
                if self.is_dir(item_path):
                    self.rmdir(item_path, recursive=True)
                else:
                    self.remove(item_path)
        
        # Now remove the empty directory
        self.remove(path)
    
    def list_dir(self, path: str) -> List[str]:
        """
        List contents of a directory or archive.
        
        Args:
            path: Path to list
            
        Returns:
            List of names of files and subdirectories
        """
        # Check if it's a regular directory first
        if os.path.isdir(path):
            return os.listdir(path)
            
        # If not, use our virtual file system
        path_info = self._path_resolver.resolve(path)
        
        # If it's a physical path with no archive components
        if not path_info.archive_components:
            if not os.path.isdir(path_info.physical_path):
                raise NotADirectoryError(f"Not a directory: '{path}'")
            return os.listdir(path_info.physical_path)
        
        # Otherwise, we need to list entries in the archive
        if is_archive_format(path):
            # If the path itself is an archive, open it and list its root
            with self._stream_provider.get_archive_handler(path_info) as handler:
                return handler.list_dir("")
        else:
            # We're listing a directory inside an archive
            parent_path_info = self._path_resolver.get_parent_archive(path_info)
            if not parent_path_info or not os.path.exists(parent_path_info.physical_path):
                raise FileNotFoundError(f"No such file or directory: '{path}'")
                
            with self._stream_provider.get_archive_handler(parent_path_info) as handler:
                return handler.list_dir(path_info.get_entry_path())
    
    def walk(self, path: str) -> Iterator[Tuple[str, List[str], List[str]]]:
        """
        Generator yielding (root, dirs, files) tuples for directory tree.
        
        Args:
            path: Starting path for walk
            
        Returns:
            Generator yielding (root, dirs, files) tuples
        """
        # Check if it's a regular directory first
        if os.path.isdir(path):
            for root, dirs, files in os.walk(path):
                yield root, dirs, files
            return
            
        # If not, use our virtual file system
        # Start with the current directory
        try:
            items = self.list_dir(path)
        except (FileNotFoundError, NotADirectoryError):
            return
        
        dirs = []
        files = []
        
        # Separate directories and files
        for item in items:
            item_path = f"{path}/{item}"
            if self.is_dir(item_path):
                dirs.append(item)
            else:
                files.append(item)
        
        # Yield the current level
        yield path, dirs, files
        
        # Recursively process subdirectories
        for dir_name in dirs:
            dir_path = f"{path}/{dir_name}"
            yield from self.walk(dir_path)
    
    def glob(self, pattern: str) -> List[str]:
        """
        Return paths matching a glob pattern.
        
        Args:
            pattern: Glob pattern to match
            
        Returns:
            List of matching paths
        """
        # Convert glob pattern to regex
        regex_pattern = pattern.replace(".", r"\.").replace("*", ".*").replace("?", ".") + "$"
        regex = re.compile(regex_pattern)
        
        # Extract the base path from the pattern (up to the first wildcard)
        wildcard_pos = min(
            (pattern.find('*') if pattern.find('*') != -1 else len(pattern)),
            (pattern.find('?') if pattern.find('?') != -1 else len(pattern))
        )
        base_path = pattern[:wildcard_pos] if wildcard_pos != len(pattern) else pattern
        base_path = base_path.rstrip("/")
        
        # Find all matching paths
        matching_paths = []
        
        # If the base path doesn't exist, return empty list
        if not self.exists(base_path):
            return matching_paths
        
        # If base path is a file and it matches the pattern, return it
        if not self.is_dir(base_path) and regex.match(base_path):
            matching_paths.append(base_path)
            return matching_paths
        
        # Walk the directory tree from the base path
        for root, dirs, files in self.walk(base_path):
            # Check if current directory matches
            if regex.match(root):
                matching_paths.append(root)
            
            # Check files in current directory
            for file in files:
                file_path = f"{root}/{file}"
                if regex.match(file_path):
                    matching_paths.append(file_path)
        
        return matching_paths