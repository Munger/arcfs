"""
File operations for the Archive File System.

A Python library that allows working with archives as if they were directories,
providing transparent access to files within archives.

Author: Tim Hosking
Contact: https://github.com/Munger
License: MIT
"""

from typing import Dict, List, Union, BinaryIO, TextIO, Any
import os
import contextlib


class FileOperations:
    """
    Implementation of file operations for ArchiveFS.
    This class is not meant to be used directly, but through ArchiveFS.
    """
    
    def exists(self, path: str) -> bool:
        """
        Check if a file or directory exists at the specified path.
        
        Args:
            path: Path to check
            
        Returns:
            Boolean indicating if path exists
        """
        # First check if it's a regular file or directory
        if os.path.exists(path):
            return True
            
        try:
            path_info = self._path_resolver.resolve(path)
            
            # If it's a physical path with no archive components
            if not path_info.archive_components:
                return os.path.exists(path_info.physical_path)
            
            # Otherwise, we need to check if the entry exists in the archive
            parent_path_info = self._path_resolver.get_parent_archive(path_info)
            if not parent_path_info or not os.path.exists(parent_path_info.physical_path):
                return False
                
            with self._stream_provider.get_archive_handler(parent_path_info) as handler:
                return handler.entry_exists(path_info.get_entry_path())
        except Exception:
            return False
    
    def remove(self, path: str) -> None:
        """
        Delete a file or empty directory.
        
        Args:
            path: Path to remove
        """
        # Check if it's a regular file or directory first
        if os.path.exists(path):
            if os.path.isdir(path):
                os.rmdir(path)
            else:
                os.remove(path)
            return
        
        # If not, treat it as an archive path
        path_info = self._path_resolver.resolve(path)
        
        # If it's a physical path with no archive components
        if not path_info.archive_components:
            if os.path.isdir(path_info.physical_path):
                os.rmdir(path_info.physical_path)
            else:
                os.remove(path_info.physical_path)
            return
        
        # Otherwise, we need to remove the entry from the archive
        parent_path_info = self._path_resolver.get_parent_archive(path_info)
        if not parent_path_info or not os.path.exists(parent_path_info.physical_path):
            raise FileNotFoundError(f"No such file or directory: '{path}'")
            
        with self._stream_provider.get_archive_handler(parent_path_info) as handler:
            handler.remove_entry(path_info.get_entry_path())
    
    def copy(self, src_path: str, dst_path: str) -> None:
        """
        Copy a file or directory tree between locations.
        
        Args:
            src_path: Source path
            dst_path: Destination path
        """
        # Check if source exists
        if not self.exists(src_path):
            raise FileNotFoundError(f"No such file or directory: '{src_path}'")
        
        # Check if source is a directory
        if self.is_dir(src_path):
            self._copy_directory(src_path, dst_path)
        else:
            self._copy_file(src_path, dst_path)
    
    def _copy_file(self, src_path: str, dst_path: str) -> None:
        """Copy a single file from source to destination."""
        # Simple case: both are regular files
        if os.path.isfile(src_path) and not '/' in dst_path:
            # Make sure the destination directory exists
            dst_dir = os.path.dirname(dst_path)
            if dst_dir:
                os.makedirs(dst_dir, exist_ok=True)
                
            # Copy the file directly using built-in file operations
            with open(src_path, 'rb') as src, open(dst_path, 'wb') as dst:
                dst.write(src.read())
            return
        
        # Use streaming to efficiently copy the file
        try:
            with self.open(src_path, 'rb') as src, self.open(dst_path, 'wb') as dst:
                while True:
                    chunk = src.read(8192)  # 8KB chunks
                    if not chunk:
                        break
                    dst.write(chunk)
        except (FileNotFoundError, ValueError) as e:
            # Fallback: Try to create parent directories and try again
            if '/' in dst_path:
                dst_dir = os.path.dirname(dst_path)
                if dst_dir:
                    os.makedirs(dst_dir, exist_ok=True)
            
            # Try again with regular file operations
            with open(src_path, 'rb') as src, open(dst_path, 'wb') as dst:
                dst.write(src.read())
    
    def _copy_directory(self, src_path: str, dst_path: str) -> None:
        """Copy a directory tree from source to destination."""
        # Create destination directory if it doesn't exist
        if not self.exists(dst_path):
            self.mkdir(dst_path)
        
        # Copy all contents
        for item in self.list_dir(src_path):
            src_item = f"{src_path}/{item}"
            dst_item = f"{dst_path}/{item}"
            
            if self.is_dir(src_item):
                self._copy_directory(src_item, dst_item)
            else:
                self._copy_file(src_item, dst_item)
    
    def move(self, src_path: str, dst_path: str) -> None:
        """
        Move a file or directory tree between locations.
        
        Args:
            src_path: Source path
            dst_path: Destination path
        """
        # Optimize for the case of moving within the same filesystem
        if os.path.exists(src_path) and not '/' in dst_path:
            try:
                # Make sure the destination directory exists
                dst_dir = os.path.dirname(dst_path)
                if dst_dir:
                    os.makedirs(dst_dir, exist_ok=True)
                    
                # Use os.rename for efficiency
                os.rename(src_path, dst_path)
                return
            except OSError:
                # Fall back to copy + remove
                pass
        
        # Archive path or different filesystem: copy and remove
        self.copy(src_path, dst_path)
        self.remove(src_path)
    
    def get_info(self, path: str) -> Dict[str, Any]:
        """
        Get metadata about a file or directory.
        
        Args:
            path: Path to get information about
            
        Returns:
            Dictionary with metadata (size, timestamps, type, etc.)
        """
        # Check if it's a regular file or directory first
        if os.path.exists(path):
            stat = os.stat(path)
            return {
                'size': stat.st_size,
                'modified': stat.st_mtime,
                'created': stat.st_ctime,
                'is_dir': os.path.isdir(path),
                'path': path
            }
        
        # If not, treat it as an archive path
        path_info = self._path_resolver.resolve(path)
        
        # If it's a physical path with no archive components
        if not path_info.archive_components:
            stat = os.stat(path_info.physical_path)
            return {
                'size': stat.st_size,
                'modified': stat.st_mtime,
                'created': stat.st_ctime,
                'is_dir': os.path.isdir(path_info.physical_path),
                'path': path
            }
        
        # Otherwise, we need to get info from the archive
        parent_path_info = self._path_resolver.get_parent_archive(path_info)
        if not parent_path_info or not os.path.exists(parent_path_info.physical_path):
            raise FileNotFoundError(f"No such file or directory: '{path}'")
            
        with self._stream_provider.get_archive_handler(parent_path_info) as handler:
            entry_info = handler.get_entry_info(path_info.get_entry_path())
            if not entry_info:
                raise FileNotFoundError(f"No such file or directory: '{path}'")
            
            # Add the full path to the info
            entry_info['path'] = path
            return entry_info
    
    def is_dir(self, path: str) -> bool:
        """
        Check if the path is a directory.
        
        Args:
            path: Path to check
            
        Returns:
            True if the path is a directory, False otherwise
        """
        # First check if it's a regular directory
        if os.path.isdir(path):
            return True
            
        # If not, check using our virtual file system
        try:
            info = self.get_info(path)
            return info.get('is_dir', False)
        except FileNotFoundError:
            return False
    
    @contextlib.contextmanager
    def transaction(self, paths: List[str]):
        """
        Context manager ensuring atomicity for operations.
        
        Args:
            paths: List of paths that will be modified in the transaction
        """
        from .utils import is_archive_format
        
        # Create backup copies of all archives that will be modified
        backups = {}
        try:
            for path in paths:
                if self.exists(path) and is_archive_format(path):
                    path_info = self._path_resolver.resolve(path)
                    if not path_info.archive_components:
                        # Only backup physical archives, not entries inside archives
                        backup_path = f"{path_info.physical_path}.bak"
                        self._copy_file(path_info.physical_path, backup_path)
                        backups[path] = backup_path
            
            # Execute the transaction body
            yield
            
            # If we get here, transaction succeeded, clean up backups
            for backup_path in backups.values():
                if os.path.exists(backup_path):
                    os.remove(backup_path)
                    
        except Exception as e:
            # Transaction failed, restore backups
            for path, backup_path in backups.items():
                if os.path.exists(backup_path):
                    path_info = self._path_resolver.resolve(path)
                    os.replace(backup_path, path_info.physical_path)
            raise e