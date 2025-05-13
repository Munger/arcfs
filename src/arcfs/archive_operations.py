"""
Archive-specific operations for the Archive File System.
Implements methods for creating, extracting, and managing archives.

Author: Tim Hosking
Contact: https://github.com/Munger
License: MIT
"""



import os
from typing import Dict, List


class ArchiveOperations:
    """
    Implementation of archive-specific operations for ArchiveFS.
    This class is not meant to be used directly, but through ArchiveFS.
    """
    
    def create_archive(self, path: str, archive_type: str = 'auto') -> None:
        """
        Create a new empty archive.
        
        Args:
            path: Path to the new archive
            archive_type: Type of archive to create (auto=detect from extension)
        """
        # Determine archive type from extension if auto
        if archive_type == 'auto':
            _, ext = os.path.splitext(path)
            if not ext:
                raise ValueError(f"Cannot determine archive type for path: '{path}'")
            archive_type = ext[1:]  # Remove leading dot
        
        # Ensure parent directory exists
        parent_dir = os.path.dirname(path)
        if parent_dir:
            os.makedirs(parent_dir, exist_ok=True)
        
        # Create empty archive based on type
        from .archive_handlers import get_handler_for_path
        handler_cls = get_handler_for_path(path)
        if not handler_cls:
            raise ValueError(f"Unsupported archive type: '{archive_type}'")
        
        handler_cls.create_empty(path)
    
    def extract_all(self, archive_path: str, target_dir: str) -> None:
        """
        Extract entire archive to target directory.
        
        Args:
            archive_path: Path to the archive
            target_dir: Directory to extract to
        """
        # Ensure target directory exists
        os.makedirs(target_dir, exist_ok=True)
        
        # Open the archive
        path_info = self._path_resolver.resolve(archive_path)
        with self._stream_provider.get_archive_handler(path_info) as handler:
            # Extract all entries
            for entry in handler.list_entries():
                # Create subdirectories if needed
                entry_path = os.path.join(target_dir, entry.path)
                if entry.is_dir:
                    os.makedirs(entry_path, exist_ok=True)
                    continue
                
                # Ensure parent directory exists
                os.makedirs(os.path.dirname(entry_path), exist_ok=True)
                
                # Extract file
                with handler.open_entry(entry.path, 'rb') as src, open(entry_path, 'wb') as dst:
                    while True:
                        chunk = src.read(8192)  # 8KB chunks
                        if not chunk:
                            break
                        dst.write(chunk)
    
    def compress_dir(self, source_dir: str, archive_path: str) -> None:
        """
        Create archive from directory contents.
        
        Args:
            source_dir: Source directory
            archive_path: Path to the new archive
        """
        # Create empty archive
        self.create_archive(archive_path)
        
        # Walk source directory and add entries to archive
        for root, dirs, files in os.walk(source_dir):
            # Get relative path from source_dir
            rel_path = os.path.relpath(root, source_dir)
            if rel_path == '.':
                rel_path = ''
            
            # Create directories in archive
            for dir_name in dirs:
                if rel_path:
                    archive_dir = f"{rel_path}/{dir_name}"
                else:
                    archive_dir = dir_name
                self.mkdir(f"{archive_path}/{archive_dir}")
            
            # Add files to archive
            for file_name in files:
                # Source file path
                src_file = os.path.join(root, file_name)
                
                # Destination path in archive
                if rel_path:
                    dst_file = f"{archive_path}/{rel_path}/{file_name}"
                else:
                    dst_file = f"{archive_path}/{file_name}"
                
                # Copy file to archive
                self.pipe(src_file, dst_file)
    
    def get_supported_formats(self) -> Dict[str, List[str]]:
        """
        Return list of supported archive and compression formats.
        
        Returns:
            Dictionary with supported formats
        """
        from .archive_handlers import get_supported_formats
        return get_supported_formats()
    
    def is_archive_path(self, path: str) -> bool:
        """
        Check if path contains archive components.
        
        Args:
            path: Path to check
            
        Returns:
            Boolean indicating if path contains archive components
        """
        from .utils import is_archive_format
        return '/' in path and is_archive_format(path.split('/')[0])
    
    def split_archive_path(self, path: str) -> tuple:
        """
        Split path into components.
        
        Args:
            path: Path to split
            
        Returns:
            Tuple of (physical_path, archive_components)
        """
        path_info = self._path_resolver.resolve(path)
        return (path_info.physical_path, path_info.archive_components)
    
    def join_path(self, components: List[str]) -> str:
        """
        Join path components, handling archive boundaries.
        
        Args:
            components: Path components to join
            
        Returns:
            Joined path string
        """
        return '/'.join(components)
    
    def set_archive_handler(self, extension: str, handler_class) -> None:
        """
        Register custom handler for specific archive format.
        
        Args:
            extension: File extension for the archive format
            handler_class: Class implementing the ArchiveHandler interface
        """
        from .archive_handlers import register_handler
        if not extension.startswith('.'):
            extension = f".{extension}"
        register_handler(extension, handler_class)
