"""
TAR archive handler for the Archive File System.
Provides access to TAR format archives with various compression types.

Author: Tim Hosking
Contact: https://github.com/Munger
License: MIT
"""

import os
import io
import tarfile
from typing import Dict, List, Optional, BinaryIO, Any, Set
import tempfile

from ..archive_handlers import ArchiveHandler, ArchiveEntry
from ..utils import get_archive_format


class TarEntryStream(io.BytesIO):
    """
    Stream wrapper for TAR entries.
    Handles reading and writing to entries in a TAR archive.
    """
    
    def __init__(self, tar_file: tarfile.TarFile, entry_name: str, mode: str):
        """
        Initialize a TAR entry stream.
        
        Args:
            tar_file: Parent TAR file
            entry_name: Name of the entry in the TAR
            mode: Access mode
        """
        self.tar_file = tar_file
        self.entry_name = entry_name
        self.mode = mode
        self.closed = False
        self.temp_file = None
        
        # Initialize with data if reading
        if 'r' in mode and not 'w' in mode:
            try:
                # Get the entry from the TAR
                entry = tar_file.getmember(entry_name)
                
                # Check if it's a directory
                if entry.isdir():
                    raise IsADirectoryError(f"Cannot open directory as file: {entry_name}")
                
                # Extract the file to a buffer
                file_obj = tar_file.extractfile(entry)
                if file_obj is None:
                    raise FileNotFoundError(f"Cannot open entry in TAR: {entry_name}")
                
                data = file_obj.read()
                super().__init__(data)
                
            except KeyError:
                # Entry doesn't exist
                raise FileNotFoundError(f"Entry not found in TAR: {entry_name}")
        else:
            # Create empty buffer for writing
            super().__init__()
    
    def close(self):
        """Close the stream, writing data back to the TAR if in write mode."""
        if self.closed:
            return
            
        # If in write mode, write the data back to the TAR
        if 'w' in self.mode or 'a' in self.mode:
            # TAR files don't support direct modification of entries
            # We would need to create a new TAR file, which is complex
            # For now, we'll rely on the temp file approach in the handler
            pass
        
        # Mark as closed and call parent close
        self.closed = True
        super().close()


class TarHandler(ArchiveHandler):
    """
    Handler for TAR format archives, including compressed variants.
    """
    
    def __init__(self, path: str, mode: str = 'r'):
        """
        Initialize the TAR handler.
        
        Args:
            path: Path to the TAR file
            mode: Access mode
        """
        self.path = path
        self.mode = mode
        self.tar_file = None
        self.temp_dir = None
        self.modified = False
        self._open()
    
    def _open(self) -> None:
        """Open the TAR file."""
        # Determine the mode and compression
        tar_mode = 'r'
        if 'w' in self.mode:
            tar_mode = 'w'
        elif 'a' in self.mode:
            tar_mode = 'a'
        
        # Add compression mode based on extension
        ext = get_archive_format(self.path)
        if ext.endswith('.gz') or ext.endswith('.tgz'):
            tar_mode += ':gz'
        elif ext.endswith('.bz2') or ext.endswith('.tbz2'):
            tar_mode += ':bz2'
        elif ext.endswith('.xz') or ext.endswith('.txz'):
            tar_mode += ':xz'
        
        # Create the directory if writing and it doesn't exist
        if tar_mode.startswith('w') and not os.path.exists(os.path.dirname(self.path)):
            os.makedirs(os.path.dirname(self.path), exist_ok=True)
        
        # Open the TAR file
        self.tar_file = tarfile.open(self.path, tar_mode)
        
        # Create a temporary directory for modifications
        if 'w' in self.mode or 'a' in self.mode:
            self.temp_dir = tempfile.mkdtemp()
    
    def close(self) -> None:
        """Close the TAR file, applying any pending changes."""
        if self.tar_file:
            self.tar_file.close()
            self.tar_file = None
            
            # If there are pending changes, rebuild the TAR file
            if self.modified and self.temp_dir:
                self._rebuild_tar()
            
            # Clean up temporary directory
            if self.temp_dir:
                import shutil
                shutil.rmtree(self.temp_dir, ignore_errors=True)
                self.temp_dir = None
    
    def _rebuild_tar(self) -> None:
        """Rebuild the TAR file with modifications."""
        # Determine the mode and compression for the new TAR
        tar_mode = 'w'
        ext = get_archive_format(self.path)
        if ext.endswith('.gz') or ext.endswith('.tgz'):
            tar_mode += ':gz'
        elif ext.endswith('.bz2') or ext.endswith('.tbz2'):
            tar_mode += ':bz2'
        elif ext.endswith('.xz') or ext.endswith('.txz'):
            tar_mode += ':xz'
        
        # Create a temporary file for the new TAR
        with tempfile.NamedTemporaryFile(delete=False) as temp_file:
            temp_path = temp_file.name
        
        try:
            # Create the new TAR file
            with tarfile.open(temp_path, tar_mode) as new_tar:
                # Add all files from the temporary directory
                for root, dirs, files in os.walk(self.temp_dir):
                    for dir_name in dirs:
                        dir_path = os.path.join(root, dir_name)
                        arcname = os.path.relpath(dir_path, self.temp_dir)
                        new_tar.add(dir_path, arcname=arcname)
                    
                    for file_name in files:
                        file_path = os.path.join(root, file_name)
                        arcname = os.path.relpath(file_path, self.temp_dir)
                        new_tar.add(file_path, arcname=arcname)
            
            # Replace the old file with the new one
            import shutil
            shutil.move(temp_path, self.path)
            
        finally:
            # Clean up the temporary file if it still exists
            if os.path.exists(temp_path):
                os.unlink(temp_path)
    
    def list_entries(self) -> List[ArchiveEntry]:
        """
        List all entries in the TAR.
        
        Returns:
            List of ArchiveEntry objects
        """
        result = []
        
        # Get info for all entries
        for member in self.tar_file.getmembers():
            # Create entry object
            entry = ArchiveEntry(
                path=member.name,
                size=member.size,
                modified=member.mtime,
                is_dir=member.isdir()
            )
            
            result.append(entry)
        
        return result
    
    def list_dir(self, path: str) -> List[str]:
        """
        List contents of a directory in the TAR.
        
        Args:
            path: Directory path within the TAR
            
        Returns:
            List of entry names in the directory
        """
        # Normalize path to not include trailing slash
        if path.endswith('/'):
            path = path[:-1]
        
        # Get all entries in the TAR
        entries = set()
        
        for member in self.tar_file.getmembers():
            # Skip entries not in this directory
            member_path = member.name
            
            # Skip the directory itself
            if member_path == path:
                continue
            
            # Check if the member is in the requested directory
            if path:
                if not member_path.startswith(path + '/'):
                    continue
                
                # Get the relative path from the directory
                rel_path = member_path[len(path) + 1:]
            else:
                rel_path = member_path
            
            # Skip empty names
            if not rel_path:
                continue
            
            # Get only the first component
            if '/' in rel_path:
                dir_name = rel_path.split('/', 1)[0]
                entries.add(dir_name)
            else:
                entries.add(rel_path)
        
        # Convert to list and sort
        return sorted(entries)
    
    def open_entry(self, path: str, mode: str = 'r') -> BinaryIO:
        """
        Open an entry for reading or writing.
        
        Args:
            path: Entry path within the TAR
            mode: Access mode
            
        Returns:
            File-like object for the entry
        """
        # Normalize the entry path
        if not path:
            raise ValueError("Entry path cannot be empty")
            
        # Check if it's a directory
        if path.endswith('/'):
            raise IsADirectoryError(f"Cannot open directory as file: {path}")
        
        # For write mode, we need special handling
        if 'w' in mode or 'a' in mode:
            # Make sure we have a temp directory
            if not self.temp_dir:
                self.temp_dir = tempfile.mkdtemp()
            
            # Create the full path in the temp directory
            temp_path = os.path.join(self.temp_dir, path)
            
            # Ensure parent directories exist
            os.makedirs(os.path.dirname(temp_path), exist_ok=True)
            
            # For append mode, copy the existing content if it exists
            if 'a' in mode and not 'w' in mode:
                try:
                    # Get the existing content
                    with TarEntryStream(self.tar_file, path, 'r') as src:
                        with open(temp_path, 'wb') as dst:
                            dst.write(src.read())
                except FileNotFoundError:
                    # Entry doesn't exist, that's fine for append mode
                    pass
            
            # Mark as modified
            self.modified = True
            
            # Open the temp file with the requested mode
            return open(temp_path, mode)
        
        # For read mode, use the TarEntryStream
        return TarEntryStream(self.tar_file, path, mode)
    
    def get_entry_info(self, path: str) -> Optional[Dict[str, Any]]:
        """
        Get information about an entry.
        
        Args:
            path: Entry path within the TAR
            
        Returns:
            Dictionary with entry information, or None if entry doesn't exist
        """
        # Handle root directory
        if not path:
            return {
                'size': 0,
                'modified': os.path.getmtime(self.path),
                'is_dir': True,
                'path': path
            }
        
        # Normalize path
        norm_path = path.rstrip('/')
        
        try:
            # Try to get the member
            member = self.tar_file.getmember(norm_path)
            
            return {
                'size': member.size,
                'modified': member.mtime,
                'is_dir': member.isdir(),
                'path': path
            }
                
        except KeyError:
            # Check if it's a directory by looking for children
            for member in self.tar_file.getmembers():
                if member.name.startswith(norm_path + '/'):
                    # It's a directory
                    return {
                        'size': 0,
                        'modified': os.path.getmtime(self.path),
                        'is_dir': True,
                        'path': path
                    }
            
            # Check in the temporary directory if we're in write mode
            if self.temp_dir:
                temp_path = os.path.join(self.temp_dir, norm_path)
                if os.path.exists(temp_path):
                    stat = os.stat(temp_path)
                    return {
                        'size': stat.st_size,
                        'modified': stat.st_mtime,
                        'is_dir': os.path.isdir(temp_path),
                        'path': path
                    }
            
            # Not found
            return None
    
    def entry_exists(self, path: str) -> bool:
        """
        Check if an entry exists in the TAR.
        
        Args:
            path: Entry path within the TAR
            
        Returns:
            True if the entry exists, False otherwise
        """
        return self.get_entry_info(path) is not None
    
    def create_dir(self, path: str) -> None:
        """
        Create a directory in the TAR.
        
        Args:
            path: Directory path to create
        """
        # Normalize directory path to not end with slash
        norm_path = path.rstrip('/')
        
        # Check if the directory already exists
        if self.entry_exists(norm_path):
            info = self.get_entry_info(norm_path)
            if not info['is_dir']:
                raise NotADirectoryError(f"Path exists but is not a directory: {path}")
            return
        
        # For TAR files, we need to rebuild the archive to add a directory
        # We'll use the temp directory approach
        if not self.temp_dir:
            self.temp_dir = tempfile.mkdtemp()
        
        # Create the directory in the temp directory
        temp_path = os.path.join(self.temp_dir, norm_path)
        os.makedirs(temp_path, exist_ok=True)
        
        # Mark as modified
        self.modified = True
    
    def remove_entry(self, path: str) -> None:
        """
        Remove an entry from the TAR.
        
        Args:
            path: Entry path to remove
        """
        # Check if the entry exists
        if not self.entry_exists(path):
            raise FileNotFoundError(f"Entry not found in TAR: {path}")
        
        # TAR files don't support direct removal, so we'll rebuild the archive
        if not self.temp_dir:
            self.temp_dir = tempfile.mkdtemp()
        
        # Extract all entries except the one to remove
        for member in self.tar_file.getmembers():
            # Skip the entry to remove and its children
            if member.name == path or member.name.startswith(path + '/'):
                continue
            
            # Extract the entry to the temp directory
            self.tar_file.extract(member, self.temp_dir)
        
        # Mark as modified
        self.modified = True
    
    @classmethod
    def create_empty(cls, path: str) -> None:
        """
        Create a new empty TAR archive.
        
        Args:
            path: Path where the TAR should be created
        """
        # Determine the mode based on the extension
        tar_mode = 'w'
        ext = get_archive_format(path)
        if ext.endswith('.gz') or ext.endswith('.tgz'):
            tar_mode += ':gz'
        elif ext.endswith('.bz2') or ext.endswith('.tbz2'):
            tar_mode += ':bz2'
        elif ext.endswith('.xz') or ext.endswith('.txz'):
            tar_mode += ':xz'
        
        # Create parent directory if it doesn't exist
        parent_dir = os.path.dirname(path)
        if parent_dir and not os.path.exists(parent_dir):
            os.makedirs(parent_dir, exist_ok=True)
        
        # Create the empty TAR file
        with tarfile.open(path, tar_mode):
            pass  # Just create the file
    
    @classmethod
    def get_supported_extensions(cls) -> Set[str]:
        """
        Get the file extensions supported by this handler.
        
        Returns:
            Set of supported extensions (with leading dot)
        """
        return {
            '.tar', '.tar.gz', '.tgz', '.tar.bz2', '.tbz2', '.tar.xz', '.txz'
        }