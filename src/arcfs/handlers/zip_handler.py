"""
ZIP archive handler for the Archive File System.
Provides access to ZIP format archives.

Author: Tim Hosking
Contact: https://github.com/Munger
License: MIT
"""

import os
import io
import zipfile
import time
from typing import Dict, List, Optional, BinaryIO, Any, Set
from datetime import datetime

from ..archive_handlers import ArchiveHandler, ArchiveEntry


class ZipEntryStream(io.BytesIO):
    """
    Stream wrapper for ZIP entries.
    Handles reading and writing to entries in a ZIP archive.
    """
    
    def __init__(self, zip_file: zipfile.ZipFile, entry_name: str, mode: str):
        """
        Initialize a ZIP entry stream.
        
        Args:
            zip_file: Parent ZIP file
            entry_name: Name of the entry in the ZIP
            mode: Access mode
        """
        self.zip_file = zip_file
        self.entry_name = entry_name
        self.mode = mode
        self.closed = False
        
        # Initialize with data if reading
        if 'r' in mode and not 'w' in mode:
            # Check if the entry exists
            try:
                data = zip_file.read(entry_name)
                super().__init__(data)
            except KeyError:
                # Entry doesn't exist
                raise FileNotFoundError(f"Entry not found in ZIP: {entry_name}")
        else:
            # Create empty buffer for writing
            super().__init__()
    
    def close(self):
        """Close the stream, writing data back to the ZIP if in write mode."""
        if self.closed:
            return
            
        # If in write mode, write the data back to the ZIP
        if 'w' in self.mode or 'a' in self.mode:
            # Get the current data
            data = self.getvalue()
            
            # Write it to the ZIP
            self.zip_file.writestr(self.entry_name, data)
        
        # Mark as closed and call parent close
        self.closed = True
        super().close()


class ZipHandler(ArchiveHandler):
    """
    Handler for ZIP format archives.
    """
    
    def __init__(self, path: str, mode: str = 'r'):
        """
        Initialize the ZIP handler.
        
        Args:
            path: Path to the ZIP file
            mode: Access mode
        """
        self.path = path
        self.mode = mode
        self.zip_file = None
        self._open()
    
    def _open(self) -> None:
        """Open the ZIP file."""
        # Convert mode to zipfile mode
        zip_mode = 'r'
        if 'w' in self.mode:
            zip_mode = 'w'
        elif 'a' in self.mode:
            zip_mode = 'a'
        
        # Check if we need to create a new ZIP file
        if zip_mode in ('w', 'a') and not os.path.exists(self.path):
            zip_mode = 'w'
        
        # Open the ZIP file
        self.zip_file = zipfile.ZipFile(self.path, zip_mode)
    
    def close(self) -> None:
        """Close the ZIP file."""
        if self.zip_file:
            self.zip_file.close()
            self.zip_file = None
    
    def list_entries(self) -> List[ArchiveEntry]:
        """
        List all entries in the ZIP.
        
        Returns:
            List of ArchiveEntry objects
        """
        result = []
        
        # Get info for all entries
        for info in self.zip_file.infolist():
            # Determine if it's a directory
            is_dir = info.filename.endswith('/')
            
            # Convert DOS timestamp to Unix timestamp
            dos_time = info.date_time
            timestamp = time.mktime(datetime(*dos_time).timetuple())
            
            # Create entry object
            entry = ArchiveEntry(
                path=info.filename,
                size=info.file_size,
                modified=timestamp,
                is_dir=is_dir
            )
            
            result.append(entry)
        
        return result
    
    def list_dir(self, path: str) -> List[str]:
        """
        List contents of a directory in the ZIP.
        
        Args:
            path: Directory path within the ZIP
            
        Returns:
            List of entry names in the directory
        """
        # Normalize path to include trailing slash for directories
        if path and not path.endswith('/'):
            path += '/'
        
        # Get all entries in the ZIP
        entries = set()
        prefix_len = len(path)
        
        for name in self.zip_file.namelist():
            # Skip entries not in this directory
            if not name.startswith(path):
                continue
            
            # Get the part after the directory prefix
            relative_name = name[prefix_len:]
            
            # Skip empty names
            if not relative_name:
                continue
            
            # Get only the first component
            if '/' in relative_name:
                dir_name = relative_name.split('/', 1)[0] + '/'
                entries.add(dir_name)
            else:
                entries.add(relative_name)
        
        # Convert to list and sort
        return sorted(entries)
    
    def open_entry(self, path: str, mode: str = 'r') -> BinaryIO:
        """
        Open an entry for reading or writing.
        
        Args:
            path: Entry path within the ZIP
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
            
        # Create a stream for the entry
        return ZipEntryStream(self.zip_file, path, mode)
    
    def get_entry_info(self, path: str) -> Optional[Dict[str, Any]]:
        """
        Get information about an entry.
        
        Args:
            path: Entry path within the ZIP
            
        Returns:
            Dictionary with entry information, or None if entry doesn't exist
        """
        # Normalize path
        norm_path = path.rstrip('/')
        
        try:
            # Try to get the info object
            if norm_path:
                info = self.zip_file.getinfo(norm_path)
                
                # Convert DOS timestamp to Unix timestamp
                dos_time = info.date_time
                timestamp = time.mktime(datetime(*dos_time).timetuple())
                
                return {
                    'size': info.file_size,
                    'compressed_size': info.compress_size,
                    'modified': timestamp,
                    'is_dir': False,
                    'path': path
                }
            else:
                # Root directory
                return {
                    'size': 0,
                    'compressed_size': 0,
                    'modified': os.path.getmtime(self.path),
                    'is_dir': True,
                    'path': path
                }
                
        except KeyError:
            # Entry not found, check if it's a directory
            dir_path = norm_path + '/'
            
            # Check if any entries start with this directory
            for name in self.zip_file.namelist():
                if name.startswith(dir_path):
                    # It's a directory
                    return {
                        'size': 0,
                        'compressed_size': 0,
                        'modified': os.path.getmtime(self.path),
                        'is_dir': True,
                        'path': path
                    }
            
            # Not found
            return None
    
    def entry_exists(self, path: str) -> bool:
        """
        Check if an entry exists in the ZIP.
        
        Args:
            path: Entry path within the ZIP
            
        Returns:
            True if the entry exists, False otherwise
        """
        return self.get_entry_info(path) is not None
    
    def create_dir(self, path: str) -> None:
        """
        Create a directory in the ZIP.
        
        Args:
            path: Directory path to create
        """
        # Normalize directory path to end with slash
        if not path.endswith('/'):
            path += '/'
            
        # Just write an empty entry with the directory name
        self.zip_file.writestr(path, '')
    
    def remove_entry(self, path: str) -> None:
        """
        Remove an entry from the ZIP.
        
        Args:
            path: Entry path to remove
        """
        # ZIP files don't support direct removal of entries
        # We need to create a new ZIP file without the entry
        
        # First, check if the entry exists
        if not self.entry_exists(path):
            raise FileNotFoundError(f"Entry not found in ZIP: {path}")
            
        # Create a temporary file
        import tempfile
        with tempfile.NamedTemporaryFile(delete=False) as temp_file:
            temp_path = temp_file.name
            
        try:
            # Create a new ZIP file
            with zipfile.ZipFile(temp_path, 'w') as new_zip:
                # Copy all entries except the one to remove
                for item in self.zip_file.infolist():
                    if item.filename != path and not item.filename.startswith(path + '/'):
                        data = self.zip_file.read(item.filename)
                        new_zip.writestr(item, data)
            
            # Close the current ZIP file
            self.close()
            
            # Replace the old file with the new one
            import shutil
            shutil.move(temp_path, self.path)
            
            # Reopen the ZIP file
            self._open()
            
        finally:
            # Clean up the temporary file if it still exists
            if os.path.exists(temp_path):
                os.unlink(temp_path)
    
    @classmethod
    def create_empty(cls, path: str) -> None:
        """
        Create a new empty ZIP archive.
        
        Args:
            path: Path where the ZIP should be created
        """
        with zipfile.ZipFile(path, 'w'):
            pass  # Just create the file
    
    @classmethod
    def get_supported_extensions(cls) -> Set[str]:
        """
        Get the file extensions supported by this handler.
        
        Returns:
            Set of supported extensions (with leading dot)
        """
        return {'.zip', '.jar', '.war', '.ear', '.apk'}