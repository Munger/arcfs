"""
ZIP archive handler for the Archive File System.
Provides access to ZIP format archives.

Author: Tim Hosking
Contact: https://github.com/Munger
License: MIT
"""

import io
import zipfile
from typing import Dict, List, Optional, BinaryIO, Any, Set
from arcfs.api.config_api import ConfigAPI
from arcfs.core.base_handler import ArchiveHandler
from arcfs.core.logging import debug_print

class ZipStream:
    """
    Stream wrapper for ZIP members.

    This stream provides a file-like interface for reading or writing ZIP archive members.
    All file operations are performed via the ARCFS file API and are agnostic to how files are buffered or stored.
    """
    def __init__(self, zip_file: zipfile.ZipFile, member_name: str, mode: str, buffer_threshold: Optional[int] = None, handler=None):
        """
        Initialize the ZIP stream.

        Args:
            zip_file: ZIP file object
            member_name: Name of the member in the ZIP archive
            mode: Access mode
            buffer_threshold: Optional buffer threshold override
            handler: Reference to parent handler (must have .fs attribute)
        """
        self.zip_file = zip_file
        self.member_name = member_name
        self.mode = mode
        # Use handler config or config API for buffer threshold
        if buffer_threshold is not None:
            self._buffer_threshold = buffer_threshold
        elif handler and hasattr(handler, 'config') and hasattr(handler.config, 'buffer_size'):
            self._buffer_threshold = handler.config.buffer_size
        else:
            # Fallback: use ConfigAPI or a reasonable default (e.g., 64*1024)
            try:
                from arcfs.api.config_api import ConfigAPI
                self._buffer_threshold = getattr(ConfigAPI, 'get_buffer_threshold', lambda: 64*1024)()
            except Exception:
                self._buffer_threshold = 64 * 1024
        self._closed = False
        self._write_mode = 'w' in mode or 'a' in mode
        self.handler = handler
        if 'r' in mode and not 'w' in mode:
            try:
                data = zip_file.read(member_name)
                # Use handler.fs.files for buffer management
                self._buffer = self.handler.fs.files.open(path=None, mode='r+b', buffering=-1, encoding=self.handler.encoding if self.handler else None)
                self._buffer.write(data)
                self._buffer.seek(0)
            except KeyError:
                raise FileNotFoundError(f"Member '{member_name}' not found in ZIP archive.")
        else:
            # Use handler.fs.files for buffer management
            self._buffer = self.handler.fs.files.open(path=None, mode='w+b', buffering=-1, encoding=self.handler.encoding if self.handler else None)

    def write(self, b):
        if self._closed:
            raise ValueError("I/O operation on closed file.")
        self._dirty = True
        return self._buffer.write(b)

    def read(self, size=-1):
        if self._closed:
            raise ValueError("I/O operation on closed file.")
        return self._buffer.read(size)

    def seek(self, offset, whence=io.SEEK_SET):
        if self._closed:
            raise ValueError("I/O operation on closed file.")
        return self._buffer.seek(offset, whence)

    def tell(self):
        if self._closed:
            raise ValueError("I/O operation on closed file.")
        return self._buffer.tell()

    @property
    def closed(self):
        return self._closed

    def close(self):
        if self._closed:
            return
        if ('w' in self.mode or 'a' in self.mode) and hasattr(self.zip_file, 'writestr'):
            if self.zip_file.fp is not None:
                self._buffer.seek(0)
            data = self._buffer.read()
            self.zip_file.writestr(self.member_name, data)
        self._buffer.close()
        self._closed = True

    # Context manager support
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def readable(self):
        return 'r' in self.mode

    def writable(self):
        return 'w' in self.mode or 'a' in self.mode

    def seekable(self):
        return True

    def flush(self):
        self._buffer.flush()


class ZipConfig:
    _overrides = {}

    @classmethod
    def set(cls, key, value):
        cls._overrides[key] = value

    @classmethod
    def get(cls, key):
        if key in cls._overrides:
            return cls._overrides[key]
        from arcfs.core.global_config import GlobalConfig
        return GlobalConfig.get(key)

    @classmethod
    def reset(cls, key=None):
        if key is None:
            cls._overrides.clear()
        else:
            cls._overrides.pop(key, None)


class ZipHandler(ArchiveHandler):
    """
    Handler for ZIP format archives.
    """
    config = ZipConfig

    # --- Required abstract methods for ArchiveHandler ---
    def stream_exists(self, arc_path: str) -> bool:
        return self.member_exists(arc_path)

    def get_stream_info(self, arc_path: str):
        return self.get_member_info(arc_path)

    @classmethod
    def get_supported_extensions(cls):
        return {'.zip', '.jar', '.war', '.ear', '.apk'}

    def open_stream(self, arc_path: str, mode: str = 'r', encoding: str = 'utf-8'):
        return self.open_member(arc_path, mode)

    def remove_stream(self, arc_path: str):
        return self.remove_member(arc_path)

    def list_streams(self):
        return self.list_members()

    """
    Handler for ZIP format archives.
    """
    def _open(self, mode: str = 'r'):
        pass
   
    def __init__(self, path: str, mode: str = 'r', fs=None):
        """
        Initialize the ZIP handler.

        Args:
            path: Path to the ZIP file
            mode: Access mode
            fs: ArchiveFS instance (required)
        """
        if fs is None:
            raise ValueError("ZipHandler requires an ArchiveFS instance via the 'fs' argument.")
        self.fs = fs
        self.path = path
        self.mode = mode
        self.zip_file = None
        self.temp_dir = None
        self.members_to_update = {}
        self.modified = False
        mode_map = {
            'r': self._open_read,
            'w': lambda: self._open_write('w'),
            'x': lambda: self._open_write('x'),
            'a': lambda: self._open_write('a')
        }
        if mode in mode_map:
            mode_map[mode]()
        else:
            raise ValueError(f"Unsupported mode: {mode}")

    def _open_read(self):
        self.zip_file = zipfile.ZipFile(self.path, 'r')

    def _open_write(self, zip_mode):
        if not self.fs.dirs.exists(self.fs.dirs.mkdirname(self.path)) and self.fs.dirs.mkdirname(self.path):
            self.fs.dirs.mkdir(self.fs.dirs.mkdirname(self.path), create_parents=True)
        self.zip_file = zipfile.ZipFile(self.path, zip_mode)
        self.temp_dir = tempfile.mkdtemp()

    def close(self) -> None:
        """Close the ZIP file."""
        try:
            if self.zip_file:
                self.zip_file.close()
                self.zip_file = None
               
                # If there are pending changes, rebuild the ZIP file
                if self.modified and self.temp_dir:
                    self._rebuild_zip()
               
                # Clean up temporary directory
                if self.temp_dir:
                    self.fs.dirs.rmdir(self.temp_dir, recursive=True)
                    self.temp_dir = None
        except Exception as e:
            debug_print(f"Exception in ZipHandler.close: {e}", level=1, exc=e)
            raise IOError(f"Error closing ZIP file: {e}")

    def _rebuild_zip(self) -> None:
        """Rebuild the ZIP file with modifications."""
        # Create a temporary file for the new ZIP
        temp_fd, temp_path = self.fs.files.mkstemp()
       
        try:
            # Create a new ZIP file
            with zipfile.ZipFile(temp_path, 'w') as new_zip:
                # First, copy existing members that haven't been modified
                if self.fs.dirs.exists(self.path):
                    try:
                        with zipfile.ZipFile(self.path, 'r') as old_zip:
                            for item in old_zip.infolist():
                                # Skip members that have been modified or deleted
                                if self.fs.dirs.join(self.temp_dir, item.filename) in self.members_to_update:
                                    continue
                               
                                # Check for deletion markers
                                if self.fs.dirs.exists(self.fs.dirs.join(self.temp_dir, item.filename + ".deleted")):
                                    continue
                               
                                # Copy the member to the new ZIP
                                data = old_zip.read(item.filename)
                                new_zip.writestr(item, data)
                    except Exception:
                        debug_print(f"Exception in ZipHandler._rebuild_zip (reading old ZIP)", level=1)
                        pass
               
                # Now add all the modified/new members from the temp directory
                for root, dirs, files in self.fs.dirs.walk(self.temp_dir):
                    # Get the relative path from the temp directory
                    rel_root = self.fs.dirs.relpath(root, self.temp_dir)
                    if rel_root == '.':
                        rel_root = ''
                   
                    # Add directories (ZIP doesn't technically need directory members, but some tools expect them)
                    for dir_name in dirs:
                        dir_path = self.fs.dirs.join(rel_root, dir_name)
                        if dir_path:
                            # Add trailing slash to indicate it's a directory
                            if not dir_path.endswith('/'):
                                dir_path += '/'
                           
                            # Add an empty directory member
                            new_zip.writestr(dir_path, '')
                   
                    # Add files
                    for file_name in files:
                        # Skip special marker files
                        if file_name.endswith('.deleted'):
                            continue
                           
                        file_path = self.fs.dirs.join(root, file_name)
                        arc_name = self.fs.dirs.join(rel_root, file_name) if rel_root else file_name
                        arc_name = arc_name.replace('\\', '/')  # Use forward slashes for ZIP
                       
                        # Add file to ZIP
                        new_zip.write(file_path, arc_name)
           
            # Replace the original file with the new one
            self.fs.files.close_fd(temp_fd)
            self.fs.files.move(temp_path, self.path)
           
        except Exception:
            raise IOError(f"Error rebuilding ZIP file")
        finally:
            # Clean up the temporary file if it still exists
            if self.fs.dirs.exists(temp_path):
                self.fs.files.remove(temp_path)

    def list_members(self) -> List[Dict[str, Any]]:
        """
        List all members in the ZIP.
       
        Returns:
            List of member information dictionaries
        """
        result = []
       
        try:
            # Get info for all members
            for info in self.zip_file.infolist():
                # Determine if it's a directory
                is_dir = info.filename.endswith('/')
               
                # Convert DOS timestamp to Unix timestamp
                dos_time = info.date_time
                timestamp = time.mktime(datetime(*dos_time).timetuple())
               
                # Create member object
                member = {
                    'path': info.filename,
                    'size': info.file_size,
                    'modified': timestamp,
                    'is_dir': is_dir
                }
               
                result.append(member)
           
            return result
        except Exception:
            # Return empty list for invalid or empty ZIP files
            return []

    def list_streams(self, path: str) -> List[str]:
        """
        List contents of a directory in the ZIP.
       
        Args:
            path: Directory path within the ZIP
           
        Returns:
            List of member names in the directory
        """
        # Normalize path to include trailing slash for directories
        if path and not path.endswith('/'):
            path += '/'
       
        try:
            # Get all members in the ZIP
            members = set()
            prefix_len = len(path)
           
            for name in self.zip_file.namelist():
                # Skip members not in this directory
                if not name.startswith(path):
                    continue
               
                # Get the part after the directory prefix
                relative_name = name[prefix_len:]
               
                # Skip empty names
                if not relative_name:
                    continue
               
                # Get only the first component
                if '/' in relative_name:
                    dir_name = relative_name.split('/', 1)[0]
                    members.add(dir_name)
                else:
                    members.add(relative_name)
           
            # Check in the temporary directory for new members
            if self.temp_dir and self.fs.dirs.exists(self.temp_dir):
                temp_path = self.fs.dirs.join(self.temp_dir, path.rstrip('/'))
                if self.fs.dirs.is_dir(temp_path):
                    for item in self.fs.dirs.listdir(temp_path):
                        # Skip deleted markers
                        if item.endswith('.deleted'):
                            members.discard(item[:-8])  # Remove the deleted member
                        else:
                            members.add(item)
           
            # Convert to list and sort
            return sorted(members)
           
        except Exception:
            # Return empty list for invalid or empty ZIP files
            return []

    def open_member(self, path: str, mode: str = 'r') -> BinaryIO:
        """
        Open a member for reading or writing.
       
        Args:
            path: Member path within the ZIP
            mode: Access mode
           
        Returns:
            File-like object for the member
        """
        # Normalize the member path
        if not path:
            raise ValueError("Member path cannot be empty")
           
        # Check if it's a directory
        if path.endswith('/'):
            raise IsADirectoryError(f"Cannot open directory as file: {path}")
       
        # For write mode, we need special handling
        if 'w' in mode or 'a' in mode:
            self.modified = True  # Mark archive as modified when opening for write/append

            # Make sure we have a temp directory
            if not self.temp_dir:
                self.temp_dir = self.fs.dirs.mkdtemp()
           
            # Create the full path in the temp directory
            temp_path = self.fs.dirs.join(self.temp_dir, path)
           
            # Mark as modified and track the updated member
            self.modified = True
            self.members_to_update[temp_path] = path
           
            # Open the temp file with the requested mode
            return self.fs.files.open(temp_path, mode)
       
        # For read mode, if a temp file exists for this member, flush it to the archive
        if self.temp_dir:
            temp_path = self.fs.dirs.join(self.temp_dir, path)
            if self.fs.files.exists(temp_path):
                with self.fs.files.open(temp_path, 'rb') as f:
                    data = f.read()
                self.zip_file.writestr(path, data)
                self.fs.files.remove(temp_path)
                if temp_path in self.members_to_update:
                    del self.members_to_update[temp_path]
        return ZipStream(self.zip_file, path, mode, handler=self, fs=self.fs)

    def get_member_info(self, path: str) -> Optional[Dict[str, Any]]:
        """
        Get information about a member.
       
        Args:
            path: Member path within the ZIP
           
        Returns:
            Dictionary with member information, or None if member doesn't exist
        """
        # Handle root directory
        if not path:
            return {
                'size': 0,
                'compressed_size': 0,
                'modified': self.fs.files.stat(self.path).st_mtime if self.fs.files.exists(self.path) else time.time(),
                'is_dir': True,
                'path': path
            }
       
        # Normalize path
        norm_path = path.rstrip('/')
        is_dir = path.endswith('/')
       
        try:
            # First check if there's a direct match
            try:
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
            except KeyError:
                pass
               
            # Check if it's a directory by looking for path/
            if not is_dir:
                try:
                    info = self.zip_file.getinfo(norm_path + '/')
                   
                    # Convert DOS timestamp to Unix timestamp
                    dos_time = info.date_time
                    timestamp = time.mktime(datetime(*dos_time).timetuple())
                   
                    return {
                        'size': 0,
                        'compressed_size': 0,
                        'modified': timestamp,
                        'is_dir': True,
                        'path': path
                    }
                except KeyError:
                    pass
           
            # Check if any members start with this directory
            dir_path = norm_path + '/'
            for name in self.zip_file.namelist():
                if name.startswith(dir_path):
                    # It's a directory
                    return {
                        'size': 0,
                        'compressed_size': 0,
                        'modified': self.fs.files.stat(self.path).st_mtime if self.fs.files.exists(self.path) else time.time(),
                        'is_dir': True,
                        'path': path
                    }
           
            # Check in the temporary directory if we're in write mode
            if self.temp_dir:
                temp_path = self.fs.dirs.join(self.temp_dir, norm_path)
                if self.fs.files.exists(temp_path):
                    stat = self.fs.files.stat(temp_path)
                    return {
                        'size': stat.st_size,
                        'compressed_size': stat.st_size,  # Same as size for temp files
                        'modified': stat.st_mtime,
                        'is_dir': self.fs.dirs.is_dir(temp_path),
                        'path': path
                    }
               
                # Check if it's a directory in the temp directory
                temp_dir_path = self.fs.dirs.join(self.temp_dir, norm_path)
                if self.fs.dirs.is_dir(temp_dir_path):
                    stat = self.fs.files.stat(temp_dir_path)
                    return {
                        'size': 0,
                        'compressed_size': 0,
                        'modified': stat.st_mtime,
                        'is_dir': True,
                        'path': path
                    }
           
            # Not found
            return None
           
        except Exception:
            # Error accessing the ZIP
            return None

    def member_exists(self, path: str) -> bool:
        """
        Check if a member exists in the ZIP.
       
        Args:
            path: Member path within the ZIP
           
        Returns:
            True if the member exists, False otherwise
        """
        return self.get_member_info(path) is not None

    def create_dir(self, path: str) -> None:
        """
        Create a directory in the ZIP.
       
        Args:
            path: Directory path to create
        """
        # Normalize directory path to end with slash
        if not path.endswith('/'):
            path += '/'
       
        # Check if the directory already exists
        if self.member_exists(path):
            info = self.get_member_info(path)
            if not info['is_dir']:
                raise NotADirectoryError(f"Path exists but is not a directory: {path}")
            return
       
        # For ZIP files with write/append mode, create directory in temp directory
        # and add an member
        if 'w' in self.mode or 'a' in self.mode:
            # Make sure we have a temp directory
            if not self.temp_dir:
                self.temp_dir = self.fs.dirs.mkdtemp()
           
            # Create the directory in the temp directory
            temp_path = self.fs.dirs.join(self.temp_dir, path.rstrip('/'))
            self.fs.dirs.mkdir(temp_path, create_parents=True)
           
            # Add directory member to the ZIP
            self.zip_file.writestr(path, '')  # Empty content for directory
           
            # Mark as modified
            self.modified = True
            self.members_to_update[temp_path] = path
            return
           
        # For pure read mode
        raise IOError(f"Cannot create directory in read-only ZIP: {path}")

    def remove_member(self, path: str) -> None:
        """
        Remove a member from the ZIP.
       
        Args:
            path: Member path to remove
        """
        # Check if the member exists
        if not self.member_exists(path):
            raise FileNotFoundError(f"Member not found in ZIP: {path}")
       
        # ZIP files don't support direct removal, so we'll mark it for removal
        # during rebuild
        if not self.temp_dir:
            self.temp_dir = self.fs.dirs.mkdtemp()
       
        # Create a sentinel file to mark for deletion
        norm_path = path.rstrip('/')
        temp_path = self.fs.dirs.join(self.temp_dir, norm_path + ".deleted")
        self.fs.dirs.mkdir(self.fs.dirs.mkdirname(temp_path), create_parents=True)
        with self.fs.files.open(temp_path, 'w') as f:
            f.write("DELETED")
       
        # Mark as modified
        self.modified = True
        self.members_to_update[temp_path] = path
        return
           
    @classmethod
    def create_empty(cls, path: str, fs=None) -> None:

        """
        Create a new empty ZIP archive.
       
        Args:
            path: Path where the ZIP should be created
        """
        try:
            # Ensure parent directory exists
            parent_dir = fs.dirs.dirname(path)
            if parent_dir and not fs.dirs.exists(parent_dir):
                fs.dirs.mkdir(parent_dir, create_parents=True)
            with zipfile.ZipFile(path, 'w'):
                pass  # Just create the file
        except Exception:
            raise IOError(f"Error creating ZIP file")

    @classmethod
    def get_supported_extensions(cls) -> Set[str]:
        """
        Get the file extensions supported by this handler.
       
        Returns:
            Set of supported extensions (with leading dot)
        """
        return {'.zip', '.jar', '.war', '.ear', '.apk'}


class ZipHandlerConfig:
    """
    Configuration for ZIP handler. Supports attribute and dict-style access, with fallback to config API and defaults.
    """
    def __init__(self, buffer_size=None, temp_dir=None):
        self._buffer_size = buffer_size
        self._temp_dir = temp_dir

    @property
    def buffer_size(self):
        if self._buffer_size is not None:
            return self._buffer_size
        try:
            from arcfs.api.config_api import ConfigAPI
            return getattr(ConfigAPI, 'get_buffer_threshold', lambda: 64*1024)()
        except Exception:
            return 64 * 1024

    @buffer_size.setter
    def buffer_size(self, value):
        self._buffer_size = value

    def __getitem__(self, key):
        return getattr(self, key)

    def __setitem__(self, key, value):
        return setattr(self, key, value)

    def __getattr__(self, key):
        if key in self.__dict__:
            return self.__dict__[key]
        raise AttributeError(key)

    def __setattr__(self, key, value):
        super().__setattr__(key, value)