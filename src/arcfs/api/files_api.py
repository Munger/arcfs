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
"""
File operations for the Archive File System.

A Python library that allows working with archives as if they were directories,
providing transparent access to files within archives.

Author: Tim Hosking
Contact: https://github.com/Munger
License: MIT
"""

import contextlib
import shutil

class FilesAPI:
    """
    Implementation of file operations for ArchiveFS.
    This class is not meant to be used directly, but through ArchiveFS.
    """
    def append(self, path: str, data: Any, binary: bool = False) -> int:
        """
        Append data to a file at the specified path. Creates the file if it does not exist.
        Args:
            path: The file path
            data: The data to append (str or bytes)
            binary: If True, open file in binary mode
        Returns:
            Number of bytes written
        """
        mode = 'ab' if binary else 'a'
        encoding = None if binary else 'utf-8'
        with self.open(path, mode, encoding=encoding) as f:
            return f.write(data)

    def __init__(self, archive_fs=None):
        # Optionally accept ArchiveFS context for future use
        self._archive_fs = archive_fs
        if archive_fs is not None:
            self._path_resolver = archive_fs._path_resolver
            self._stream_provider = archive_fs._stream_provider
        else:
            from arcfs.core.path_resolver import PathResolver
            from arcfs.core.stream_provider import StreamProvider
            self._path_resolver = PathResolver()
            self._stream_provider = StreamProvider()

    def create(self, path: str, format: str = None, **kwargs):
        """
        Create an empty archive at the specified path.
        Args:
            path: Path to the archive file to create
            format: Optional archive format (e.g., 'zip', 'tar'). If not given, inferred from extension.
            kwargs: Additional options for the handler
        """
        from arcfs.core.archive_handlers import get_handler_for_path
        handler_cls = get_handler_for_path(path)
        if handler_cls is None:
            raise ValueError(f"No handler registered for archive: {path}")
        handler_cls.create_empty(path, fs=self._archive_fs, **kwargs)

    def mkstemp(self, suffix='', prefix='arcfs_', dir=None):
        """
        Create a secure temporary file. Returns (fd, path).
        """
        import tempfile
        from arcfs.api.config_api import ConfigAPI
        try:
            fd, path = tempfile.mkstemp(suffix=suffix, prefix=prefix, dir=dir)
            debug_print(f"[FilesAPI.mkstemp] Created temp file: {path}", level=2)
            return fd, path
        except Exception as e:
            debug_print(f"[FilesAPI.mkstemp] Failed to create temp file: {e}", level=1)
            raise IOError(f"Failed to create temp file: {e}")

    def mkdtemp(self, suffix='', prefix='arcfs_', dir=None):
        """
        Create a secure temporary directory. Returns path.
        """
        import tempfile
        from arcfs.api.config_api import ConfigAPI
        try:
            path = tempfile.mkdtemp(suffix=suffix, prefix=prefix, dir=dir)
            debug_print(f"[FilesAPI.mkdtemp] Created temp dir: {path}", level=2)
            return path
        except Exception as e:
            debug_print(f"[FilesAPI.mkdtemp] Failed to create temp dir: {e}", level=1)
            raise IOError(f"Failed to create temp dir: {e}")

    def close_fd(self, fd):
        """
        Safely close a file descriptor.
        """
        import os
        from arcfs.api.config_api import ConfigAPI
        try:
            os.close(fd)
            debug_print(f"[FilesAPI.close_fd] Closed fd: {fd}", level=2)
        except Exception as e:
            debug_print(f"[FilesAPI.close_fd] Failed to close fd {fd}: {e}", level=1)
            raise IOError(f"Failed to close file descriptor {fd}: {e}")

    """
    Implementation of file operations for ArchiveFS.
    This class is not meant to be used directly, but through ArchiveFS.
    """

    def open(self, path: str, mode: str = 'r', buffering: int = -1, encoding: str = None, errors: str = None, newline: str = None) -> Any:
        """
        Open a file at the specified path. Supports both physical and archive files.
        """
        from ..core.arcfs_physical_io import ArcfsPhysicalIO
        from arcfs.core.buffering import HybridBufferedFile
        # Physical file
        if ArcfsPhysicalIO.exists(path) and not self.is_archive_path(path):
            return open(path, mode, buffering=buffering, encoding=encoding, errors=errors, newline=newline)
        # Archive/virtual file
        path_info = self._path_resolver.resolve(path)
        if not path_info.archive_components:
            return open(path_info.physical_path, mode, buffering=buffering, encoding=encoding, errors=errors, newline=newline)
        parent_path_info = self._path_resolver.get_parent_archive(path_info)
        if not parent_path_info or not ArcfsPhysicalIO.exists(parent_path_info.physical_path):
            raise FileNotFoundError(f"No such file or archive: '{path}'")
        with self._stream_provider.get_archive_handler(parent_path_info) as handler:
            return handler.open_entry(path_info.get_entry_path(), mode)

    def read(self, path: str, size: int = -1, encoding: str = None) -> Any:
        """
        Read contents from a file at the specified path.
        """
        with self.open(path, 'r', encoding=encoding) as f:
            return f.read(size)

    def write(self, path: str, data: Any, encoding: str = None) -> int:
        """
        Write data to a file at the specified path. Returns number of bytes written.
        """
        with self.open(path, 'w', encoding=encoding) as f:
            return f.write(data)

    def truncate(self, path: str, size: int = 0) -> None:
        """
        Truncate a file to a given size.
        """
        with self.open(path, 'r+') as f:
            f.truncate(size)

    def rename(self, src: str, dst: str) -> None:
        """
        Rename a file from src to dst.
        """
        import os
        from ..core.arcfs_physical_io import ArcfsPhysicalIO
        if ArcfsPhysicalIO.exists(src) and not self.is_archive_path(src):
            os.rename(src, dst)
            return
        # Archive rename
        path_info = self._path_resolver.resolve(src)
        parent_path_info = self._path_resolver.get_parent_archive(path_info)
        if not parent_path_info or not ArcfsPhysicalIO.exists(parent_path_info.physical_path):
            raise FileNotFoundError(f"No such file or archive: '{src}'")
        with self._stream_provider.get_archive_handler(parent_path_info) as handler:
            handler.rename_entry(path_info.get_entry_path(), dst)

    def touch(self, path: str) -> None:
        """
        Update the modification and access time of the file, creating it if it does not exist.
        """
        import os
        from ..core.arcfs_physical_io import ArcfsPhysicalIO
        if ArcfsPhysicalIO.exists(path) and not self.is_archive_path(path):
            return os.utime(path, None)
        # Archive/virtual file
        try:
            with self.open(path, 'a'):
                pass
        except Exception:
            raise

    def is_file(self, path: str) -> bool:
        """
        Check if the path is a file (not a directory).
        """
        import os
        if os.path.isfile(path):
            return True
        try:
            info = self.get_info(path)
            return not info.get('is_dir', False)
        except FileNotFoundError:
            return False

    @staticmethod
    def is_archive_path(path: str) -> bool:
        from arcfs.core.utils import is_archive_format
        if not path:
            return False
        base = path.split('/', 1)[0]
        return is_archive_format(base)


    def exists(self, path: str) -> bool:
        """
        Check if a file or directory exists at the specified path.

        Args:
            path: Path to check

        Returns:
            Boolean indicating if path exists
        """
        from arcfs.core.logging import debug_print
        debug_print(f"[FilesAPI.exists] Checking path: {path}", level=2)
        from ..core.arcfs_physical_io import ArcfsPhysicalIO
        if ArcfsPhysicalIO.exists(path):
            debug_print(f"[FilesAPI.exists] Path exists in ArcfsPhysicalIO: {path}", level=2)
            return True

        try:
            # Use the regular method with path resolution
            path_info = self._path_resolver.resolve(path)
            if not path_info.archive_components:
                if not ArcfsPhysicalIO.exists(path_info.physical_path):
                    debug_print(f"[FilesAPI.exists] Physical path does not exist: {path_info.physical_path}", level=2)
                    return False
                debug_print(f"[FilesAPI.exists] Physical path exists: {path_info.physical_path}", level=2)
                return True

            parent_path_info = self._path_resolver.get_parent_archive(path_info)
            if not parent_path_info or not ArcfsPhysicalIO.exists(parent_path_info.physical_path):
                debug_print(f"[FilesAPI.exists] Parent archive does not exist for: {path}", level=2)
                return False

            with self._stream_provider.get_archive_handler(parent_path_info) as handler:
                exists = handler.entry_exists(path_info.get_entry_path())
                debug_print(f"[FilesAPI.exists] Entry exists in archive: {exists} for {path}", level=2)
                return exists

        except Exception as e:
            debug_print(f"[FilesAPI.exists] Exception: {e}", level=1, exc=e)
            # If any error occurs, the path doesn't exist
            return False


    def remove(self, path: str) -> None:
        """
        Delete a file or empty directory.

        Args:
            path: Path to remove
        """
        # Check if it's a regular file or directory first
        from ..core.arcfs_physical_io import ArcfsPhysicalIO
        if ArcfsPhysicalIO.exists(path):
            from arcfs.api.config_api import ConfigAPI
            debug_print(f"[FilesAPI.remove] Removing path via ArcfsPhysicalIO: {path}", level=2)
            if ArcfsPhysicalIO.stat(path).st_mode & 0o170000 == 0o040000:  # Directory
                ArcfsPhysicalIO.rmdir(path)
            else:
                ArcfsPhysicalIO.remove(path)
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
        if self.is_dir(src_path):
            self._copy_directory(src_path, dst_path)
            return
        self._copy_file(src_path, dst_path)

    def _copy_file(self, src_path: str, dst_path: str) -> None:
        """Copy a single file from source to destination."""
        # Simple case: both are regular files
        from ..core.arcfs_physical_io import ArcfsPhysicalIO
        if ArcfsPhysicalIO.stat(src_path) and ArcfsPhysicalIO.stat(src_path).st_mode & 0o170000 != 0o040000 and ArcfsPhysicalIO.exists(os.path.dirname(dst_path)) and not self.is_archive_path(dst_path):
            # Make sure the destination directory exists
            dst_dir = os.path.dirname(dst_path)
            if dst_dir:
                ArcfsPhysicalIO.mkdir(os.path.abspath(dst_dir), parents=True, exist_ok=True)
            # Copy the file directly using built-in file operations
            from arcfs.api.config_api import ConfigAPI
            debug_print(f"[FilesAPI._copy_file] Copying file via ArcfsPhysicalIO: {src_path} -> {dst_path}", level=2)
            with ArcfsPhysicalIO.open(src_path, 'rb') as src, ArcfsPhysicalIO.open(dst_path, 'wb') as dst:
                dst.write(src.read())
            return

        # Use streaming to efficiently copy the file
        try:
            with self.open(src_path, 'rb') as src, self.open(dst_path, 'wb') as dst:
                shutil.copyfileobj(src, dst)
        except Exception as e:
            raise IOError(f"Error copying file '{src_path}' to '{dst_path}': {e}")

    def _copy_directory(self, src_path: str, dst_path: str) -> None:
        """Copy a directory tree from source to destination."""
        # Create destination directory if it doesn't exist
        from ..core.arcfs_physical_io import ArcfsPhysicalIO
        if not ArcfsPhysicalIO.exists(dst_path):
            ArcfsPhysicalIO.mkdir(dst_path, parents=True, exist_ok=True)
        from arcfs.api.config_api import ConfigAPI
        debug_print(f"[FilesAPI._copy_directory] Copying directory tree: {src_path} -> {dst_path}", level=2)
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
        if os.path.exists(src_path) and not self.is_archive_path(dst_path):
            try:
                # Make sure the destination directory exists
                dst_dir = os.path.dirname(dst_path)
                if dst_dir:
                    from .directory_operations import DirectoryOperations
                    DirectoryOperations().mkdir(dst_dir, create_parents=True)

                # Use os.rename for efficiency
                os.rename(src_path, dst_path)
                return
            except OSError as e:
                from arcfs.api.config_api import ConfigAPI
                debug_print(f"Exception in FilesAPI.move: {e}", level=1)
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
        # 1. Check if it's a regular file or directory first
        if os.path.exists(path):
            stat = os.stat(path)
            return {
                'size': stat.st_size,
                'modified': stat.st_mtime,
                'created': stat.st_ctime,
                'is_dir': os.path.isdir(path),
                'path': path
            }

        # 2. Resolve path_info
        path_info = self._path_resolver.resolve(path)
        if not path_info.archive_components:
            if not os.path.exists(path_info.physical_path):
                raise FileNotFoundError(f"No such file or directory: '{path}'")
            stat = os.stat(path_info.physical_path)
            return {
                'size': stat.st_size,
                'modified': stat.st_mtime,
                'created': stat.st_ctime,
                'is_dir': os.path.isdir(path_info.physical_path),
                'path': path
            }

        # 3. Otherwise, get info from the archive
        parent_path_info = self._path_resolver.get_parent_archive(path_info)
        if not parent_path_info or not os.path.exists(parent_path_info.physical_path):
            raise FileNotFoundError(f"No such file or directory: '{path}'")
        with self._stream_provider.get_archive_handler(parent_path_info) as handler:
            entry_info = handler.get_entry_info(path_info.get_entry_path())
            if not entry_info:
                raise FileNotFoundError(f"No such file or directory: '{path}'")
            entry_info['path'] = path
            return entry_info

    def is_dir(self, path: str) -> bool:
        """
        Check if a path is a directory.

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
        except FileNotFoundError as e:
            from arcfs.api.config_api import ConfigAPI
            debug_print(f"Exception in FilesAPI.is_dir: {e}", level=1, exc=e)
            return False
  
    @contextlib.contextmanager
    def transaction(self, paths: List[str]):
        """
        Context manager ensuring atomicity for operations.
      
        Args:
            paths: List of paths that will be modified in the transaction
        """
        import os
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
                        shutil.copy2(path_info.physical_path, backup_path)
                        backups[path] = backup_path
          
            # Execute the transaction body
            yield
          
            # If we get here, transaction succeeded, clean up backups
            for backup_path in backups.values():
                if os.path.exists(backup_path):
                    os.remove(backup_path)
                  
        except Exception as e:
            debug_print(f"[FilesAPI.transaction] Exception: {e}", level=1, exc=e)
            # Transaction failed, restore backups
            for path, backup_path in backups.items():
                if os.path.exists(backup_path):
                    path_info = self._path_resolver.resolve(path)
                    os.replace(backup_path, path_info.physical_path)
            raise e