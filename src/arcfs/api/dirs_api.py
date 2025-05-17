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

from arcfs.core.utils import is_archive_format

from arcfs.core.logging import debug_print

class DirsAPI:
    def dirname(self, path: str) -> str:
        """
        Return the directory component of a path.
        Equivalent to os.path.dirname, but exposed via the ARCFS API.
        """
        import os
        return os.path.dirname(path)

    """
    ARCFS Public API: Directory Operations. This class is exposed as fs.dirs on ArchiveFS.
    This class is not meant to be used directly, but through ArchiveFS.
    """
    def __init__(self, archive_fs=None):
        if archive_fs is not None:
            self._path_resolver = archive_fs._path_resolver
            self._stream_provider = archive_fs._stream_provider
        else:
            from arcfs.core.path_resolver import PathResolver
            from arcfs.core.stream_provider import StreamProvider
            self._path_resolver = PathResolver()
            self._stream_provider = StreamProvider()
            
    def is_dir(self, path: str) -> bool:
        """
        Check if a path is a directory (physical or virtual/archive).
        """
        if os.path.isdir(path):
            return True
        try:
            items = self.list_dir(path)
            return True
        except (FileNotFoundError, NotADirectoryError):
            return False

    def exists(self, path: str) -> bool:
        """
        Check if a directory exists (physical or virtual/archive).
        """
        if os.path.isdir(path):
            return True
        try:
            self.list_dir(path)
            return True
        except (FileNotFoundError, NotADirectoryError):
            return False

    @staticmethod
    def is_archive_path(path: str) -> bool:
        """
        Check if path contains archive components (i.e. is an archive path).
        """
        from ..core.utils import is_archive_format
        if not path:
            return False
        # Split at first '/' to check the base path
        base = path.split('/', 1)[0]
        return is_archive_format(base)
    
    def mkdir(self, path: str, create_parents: bool = False) -> None:
        """
        Create a directory or virtual directory within an archive.
        
        Args:
            path: Path to create
            create_parents: If True, create parent directories if they don't exist
        """
        # If the path exists as a file, we can't create a directory there
        from ..core.arcfs_physical_io import ArcfsPhysicalIO
        if ArcfsPhysicalIO.stat(path) and ArcfsPhysicalIO.stat(path).st_mode & 0o170000 != 0o040000:
            from arcfs.api.config_api import ConfigAPI
            debug_print(f"[DirsAPI.mkdir] File exists at path: {path}", level=2)
            raise FileExistsError(f"Cannot create directory '{path}': File exists")
            
        # First try as a regular directory
        if not self.is_archive_path(path):
            try:
                from ..core.arcfs_physical_io import ArcfsPhysicalIO
                abs_path = os.path.abspath(path)
                if create_parents:
                    ArcfsPhysicalIO.mkdir(abs_path, parents=True, exist_ok=True)
                else:
                    ArcfsPhysicalIO.mkdir(abs_path, parents=False, exist_ok=True)
                from arcfs.api.config_api import ConfigAPI
                debug_print(f"[DirsAPI.mkdir] Created physical directory at: {abs_path}", level=2)
                return
            except (FileNotFoundError, NotADirectoryError) as e:
                from arcfs.api.config_api import ConfigAPI
                debug_print(f"Exception in DirsAPI.mkdir: {e}", level=1)
                # Fall back to our virtual file system
                pass
        
        # If this is inside an archive, make sure the archive exists
        if '/' in path:
            parts = path.split('/')
            archive_part = parts[0]
            
            # Create the archive if needed
            from ..core.arcfs_physical_io import ArcfsPhysicalIO
            if is_archive_format(archive_part) and not ArcfsPhysicalIO.exists(archive_part):
                from ..core.archive_handlers import get_handler_for_path
                handler_cls = get_handler_for_path(archive_part)
                if handler_cls:
                    handler_cls.create_empty(archive_part)
                
                # Now we can create the directory inside
                with handler_cls(archive_part, 'a') as handler:
                    dir_path = '/'.join(parts[1:])
                    handler.create_dir(dir_path)
                return
        
        # Handle it as a virtual path
        path_info = self._path_resolver.resolve(path)
        
        # If it's a physical path with no archive components
        if not path_info.archive_components:
            if create_parents:
                from ..core.arcfs_physical_io import ArcfsPhysicalIO
                ArcfsPhysicalIO.mkdir(os.path.abspath(path_info.physical_path), parents=True, exist_ok=True)
                from arcfs.api.config_api import ConfigAPI
                debug_print(f"[DirsAPI.mkdir] Created physical directory (parents=True) at: {os.path.abspath(path_info.physical_path)}", level=2)
            else:
                from ..core.arcfs_physical_io import ArcfsPhysicalIO
                ArcfsPhysicalIO.mkdir(os.path.abspath(path_info.physical_path), parents=False, exist_ok=True)
                from arcfs.api.config_api import ConfigAPI
                debug_print(f"[DirsAPI.mkdir] Created physical directory (parents=False) at: {os.path.abspath(path_info.physical_path)}", level=2)
            return
        
        # Otherwise, we need to create a directory in the archive
        parent_path_info = self._path_resolver.get_parent_archive(path_info)
        if not parent_path_info:
            raise ValueError(f"Cannot create directory in non-existent archive: '{path}'")
            
        # Ensure parent archive exists
        from ..core.arcfs_physical_io import ArcfsPhysicalIO
        if not ArcfsPhysicalIO.exists(parent_path_info.physical_path):
            if create_parents:
                # Create the archive
                parent_dir = os.path.dirname(parent_path_info.physical_path)
                if parent_dir:
                    from ..core.arcfs_physical_io import ArcfsPhysicalIO
                    ArcfsPhysicalIO.mkdir(os.path.abspath(parent_dir), parents=True, exist_ok=True)
                    from arcfs.api.config_api import ConfigAPI
                    debug_print(f"[DirsAPI.mkdir] Created parent dir for archive: {os.path.abspath(parent_dir)}", level=2)
                
                # Create the archive directly
                from ..core.archive_handlers import get_handler_for_path
                handler_cls = get_handler_for_path(parent_path_info.physical_path)
                if handler_cls:
                    handler_cls.create_empty(parent_path_info.physical_path, fs=self._archive_fs)
            else:
                raise FileNotFoundError(f"No such file or directory: '{parent_path_info.physical_path}'")
        
        # Create directory in archive
        from ..core.archive_handlers import get_handler_for_path
        handler_cls = get_handler_for_path(parent_path_info.physical_path)
        if handler_cls:
            with handler_cls(parent_path_info.physical_path, 'a') as handler:
                handler.create_dir(path_info.get_entry_path())
    
    def rmdir(self, path: str, recursive: bool = False) -> None:
        """
        Remove a directory. If recursive is True, remove all contents.
        Logs all errors and raises clear exceptions on failure.
        """
        import shutil
        import os
        from arcfs.api.config_api import ConfigAPI
        try:
            if not self.is_archive_path(path):
                abs_path = os.path.abspath(path)
                if recursive:
                    debug_print(f"[DirsAPI.rmdir] Recursively removing: {abs_path}", level=2)
                    shutil.rmtree(abs_path)
                else:
                    debug_print(f"[DirsAPI.rmdir] Removing directory: {abs_path}", level=2)
                    os.rmdir(abs_path)
                debug_print(f"[DirsAPI.rmdir] Removed directory: {abs_path}", level=2)
                return
            # Archive/virtual dir removal not implemented
            debug_print(f"[DirsAPI.rmdir] Virtual/archive directory removal not implemented: {path}", level=1)
            raise NotImplementedError("Virtual/archive directory removal not implemented")
        except Exception as e:
            debug_print(f"[DirsAPI.rmdir] Exception: {e}", level=1, exc=e)
            raise IOError(f"Failed to remove directory {path}: {e}")

        """
        Remove a directory.
        
        Args:
            path: Path to remove
            recursive: If True, recursively remove all contents
        """
        # Check if it's a regular directory first
        from ..core.arcfs_physical_io import ArcfsPhysicalIO
        if ArcfsPhysicalIO.stat(path) and ArcfsPhysicalIO.stat(path).st_mode & 0o170000 == 0o040000:
            from arcfs.api.config_api import ConfigAPI
            debug_print(f"[DirsAPI.rmdir] Removing directory at: {path}, recursive={recursive}", level=2)
            ArcfsPhysicalIO.rmdir(path, recursive=recursive)
            return
        
        # If not, use our virtual file system
        if not self.is_dir(path):
            raise NotADirectoryError(f"Not a directory: '{path}'")
        
        # Check if directory is empty when recursive is False
        if not recursive and len(self.list_dir(path)) > 0:
            raise OSError(f"Directory not empty: '{path}'")
        
        # Remove all contents if recursive
        if recursive:
            for item in self.list_dir(path):
                item_path = f"{path}/{item}"
                remove_op = self.rmdir if self.is_dir(item_path) else self.remove
                remove_op(item_path, recursive=True) if self.is_dir(item_path) else remove_op(item_path)
        
        # Remove the empty directory
        if self.is_dir(path):
            self.remove(path)
        else:
            raise FileNotFoundError(f"No such directory: '{path}'")
    
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
            # Check if it might be an archive itself
            if is_archive_format(path) and os.path.isfile(path_info.physical_path):
                # Treat as if the path has archive components (it's the archive itself)
                from ..core.archive_handlers import get_handler_for_path
                handler_cls = get_handler_for_path(path)
                if handler_cls:
                    with handler_cls(path, 'r') as handler:
                        return handler.list_dir("")
            
            # Otherwise, it should be a directory
            if not os.path.isdir(path_info.physical_path):
                raise NotADirectoryError(f"Not a directory: '{path}'")
            return os.listdir(path_info.physical_path)
        
        # Otherwise, we need to list entries in the archive
        parent_path_info = self._path_resolver.get_parent_archive(path_info)
        if not parent_path_info or not os.path.exists(parent_path_info.physical_path):
            raise FileNotFoundError(f"No such file or directory: '{path}'")
                
        from ..core.archive_handlers import get_handler_for_path
        handler_cls = get_handler_for_path(parent_path_info.physical_path)
        if handler_cls:
            with handler_cls(parent_path_info.physical_path, 'r') as handler:
                return handler.list_dir("")
    
    def walk(self, path: str):
        """
        Generator yielding (root, dirs, files) tuples for directory tree.
        
        Args:
            path: Starting path for walk
            
        Returns:
            Generator yielding (root, dirs, files) tuples
        """
        # Use self.is_dir to support both physical and archive/virtual directories
        if self.is_dir(path):
            # If it's a physical directory, use os.walk for efficiency
            if os.path.isdir(path):
                for root, dirs, files in os.walk(path):
                    # Archive transparency: handle single-file and multi-file archives
                    from ..core.archive_handlers import get_handler_for_path
                    from arcfs.core.utils import is_archive_format
                    new_dirs = list(dirs)
                    new_files = []
                    for f in files:
                        file_path = os.path.join(root, f)
                        if is_archive_format(f) and os.path.isfile(file_path):
                            handler_cls = get_handler_for_path(file_path)
                            if handler_cls:
                                if handler_cls.is_single_file_archive():
                                    # Single-file archive: replace with decompressed name
                                    decompressed_name = handler_cls.get_decompressed_name(file_path)
                                    new_files.append(decompressed_name)
                                else:
                                    # Multi-file archive: treat as directory
                                    new_dirs.append(f)
                                    # Yield contents of the archive
                                    with handler_cls(file_path, 'r') as handler:
                                        for archive_root, archive_dirs, archive_files in handler.walk(""):
                                            yield os.path.join(root, f, archive_root), archive_dirs, archive_files
                        else:
                            new_files.append(f)
                    yield root, new_dirs, new_files
                    # Recurse into both real and archive directories
                    for d in new_dirs:
                        dir_path = os.path.join(root, d)
                        yield from self.walk(dir_path)

                return
            # Otherwise, walk the virtual/archive directory tree
            try:
                items = self.list_dir(path)
            except (FileNotFoundError, NotADirectoryError):
                return
            dirs = []
            files = []
            for item in items:
                item_path = f"{path}/{item}"
                if self.is_dir(item_path):
                    dirs.append(item)
                else:
                    files.append(item)
            yield path, dirs, files
            for dir_name in dirs:
                dir_path = f"{path}/{dir_name}"
                yield from self.walk(dir_path)
        # If not a directory, do nothing
        return

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