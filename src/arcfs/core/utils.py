"""
Utility functions for the Archive File System.

A Python library that allows working with archives as if they were directories,
providing transparent access to files within archives.

Author: Tim Hosking
Contact: https://github.com/Munger
License: MIT
"""

import os
from typing import Dict, List, Set

# Archive format extensions
_ARCHIVE_FORMATS: Set[str] = {
    # Zip formats
    '.zip', '.jar', '.war', '.ear', '.apk',
    
    # Tar formats (with various compressions)
    '.tar', '.tar.gz', '.tgz', '.tar.bz2', '.tbz2', '.tar.xz', '.txz',
    
    # Compression formats (treated as archives with a single file)
    '.gz', '.bz2', '.xz', '.z',
    
    # Other common formats
    '.7z', '.rar', '.iso'
}

# Map of file extensions to MIME types
_MIME_TYPES: Dict[str, str] = {
    '.zip': 'application/zip',
    '.tar': 'application/x-tar',
    '.tar.gz': 'application/x-gtar',
    '.tgz': 'application/x-gtar',
    '.tar.bz2': 'application/x-gtar',
    '.tbz2': 'application/x-gtar',
    '.tar.xz': 'application/x-gtar',
    '.txz': 'application/x-gtar',
    '.gz': 'application/gzip',
    '.bz2': 'application/x-bzip2',
    '.xz': 'application/x-xz',
    '.7z': 'application/x-7z-compressed',
    '.rar': 'application/vnd.rar',
    '.iso': 'application/x-iso9660-image'
}


def is_archive_format(path: str) -> bool:
    """
    Determine if a path represents an archive format.
    
    Args:
        path: Path or filename to check
        
    Returns:
        True if the path has an archive extension
    """
    # If path is empty, it's not an archive
    if not path:
        return False
        
    # Get the lowercase path for case-insensitive comparison
    lower_path = path.lower()
    
    # Check for compound extensions first (like .tar.gz)
    for ext in sorted(_ARCHIVE_FORMATS, key=len, reverse=True):
        if lower_path.endswith(ext):
            return True
            
    return False


def get_archive_format(path: str) -> str:
    """
    Get the archive format extension from a path.
    
    Args:
        path: Path or filename to check
        
    Returns:
        Archive extension (including leading dot) or empty string if not an archive
    """
    # If path is empty, it's not an archive
    if not path:
        return ""
        
    # Get the lowercase path for case-insensitive comparison
    lower_path = path.lower()
    
    # Check for compound extensions first (like .tar.gz)
    for ext in sorted(_ARCHIVE_FORMATS, key=len, reverse=True):
        if lower_path.endswith(ext):
            return ext
            
    return ""


def get_mime_type(path: str) -> str:
    """
    Get the MIME type for a file based on its extension.
    
    Args:
        path: Path or filename to check
        
    Returns:
        MIME type string or 'application/octet-stream' if unknown
    """
    ext = get_archive_format(path)
    if ext in _MIME_TYPES:
        return _MIME_TYPES[ext]
    return 'application/octet-stream'


def is_compression_format(path: str) -> bool:
    """
    Determine if a path represents a compression format (not a multi-file archive).
    
    Args:
        path: Path or filename to check
        
    Returns:
        True if the path has a compression-only extension
    """
    ext = get_archive_format(path)
    return ext in {'.gz', '.bz2', '.xz', '.z'}


# safe_makedirs removed: use DirectoryOperations().mkdir(path, create_parents=True) instead
    """
    Safely create directories only under the system temp directory.
    Raises PermissionError if path is not under the system temp dir.
    """
    import tempfile
    temp_root = tempfile.gettempdir()
    abs_path = os.path.abspath(path)
    import traceback

    if abs_path.startswith(temp_root):
        os.makedirs(path, exist_ok=True)
    else:


        stack = traceback.extract_stack()[:-1]  # exclude this print itself
        for frame in reversed(stack):
            print(f"  File \"{frame.filename}\", line {frame.lineno}, in {frame.name}")
        raise PermissionError(f"Refusing to create directory outside system temp: {path}")

def ensure_dir_exists(path: str) -> None:
    """
    Ensure a directory exists, creating it if necessary.
    
    Args:
        path: Directory path
    """
    if path and not os.path.exists(path):
        os.makedirs(path, exist_ok=True)


def get_base_name(path: str) -> str:
    """
    Get the base name of a file without any archive extensions.
    
    Args:
        path: Path or filename
        
    Returns:
        Base name without archive extensions
    """
    # Get the filename without directory
    filename = os.path.basename(path)
    
    # Remove archive extensions
    ext = get_archive_format(filename)
    if ext:
        return filename[:-len(ext)]
    
    return filename


def is_same_filesystem(path1: str, path2: str) -> bool:
    """
    Check if two paths are on the same filesystem.
    
    Args:
        path1: First path
        path2: Second path
        
    Returns:
        True if both paths are on the same filesystem
    """
    try:
        # Get the device ID for both paths
        stat1 = os.stat(path1)
        stat2 = os.stat(path2)
        
        # Compare device IDs
        return stat1.st_dev == stat2.st_dev
    except OSError as e:
        from arcfs.core.logging import debug_print
        debug_print(f"Exception in is_same_filesystem: {e}", level=1, exc=e)
        # If either path doesn't exist, assume different filesystems
        return False