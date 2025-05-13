"""
Archive handlers registry for the Archive File System.
Manages the registry of handlers for different archive formats.

Author: Tim Hosking
Contact: https://github.com/Munger
License: MIT
"""

import os
from typing import Dict, Optional, List, Type, Set

from .base_handler import ArchiveHandler, ArchiveEntry
from .utils import get_archive_format


# Registry of archive handlers by extension
_ARCHIVE_HANDLERS: Dict[str, Type[ArchiveHandler]] = {}


def register_handler(extension: str, handler_class: Type[ArchiveHandler]) -> None:
    """
    Register an archive handler for a file extension.
    
    Args:
        extension: File extension (with leading dot)
        handler_class: Handler class for the extension
    """
    global _ARCHIVE_HANDLERS
    _ARCHIVE_HANDLERS[extension] = handler_class


def get_handler_for_path(path: str) -> Optional[Type[ArchiveHandler]]:
    """
    Get the appropriate archive handler for a path.
    
    Args:
        path: Path to an archive
        
    Returns:
        Archive handler class, or None if no handler is registered
    """
    ext = get_archive_format(path)
    if ext in _ARCHIVE_HANDLERS:
        return _ARCHIVE_HANDLERS[ext]
    return None


def get_supported_formats() -> Dict[str, List[str]]:
    """
    Get all supported archive and compression formats.
    
    Returns:
        Dictionary with format categories and supported extensions
    """
    # Group handlers by category
    formats = {
        'archive': [],
        'compression': [],
        'other': []
    }
    
    for ext, handler in _ARCHIVE_HANDLERS.items():
        # Categorize based on extension
        if ext in {'.zip', '.tar', '.tar.gz', '.tgz', '.tar.bz2', '.tbz2', '.tar.xz', '.txz', '.7z', '.rar'}:
            formats['archive'].append(ext)
        elif ext in {'.gz', '.bz2', '.xz', '.z'}:
            formats['compression'].append(ext)
        else:
            formats['other'].append(ext)
    
    return formats


# Import handlers - will be done after all handler files are updated
# from .handlers.zip_handler import ZipHandler
# from .handlers.tar_handler import TarHandler
# from .handlers.gzip_handler import GzipHandler
# from .handlers.bzip2_handler import Bzip2Handler
# from .handlers.xz_handler import XzHandler
