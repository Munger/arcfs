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


from arcfs.core.handler_manager import HandlerManager

def get_handler_for_path(path: str):
    """
    Get the appropriate archive handler for a path using HandlerManager.
    """
    return HandlerManager.get_handler_for_path(path)


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
