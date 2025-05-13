"""
ArcFS: Transparent Archive File System

A Python library that allows working with archives as if they were directories,
providing transparent access to files within archives.

Author: Tim Hosking
# Contact: https://github.com/Munger
# License: MIT
"""

from .arcfs import ArchiveFS

__version__ = '0.1.0'
__all__ = ["ArchiveFS"]

# Avoid circular imports by importing and registering handlers here
from .archive_handlers import register_handler

# Import handler classes
from .handlers.zip_handler import ZipHandler
from .handlers.tar_handler import TarHandler
from .handlers.gzip_handler import GzipHandler
from .handlers.bzip2_handler import Bzip2Handler
from .handlers.xz_handler import XzHandler

# Register built-in handlers
register_handler('.zip', ZipHandler)
register_handler('.jar', ZipHandler)
register_handler('.war', ZipHandler)
register_handler('.ear', ZipHandler)
register_handler('.apk', ZipHandler)

register_handler('.tar', TarHandler)
register_handler('.tar.gz', TarHandler)
register_handler('.tgz', TarHandler)
register_handler('.tar.bz2', TarHandler)
register_handler('.tbz2', TarHandler)
register_handler('.tar.xz', TarHandler)
register_handler('.txz', TarHandler)

register_handler('.gz', GzipHandler)
register_handler('.bz2', Bzip2Handler)
register_handler('.xz', XzHandler)
