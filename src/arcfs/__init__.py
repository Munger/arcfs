"""
ArcFS: Transparent Archive File System

A Python library that allows working with archives as if they were directories,
providing transparent access to files within archives.

Author: Tim Hosking
# Contact: https://github.com/Munger
# License: MIT
"""

"""
ArcFS: Transparent Archive File System

A Python library that allows working with archives as if they were directories,
providing transparent access to files within archives.

Author: Tim Hosking
Contact: https://github.com/Munger
License: MIT

Public API:
    - ArchiveFS: Main entry point. Provides .files, .dirs, .archives, .batch, .config namespaces.

Example usage:
    from arcfs import ArchiveFS
    fs = ArchiveFS()
    with fs.files.open('archive.zip/foo.txt', 'w') as f:
        f.write('Hello!')
    fs.dirs.mkdir('archive.zip/newdir')
    fs.archives.create_archive('my.tar.gz')
    with fs.batch as batch:
        batch.files.write('archive.zip/bulk.txt', 'data')
    fs.config.set_buffer_size(128 * 1024 * 1024)
"""

import arcfs.handlers
from .arcfs import ArchiveFS

__version__ = '0.1.0'
__all__ = ["ArchiveFS"]
