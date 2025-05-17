"""
TAR archive handler for the Archive File System.
Provides access to TAR format archives with various compression types.

Author: Tim Hosking
Contact: https://github.com/Munger
License: MIT
"""

import io
import tarfile
import tempfile

import time
import platform
from typing import Dict, List, Optional, BinaryIO, Any, Set
from datetime import datetime


# Utility function for archive format and compression selection

def get_archive_format(path: str) -> str:
    """
    Extracts the extension from the archive path (e.g., '.tar.gz', '.tar.bz2', etc.)
    """
    import os
    base = os.path.basename(path)
    # Recognize .tar.gz, .tar.bz2, .tar.xz, etc.
    for ext in ('.tar.gz', '.tgz', '.tar.bz2', '.tbz2', '.tar.xz', '.txz', '.tar'):
        if base.endswith(ext):
            return ext
    # fallback to last extension
    return os.path.splitext(base)[1]

def get_tar_compression(ext: str) -> str:
    mapping = {
        ('.gz', '.tgz'): ':gz',
        ('.bz2', '.tbz2'): ':bz2',
        ('.xz', '.txz'): ':xz',
    }
    for exts, comp in mapping.items():
        if any(ext.endswith(e) for e in exts):
            return comp
    return ''

from arcfs.api.config_api import ConfigAPI
from arcfs.core.base_handler import ArchiveHandler
from arcfs.core.logging import debug_print


class TarStream:
    """
    A stream for reading or writing a TAR archive member.

    This class provides a file-like interface for reading or writing a TAR archive member.
    All file operations are performed via the ARCFS file API and are agnostic to how files are buffered or stored.

    Attributes:
        tar_file (tarfile.TarFile): The TAR archive file.
        member (tarfile.TarInfo): The TAR archive member.
        mode (str): The mode for reading or writing the member.
        _closed (bool): Whether the stream is closed.
        _write_mode (bool): Whether the stream is in write mode.
        handler (TarHandler): The TarHandler instance that created this stream.
    """

    def __init__(self, tar_file: tarfile.TarFile, member: tarfile.TarInfo, mode: str, buffer_threshold: Optional[int] = None, handler=None):
        self.tar_file = tar_file
        self.member = member
        self.mode = mode
        # Use handler config or config API for buffer threshold
        if buffer_threshold is not None:
            self._buffer_threshold = buffer_threshold
        elif handler and hasattr(handler, 'config') and hasattr(handler.config, 'buffer_size'):
            self._buffer_threshold = handler.config.buffer_size
        else:
            # Fallback: use ConfigAPI or a reasonable default (e.g., 64*1024)
            try:
                self._buffer_threshold = getattr(ConfigAPI, 'get_buffer_threshold', lambda: 64*1024)()
            except Exception:
                self._buffer_threshold = 64 * 1024
        self._closed = False
        self._write_mode = 'w' in mode or 'a' in mode
        self.handler = handler
        if 'r' in mode and not 'w' in mode:
            try:
                fileobj = tar_file.extractfile(member)
                data = fileobj.read() if fileobj else b''
                # Use handler.fs.files for buffer management
                self._buffer = self.handler.fs.files.open(path=None, mode='r+b', buffering=-1, encoding=None)
                self._buffer.write(data)
                self._buffer.seek(0)
            except Exception as e:
                raise IOError(f"Error extracting member from TAR archive: {e}")
        else:
            # Use handler.fs.files for buffer management
            self._buffer = self.handler.fs.files.open(path=None, mode='w+b', buffering=-1, encoding=None)

    def write(self, b):
        if self._closed:
            raise ValueError("I/O operation on closed file.")
        return self._buffer.write(b)

    def read(self, size: int = -1):
        if self._closed:
            raise ValueError("I/O operation on closed file.")
        return self._buffer.read(size)

    def seek(self, offset: int, whence: int = io.SEEK_SET):
        if self._closed:
            raise ValueError("I/O operation on closed file.")
        return self._buffer.seek(offset, whence)

    def close(self):
        if self._closed:
            return
        self._closed = True
        self._buffer.close()

class TarConfig:
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


class TarHandler(ArchiveHandler):
    config = TarConfig

    # Implement required abstract methods with correct names/signatures
    def entry_exists(self, path: str) -> bool:
        return self.member_exists(path)

    def get_entry_info(self, path: str) -> Optional[dict]:
        return self.get_member_info(path)

    def remove_entry(self, path: str) -> None:
        return self.remove_member(path)

    def open_entry(self, path: str, mode: str = 'r'):
        return self.open_member(path, mode)

    def list_entries(self) -> list:
        # Return a list of ArchiveEntry for all members in the archive
        entries = []
        if self.tar_file:
            for member in self.tar_file.getmembers():
                entries.append(
                    ArchiveEntry(
                        path=member.name,
                        size=member.size,
                        modified=member.mtime,
                        is_dir=member.isdir()
                    )
                )
        # Add staged files not yet in tar_file
        for arc_path, buf in self.staged_files.items():
            if arc_path not in self.deleted_files:
                entries.append(
                    ArchiveEntry(
                        path=arc_path,
                        size=buf.getbuffer().nbytes if hasattr(buf, 'getbuffer') else 0,
                        modified=int(time.time()),
                        is_dir=False
                    )
                )
        # Remove deleted files from list
        entries = [e for e in entries if e.path not in self.deleted_files]
        return entries



    # --- Required abstract methods for ArchiveHandler ---
    def stream_exists(self, arc_path: str) -> bool:
        return self.member_exists(arc_path)

    def get_stream_info(self, arc_path: str):
        # For now, just return None if not found, else provide basic info
        if not self.member_exists(arc_path):
            return None
        try:
            member = self.tar_file.getmember(arc_path)
            return {
                'size': member.size,
                'modified': member.mtime,
                'is_dir': member.isdir(),
                'path': arc_path
            }
        except Exception:
            return None

    @classmethod
    def get_supported_extensions(cls):
        return {'.tar', '.tar.gz', '.tgz', '.tar.bz2', '.tbz2', '.tar.xz', '.txz'}

    def open_stream(self, arc_path: str, mode: str = 'r', encoding: str = 'utf-8'):
        return self.open_member(arc_path, mode, encoding)

    def remove_stream(self, arc_path: str):
        return self.remove_member(arc_path)

    """
    Robust TAR handler for ARCFS. All writes go to a temp dir, and the archive is only rebuilt on commit/close.
    """
    def __init__(self, path: str, mode: str = 'r', fs=None):
        """
        Initialize the TAR handler.

        Args:
            path: Path to the TAR file
            mode: Access mode
            fs: ArchiveFS instance (required)
        """
        if fs is None:
            raise ValueError("TarHandler requires an ArchiveFS instance via the 'fs' argument.")
        self.fs = fs
        self.path = path
        self.mode = mode
        self.tar_file = None
        self.temp_dir = self.fs.dirs.mkdtemp()
        self.staged_files: Dict[str, str] = {}   # archive_path -> temp_path
        self.deleted_files: Set[str] = set()
        self.modified = False
        self._open_archive()

    # --- Required abstract methods for ArchiveHandler ---
    def _open(self, mode: str = 'r'):
        self.mode = mode
        self._open_archive()

    def create_dir(self, arc_path: str):
        temp_dir_path = self.fs.dirs.join(self.temp_dir, arc_path.rstrip('/'))
        self.fs.dirs.mkdir(temp_dir_path, create_parents=True)
        self.modified = True

    def member_exists(self, arc_path: str) -> bool:
        if arc_path in self.deleted_files:
            return False
        if arc_path in self.staged_files and self.fs.files.exists(self.staged_files[arc_path]):
            return True
        if self.tar_file:
            try:
                self.tar_file.getmember(arc_path)
                return True
            except KeyError:
                return False
        return False

    def get_member_info(self, arc_path: str):
        if not self.member_exists(arc_path):
            raise FileNotFoundError(f"Member not found in TAR: {arc_path}")
        # Return a minimal info dict for compatibility
        return {'name': arc_path}

    def list_streams(self, dir_path: str = "") -> list:
        return self.list_dir(dir_path)

    def _open_archive(self):
        # Open the archive for reading (never writing directly)
        compression = ''
        ext = get_archive_format(self.path)
        if ext.endswith('.gz') or ext.endswith('.tgz'):
            compression = ':gz'
        elif ext.endswith('.bz2') or ext.endswith('.tbz2'):
            compression = ':bz2'
        elif ext.endswith('.xz') or ext.endswith('.txz'):
            compression = ':xz'
        if self.fs.files.exists(self.path):
            self.tar_file = tarfile.open(self.path, 'r' + compression)
        else:
            self.tar_file = None

    def open_member(self, arc_path: str, mode: str = 'r', encoding: str = 'utf-8') -> BinaryIO:
        """
        Open an member for reading or writing. Writes/overwrites always go to a temp file.
        """
        if 'w' in mode or 'a' in mode:
            self.modified = True  # Mark archive as modified when opening for write/append
            # For append, copy existing content if exists
            buffer = self.fs.files.open(path=None, mode='w+b', buffering=-1, encoding=None)
            if 'a' in mode and self.tar_file:
                try:
                    member = self.tar_file.getmember(arc_path)
                    with self.tar_file.extractfile(member) as src:
                        buffer.write(src.read())
                        buffer.seek(0, io.SEEK_END)
                except KeyError:
                    pass
            self.staged_files[arc_path] = buffer

            return buffer
        buffer = self.staged_files.get(arc_path)
        if buffer is not None:
            buffer.seek(0)
            return buffer
        if not self.tar_file:
            raise FileNotFoundError(f"Archive not found: {self.path}")
        try:
            member = self.tar_file.getmember(arc_path)
            fileobj = self.tar_file.extractfile(member) if not member.isdir() else None
            if fileobj is None:
                raise FileNotFoundError(f"Member not found in TAR: {arc_path}")
            if 'b' in mode:
                buf = self.fs.files.open(path=None, mode='w+b', buffering=-1, encoding=None)
                buf.write(fileobj.read())
                buf.seek(0)
                return buf
            else:
                buf = self.fs.files.open(path=None, mode='w+', buffering=-1, encoding=encoding)
                buf.write(fileobj.read().decode(encoding))
                buf.seek(0)
                return buf
        except KeyError:
            raise FileNotFoundError(f"Member not found in TAR: {arc_path}")

    def write(self, arc_path: str, data: Any, encoding: str = 'utf-8'):
        mode = 'wb' if isinstance(data, bytes) else 'w'
        with self.open_member(arc_path, mode, encoding=encoding) as f:
            f.write(data)

    def remove_member(self, arc_path: str):
        self.deleted_files.add(arc_path)
        self.modified = True
        if arc_path in self.staged_files:
            try:
                buf = self.staged_files[arc_path]
                if hasattr(buf, 'close'):
                    buf.close()
            except Exception as e:
                debug_print(f"Exception in TarHandler.remove_member: {e}", level=1, exc=e)
            del self.staged_files[arc_path]

    def list_dir(self, dir_path: str) -> List[str]:
        dir_path = dir_path.rstrip('/')
        streams = set()
        if self.tar_file:
            for member in self.tar_file.getmembers():
                if member.name == dir_path:
                    continue
                if not member.name.startswith(dir_path + '/'):
                    continue
                rel = member.name[len(dir_path) + 1:]
                if '/' in rel:
                    streams.add(rel.split('/', 1)[0])
                else:
                    streams.add(rel)
        for staged in self.staged_files:
            if staged == dir_path or not staged.startswith(dir_path + '/'):
                continue
            rel = staged[len(dir_path) + 1:]
            if '/' in rel:
                streams.add(rel.split('/', 1)[0])
            else:
                streams.add(rel)
        for deleted in self.deleted_files:
            if deleted == dir_path or not deleted.startswith(dir_path + '/'):
                continue
            rel = deleted[len(dir_path) + 1:]
            if '/' in rel:
                streams.discard(rel.split('/', 1)[0])
            else:
                streams.discard(rel)
        return sorted(streams)

    def close(self):
        if self.tar_file:
            self.tar_file.close()
            self.tar_file = None
        if self.modified:
            self._commit()
        if self.fs.dirs.exists(self.temp_dir):
            self.fs.dirs.rmdir(self.temp_dir, recursive=True)

    def _commit(self):
        ext = get_archive_format(self.path)
        compression = get_tar_compression(ext)
        
        temp_fd, temp_path = self.fs.files.mkstemp()
        self.fs.files.close_fd(temp_fd)
        try:
            with tarfile.open(temp_path, 'w' + compression) as out_tar:
                if self.tar_file:
                    for member in self.tar_file.getmembers():
                        if member.name in self.deleted_files or member.name in self.staged_files:
                            continue
                        fileobj = self.tar_file.extractfile(member) if not member.isdir() else None
                        out_tar.addfile(member, fileobj)
                for arc_path, buffer in self.staged_files.items():
                    if arc_path in self.deleted_files:
                        continue
                    arc_name = arc_path.replace('\\', '/')
                    info = tarfile.TarInfo(arc_name)
                    buffer.seek(0)
                    data = buffer.read()
                    info.size = len(data)
                    info.mtime = int(time.time())
                    info.mode = 0o644
                    buf = self.fs.files.open(path=None, mode='r+b', buffering=-1, encoding=None)
                    buf.write(data)
                    buf.seek(0)
                    out_tar.addfile(info, buf)
                    buf.close()
            self.fs.files.move(temp_path, self.path)
        except Exception as e:
            debug_print(f"Exception in TarHandler._commit: {e}", level=1, exc=e)
        finally:
            if self.fs.files.exists(temp_path):
                self.fs.files.remove(temp_path)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    @classmethod
    def create_empty(cls, path: str, fs=None) -> None:
        """
        Create a new empty TAR archive using the ARCFS API.
        Args:
            path: Path where the TAR should be created
            fs: ArchiveFS instance providing API access (required)
        """
        if fs is None:
            raise ValueError("ArchiveFS instance (fs) must be provided to create_empty.")
        try:
            # Determine the mode based on the extension
            tar_mode = 'w'
            ext = get_archive_format(path)
            compression = get_tar_compression(ext)
            # Create parent directory if it doesn't exist
            parent_dir = fs.dirs.dirname(path) if hasattr(fs.dirs, 'dirname') else os.path.dirname(path)
            if parent_dir and not fs.dirs.exists(parent_dir):
                fs.dirs.mkdir(parent_dir, create_parents=True)
            # Create the empty TAR file
            with tarfile.open(path, tar_mode + compression):
                pass  # Just create the file
        except Exception as e:
            debug_print(f"Exception in TarHandler.create_empty: {e}", level=1, exc=e)
            raise IOError(f"Error creating empty TAR file: {e}")
        return {
            '.tar', '.tar.gz', '.tgz', '.tar.bz2', '.tbz2', '.tar.xz', '.txz'
        }
    @property
    def buffer_size(self):
        if self._buffer_size is not None:
            return self._buffer_size
        try:
            return getattr(ConfigAPI, 'get_buffer_threshold', lambda: 64*1024)()
        except Exception:
            return 64 * 1024

    @buffer_size.setter
    def buffer_size(self, value):
        self._buffer_size = value

    @property
    def temp_dir(self):
        if self._temp_dir is not None:
            return self._temp_dir
        # Could add a config API global for temp_dir if needed, else fallback
        return '/tmp'

    @temp_dir.setter
    def temp_dir(self, value):
        self._temp_dir = value

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