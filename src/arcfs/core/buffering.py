"""
Generic buffering class for ARCFS archive handlers.
Provides unified in-memory and temporary file buffering for archive entries.

Author: Tim Hosking
Contact: https://github.com/Munger
License: MIT
"""

import io
import os
import tempfile
from threading import RLock
from typing import Optional, Callable, Set

from arcfs.api.config_api import ConfigAPI
from arcfs.core.logging import debug_print

class HybridBufferedFile:
    """
    Static interface for hybrid in-memory/tempfile buffering, as expected by ARCFS codebase.
    Delegates all logic to HybridBufferedStream. No code duplication.
    """
    @staticmethod
    def open(path, mode='r', buffering=-1, encoding=None, errors=None, newline=None):
        # For now, just return a new buffer. Real implementation could map path to buffer registry.
        return HybridBufferedStream(mode=mode, encoding=encoding)

    @staticmethod
    def is_managed(path):
        # For now, no paths are managed by HybridBufferedFile unless you implement a registry
        return False

    @staticmethod
    def remove(path):
        raise NotImplementedError("HybridBufferedFile.remove is not implemented.")

    @staticmethod
    def mkdir(path, parents=False, exist_ok=True):
        raise NotImplementedError("HybridBufferedFile.mkdir is not implemented.")

    @staticmethod
    def rmdir(path, recursive=False):
        raise NotImplementedError("HybridBufferedFile.rmdir is not implemented.")

    @staticmethod
    def exists(path):
        return False

    @staticmethod
    def stat(path):
        raise NotImplementedError("HybridBufferedFile.stat is not implemented.")

    @staticmethod
    def listdir(path):
        raise NotImplementedError("HybridBufferedFile.listdir is not implemented.")

    @staticmethod
    def rename(src, dst):
        raise NotImplementedError("HybridBufferedFile.rename is not implemented.")

class HybridBufferedStream:
    """
    Generic buffered stream for archive file entries.
    Transparently uses in-memory buffer or tempfile based on data size.
    Handles text/binary modes, flush-on-close, and all temp file cleanup internally.
    All open streams are registered for global flush/close (for interruption handling).
    """
    _open_streams: Set['HybridBufferedStream'] = set()
    _lock = RLock()

    def __init__(self, mode: str = 'w+b', encoding: Optional[str] = None, max_memory_size: int = None):
        """
        Generic, fully transparent file-like buffer for archive file entries.
        Transparently uses in-memory buffer or tempfile based on data size.
        Archive handlers should treat this as a file object and never manage buffering or temp files directly.
        """
        self.mode = mode
        self.encoding = encoding or 'utf-8'
        self._max_memory_size = max_memory_size if max_memory_size is not None else ConfigAPI().get_buffer_size()
        self._dirty = False
        self._closed = False
        self._is_text = 't' in mode or 'b' not in mode
        self._buffer = io.BytesIO()
        self._tempfile = None
        self._using_tempfile = False
        if self._is_text:
            self._textio = io.TextIOWrapper(self._buffer, encoding=self.encoding, write_through=True)
        else:
            self._textio = None
        with HybridBufferedStream._lock:
            HybridBufferedStream._open_streams.add(self)

    def _rollover_to_tempfile(self):
        if self._using_tempfile:
            return
        # For text mode, flush and detach the wrapper to get the underlying buffer
        if self._is_text:
            try:
                self._textio.flush()
            except Exception as e:
                debug_print(f"Exception in HybridBufferedStream._rollover_to_tempfile (flushing textio): {e}", level=1)
            try:
                self._textio.detach()
            except Exception as e:
                debug_print(f"Exception in HybridBufferedStream._rollover_to_tempfile (detaching textio): {e}", level=1)
        # Create temp file and copy buffer
        try:
            self._buffer.seek(0)
        except Exception as e:
            debug_print(f"Exception in HybridBufferedStream._rollover_to_tempfile (seeking buffer): {e}", level=1, exc=e)
            return
        try:
            temp = tempfile.NamedTemporaryFile(mode='w+b', delete=False)
        except Exception as e:
            debug_print(f"Exception in HybridBufferedStream._rollover_to_tempfile (creating temp file): {e}", level=1, exc=e)
            return
        try:
            temp.write(self._buffer.read())
        except Exception as e:
            debug_print(f"Exception in HybridBufferedStream._rollover_to_tempfile (writing to temp file): {e}", level=1, exc=e)
            return
        try:
            temp.flush()
        except Exception as e:
            debug_print(f"Exception in HybridBufferedStream._rollover_to_tempfile (flushing temp file): {e}", level=1, exc=e)
            return
        self._buffer.close()
        self._buffer = temp
        self._tempfile = temp
        self._using_tempfile = True
        if self._is_text:
            try:
                self._buffer.seek(0, io.SEEK_SET)
            except Exception as e:
                debug_print(f"Exception in HybridBufferedStream._rollover_to_tempfile (seeking buffer): {e}", level=1, exc=e)
                return
            self._textio = io.TextIOWrapper(self._buffer, encoding=self.encoding, write_through=True)

    def write(self, data):
        if self._closed:
            debug_print("[HybridBufferedStream.write] Attempted to write to a closed stream", level=1)
            return
        self._dirty = True
        # Write at the current file pointer, do not force seek to end
        if self._is_text:
            try:
                res = self._textio.write(data)
            except Exception as e:
                debug_print(f"Exception in HybridBufferedStream.write (writing to textio): {e}", level=1, exc=e)
                return
            try:
                self._textio.flush()
            except Exception as e:
                debug_print(f"Exception flushing textio: {e}", level=1, exc=e)
        else:
            try:
                res = self._buffer.write(data)
            except Exception as e:
                debug_print(f"Exception in HybridBufferedStream.write (writing to buffer): {e}", level=1, exc=e)
                return
        # Check if we need to rollover
        if not self._using_tempfile and self._buffer.tell() >= self._max_memory_size:
            self._rollover_to_tempfile()
        return res

    def read(self, size=-1):
        if self._closed:
            debug_print("[HybridBufferedStream.read] Attempted to read from a closed stream", level=1)
            return
        if self._is_text:
            try:
                return self._textio.read(size)
            except Exception as e:
                debug_print(f"Exception in HybridBufferedStream.read (reading from textio): {e}", level=1, exc=e)
                return
        else:
            try:
                return self._buffer.read(size)
            except Exception as e:
                debug_print(f"Exception in HybridBufferedStream.read (reading from buffer): {e}", level=1, exc=e)
                return

    def flush(self):
        if self._closed:
            debug_print("[HybridBufferedStream.flush] Attempted to flush a closed stream", level=1)
            return
        if self._is_text:
            try:
                self._textio.flush()
            except Exception as e:
                debug_print(f"Exception flushing textio: {e}", level=1, exc=e)
        try:
            self._buffer.flush()
        except Exception as e:
            debug_print(f"Exception in HybridBufferedStream.flush (flushing buffer): {e}", level=1, exc=e)

    def close(self):
        if self._closed:
            debug_print("[HybridBufferedStream.close] Attempted to close an already closed stream", level=1)
            return
        self.flush()
        if self._tempfile is not None:
            temp_path = self._tempfile.name
            try:
                self._tempfile.close()
            except Exception as e:
                debug_print(f"Exception in HybridBufferedStream.close (closing temp file): {e}", level=1, exc=e)
            try:
                if os.path.exists(temp_path):
                    os.remove(temp_path)
            except Exception as e:
                debug_print(f"Exception in HybridBufferedStream.close (removing temp file): {e}", level=1, exc=e)
                os.remove(temp_path)
        self._closed = True
        with HybridBufferedStream._lock:
            HybridBufferedStream._open_streams.discard(self)

    def get_bytes(self):
        """Return all data as bytes, regardless of backend."""
        self.flush()
        pos = self._buffer.tell()
        self._buffer.seek(0)
        data = self._buffer.read()
        self._buffer.seek(pos)
        return data

    @classmethod
    def flush_all(cls):
        with cls._lock:
            for stream in list(cls._open_streams):
                try:
                    stream.flush()
                    stream.close()
                except Exception as e:
                    debug_print(f"Exception in HybridBufferedStream.flush_all: {e}", level=1, exc=e)
                    pass

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def seek(self, offset, whence=os.SEEK_SET):
        if self._is_text:
            return self._textio.seek(offset, whence)
        else:
            return self._buffer.seek(offset, whence)

    def tell(self):
        if self._is_text:
            return self._textio.tell()
        else:
            return self._buffer.tell()

    def writable(self):
        return True

    def readable(self):
        return True

    def seekable(self):
        return True

    @property
    def closed(self):
        return self._closed

