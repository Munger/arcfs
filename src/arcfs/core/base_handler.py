"""
Base handler for archive types.
Defines the interfaces that all archive handlers must implement.

Author: Tim Hosking
Contact: https://github.com/Munger
License: MIT
"""

from typing import Dict, Optional, List, Any, BinaryIO, Type, NamedTuple, Set
from abc import ABC, abstractmethod


class ArchiveEntry(NamedTuple):
    """Information about an entry in an archive."""
    path: str
    size: int
    modified: float
    is_dir: bool


class ArchiveHandler(ABC):
    """
    Base class for archive format handlers.
    Provides generic config, context management, logging, error handling, buffering, and path normalization.
    Concrete handlers should be as concise as possible and override only what is needed.
    Automatically registers all subclasses with HandlerManager on definition.
    """

    def __init_subclass__(cls, **kwargs):
        super().__init_subclass__(**kwargs)
        try:
            from arcfs.core.handler_manager import HandlerManager
            # Only register concrete subclasses (not the abstract base)
            if cls is not ArchiveHandler and hasattr(cls, 'get_supported_extensions'):
                exts = cls.get_supported_extensions()
                # Find config class by convention
                config_cls = None
                config_name = cls.__name__.replace('Handler', 'Config')
                mod = __import__(cls.__module__, fromlist=[config_name])
                if hasattr(mod, config_name):
                    config_cls = getattr(mod, config_name)
                for ext in exts:
                    HandlerManager.register_handler(ext, cls, config_cls)
        except Exception as e:
            # Do not break import if registration fails
            import warnings
            warnings.warn(f"Handler registration failed for {cls.__name__}: {e}")

    # --- Generic config access ---
    @property
    def config(self):
        # By convention, look for <HandlerName>Config in the same module
        name = type(self).__name__
        config_name = name.replace('Handler', 'Config')
        mod = __import__(self.__module__, fromlist=[config_name])
        return getattr(mod, config_name, None)

    # --- Context management ---
    def __enter__(self):
        """Enter context manager."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    # --- Logging and error handling ---
    def _log(self, msg, level=1, exc=None):
        from arcfs.core.global_config import GlobalConfig
        GlobalConfig.debug_print(f"{type(self).__name__}: {msg}", level=level, exc=exc)

    def _handle_error(self, msg, exc):
        self._log(msg, level=1, exc=exc)

    # --- Buffering utility ---
    def _make_buffer(self, *args, **kwargs):
        try:
            from arcfs.core.buffering import HybridBufferedFile
            return HybridBufferedFile(*args, **kwargs)
        except ImportError:
            import io
            return io.BytesIO(*args, **kwargs)

    # --- Path normalization helpers ---
    @staticmethod
    def _normalize_path(path):
        return path.strip().replace('\\', '/').replace('//', '/')

    @staticmethod
    def _split_path(path):
        parts = path.strip('/').split('/')
        return parts if parts != [''] else []

    # --- Optional methods with default NotImplementedError ---
    def move_entry(self, src, dst):
        raise NotImplementedError(f"{type(self).__name__} does not support move_entry.")

    def copy_entry(self, src, dst):
        raise NotImplementedError(f"{type(self).__name__} does not support copy_entry.")

    # --- Abstract methods ---
    def __init__(self, path: str, mode: str = 'r'):
        """
        Initialize the archive handler.
        
        Args:
            path: Path to the archive
            mode: Access mode ('r' for read, 'w' for write, 'a' for append)
        """
        self.path = path
        self.mode = mode
        self._open()
    
    @abstractmethod
    def _open(self) -> None:
        """Open the archive for access."""
        pass
    
    @abstractmethod
    def close(self) -> None:
        """Close the archive, releasing any resources."""
        pass
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Exit context manager, closing the archive."""
        self.close()
    
    @abstractmethod
    def list_entries(self) -> List[ArchiveEntry]:
        """
        List all entries in the archive.
        
        Returns:
            List of ArchiveEntry objects
        """
        pass
    
    @abstractmethod
    def list_dir(self, path: str) -> List[str]:
        """
        List contents of a directory in the archive.
        
        Args:
            path: Directory path within the archive
            
        Returns:
            List of entry names in the directory
        """
        pass
    
    @abstractmethod
    def open_entry(self, path: str, mode: str = 'r') -> BinaryIO:
        """
        Open an entry for reading or writing, returning a file-like object.
        
        Args:
            path: Entry path within the archive
            mode: Access mode
            
        Returns:
            File-like entry stream for the entry
        """
        pass
    
    @abstractmethod
    def get_entry_info(self, path: str) -> Optional[Dict[str, Any]]:
        """
        Get information about an entry.
        
        Args:
            path: Entry path within the archive
            
        Returns:
            Dictionary with entry information, or None if entry doesn't exist
        """
        pass
    
    @abstractmethod
    def entry_exists(self, path: str) -> bool:
        """
        Check if an entry exists in the archive.
        
        Args:
            path: Entry path within the archive
            
        Returns:
            True if the entry exists, False otherwise
        """
        pass
    
    @abstractmethod
    def create_dir(self, path: str) -> None:
        """
        Create a directory in the archive.
        
        Args:
            path: Directory path to create
        """
        pass
    
    @abstractmethod
    def remove_entry(self, path: str) -> None:
        """
        Remove an entry from the archive.
        
        Args:
            path: Entry path to remove
        """
        pass
    
    @classmethod
    @abstractmethod
    def create_empty(cls, path: str) -> None:
        """
        Create a new empty archive.
        
        Args:
            path: Path where the archive should be created
        """
        pass
    
    @classmethod
    @abstractmethod
    def get_supported_extensions(cls) -> Set[str]:
        """
        Get the file extensions supported by this handler.
        
        Returns:
            Set of supported extensions (with leading dot)
        """
        pass
