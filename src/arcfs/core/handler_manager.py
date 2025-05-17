"""
HandlerManager for the Archive File System.
Handles generic archive-level operations such as creation, detection, and dispatching to the appropriate handler based on archive type.

Author: Tim Hosking
Contact: https://github.com/Munger
License: MIT
"""

from typing import Dict, Tuple, Optional

class HandlerManager:
    """
    Central registry and orchestrator for archive handlers and their config interfaces.
    Provides registration, lookup (by extension and by path), deregistration, config schema support, and dispatch for both operational logic and config.

    Usage example:
        HandlerManager.register_handler('.tar', TarHandler, TarConfig)
        handler_cls = HandlerManager.get_handler('.tar')
        config_iface = HandlerManager.get_config('.tar')
        handler_cls2 = HandlerManager.get_handler_for_path('foo.tar.gz')
        HandlerManager.deregister_handler('.tar')
        HandlerManager.create_archive('foo.tar')
    """
    _registry: Dict[str, Tuple[type, Optional[object], Optional[dict]]] = {}

    @classmethod
    def get_handler_for_path(cls, path: str):
        """
        Resolve the handler for a given path, handling multi-extension (e.g., .tar.gz).
        Returns the handler class or None.
        """
        import os
        basename = os.path.basename(path)
        for ext in sorted(cls._registry.keys(), key=len, reverse=True):
            if basename.lower().endswith(ext):
                return cls._registry[ext][0]
        return None

    @classmethod
    def register_handler(cls, ext: str, handler_cls: type, config_iface: Optional[object] = None, config_schema: Optional[dict] = None):
        """
        Register a handler, its config interface, and (optionally) a config schema for a given extension.
        Args:
            ext: Archive extension (e.g., '.tar')
            handler_cls: Handler class implementing archive logic
            config_iface: Config interface object (optional)
            config_schema: Dict describing config schema (optional)
        """
        cls._registry[ext.lower()] = (handler_cls, config_iface, config_schema)

    @classmethod
    def deregister_handler(cls, ext: str):
        """
        Remove a handler and its config from the registry.
        """
        cls._registry.pop(ext.lower(), None)

    @classmethod
    def get_handler(cls, ext: str):
        """
        Get the handler class for a given extension.
        Ensures handler registration if not already done.
        """
        entry = cls._registry.get(ext.lower())
        return entry[0] if entry else None

    @classmethod
    def get_handler_config(cls, ext: str):
        """
        Get the config interface for a given extension.
        """
        entry = cls._registry.get(ext.lower())
        return entry[1] if entry else None

class HandlerConfigProxy:
    """
    Proxy for handler-specific configuration.
    Provides attribute and dict-style access to handler config fields.
    """
    def __init__(self, config_obj):
        self._config_obj = config_obj

    def __getattr__(self, key):
        return getattr(self._config_obj, key)

    def __setattr__(self, key, value):
        if key == '_config_obj':
            super().__setattr__(key, value)
        else:
            setattr(self._config_obj, key, value)

    def __getitem__(self, key):
        return getattr(self._config_obj, key)

    def __setitem__(self, key, value):
        setattr(self._config_obj, key, value)

    def __iter__(self):
        return iter(dir(self._config_obj))

    def __len__(self):
        return len(dir(self._config_obj))

    @classmethod
    def get_config(cls, ext: str):
        """
        Get the config interface for a given extension.
        Always returns a HandlerConfigProxy, which provides both attribute and dict-style access.
        """
        entry = HandlerManager._registry.get(ext.lower())
        config = entry[1] if entry else None
        if config is None:
            return None
        if isinstance(config, HandlerConfigProxy):
            return config
        return HandlerConfigProxy(config)

    @classmethod
    def get_config_schema(cls, ext: str):
        """
        Get the config schema for a given extension (if any).
        """
        entry = cls._registry.get(ext.lower())
        return entry[2] if entry and len(entry) > 2 else None

    @classmethod
    def get_supported_formats(cls):
        """
        Return a list of all registered archive extensions.
        """
        return list(cls._registry.keys())

    @classmethod
    def get_all_handlers(cls):
        """
        Return a dict of all registered handler classes: {ext: handler_cls}
        """
        return {ext: entry[0] for ext, entry in cls._registry.items()}

    @classmethod
    def get_all_configs(cls):
        """
        Return a dict of all registered config interfaces: {ext: config_iface}
        """
        return {ext: entry[1] for ext, entry in cls._registry.items()}

    @classmethod
    def create_archive(cls, path: str, config: Optional[object] = None):
        """
        Create an empty archive at the specified path.
        Raises:
            NotImplementedError: If no handler supports creation for this archive type
        """
        handler_cls = cls.get_handler_for_path(path)
        if handler_cls and hasattr(handler_cls, 'create_empty'):
            if config is not None:
                handler_cls.create_empty(path, config=config)
            else:
                handler_cls.create_empty(path)
        else:
            raise NotImplementedError(f"Archive creation for '{path}' is not supported.")
