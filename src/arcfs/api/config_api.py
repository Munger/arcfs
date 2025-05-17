"""
Configuration operations for the Archive File System.

Author: Tim Hosking
Contact: https://github.com/Munger
License: MIT
"""

from arcfs.core.handler_manager import HandlerManager
from arcfs.core.global_config import GlobalConfig



class ConfigAPI:
    """
    ARCFS Public API: Configuration Operations

    Provides unified access to global ARCFS configuration (buffer threshold, debug level, etc.)
    and dynamic access to handler-specific configuration via HandlerManager.
    """

    def __init__(self):
        """ConfigAPI exposes unified global and handler config access."""
        pass

    def set(self, key, value):
        """
        Set a global config value by key. This is primarily for compatibility with existing tests.

    Examples:
        # Unified access to global config (attribute or dict-style)
        fs.config.buffer_threshold = 256 * 1024 * 1024
        fs.config['buffer_threshold'] = 256 * 1024 * 1024
        x = fs.config.buffer_threshold
        y = fs.config['buffer_threshold']

        # Handler-specific config (attribute or dict-style)
        fs.config.tar.buffer_size = 65536
        fs.config['tar']['buffer_size'] = 65536
        # You can also read:
        x = fs.config.tar.buffer_size
        y = fs.config['tar']['buffer_size']

        # If temp_dir is supported and exposed as a property:
        # fs.config.temp_dir = "/tmp"
        # x = fs.config.temp_dir
    """

    def __init__(self):
        """ConfigAPI exposes unified global and handler config access."""
        pass

    def set(self, key, value):
        """
        Set a global config value by key. This is primarily for compatibility with existing tests.
        """
        if key in GlobalConfig._settings or key in GlobalConfig._defaults:
            GlobalConfig.set(key, value)
        else:
            cfg = HandlerManager.get_handler_config(f'.{key}')
            if cfg is None:
                raise AttributeError(f"No config for handler '{key}'")
            if hasattr(cfg, 'set'):
                cfg.set(key, value)
            else:
                setattr(cfg, key, value)
        if key in GlobalConfig._settings or key in GlobalConfig._defaults:
            GlobalConfig.set(key, value)
        else:
            cfg = HandlerManager.get_handler_config(f'.{key}')
            if cfg is None:
                raise AttributeError(f"No config for handler '{key}'")
            if hasattr(cfg, 'set'):
                cfg.set(key, value)
            else:
                setattr(cfg, key, value)

    def reset(self, key=None):
        """
        Reset all global config and all handler configs, or just a single key if provided.
        """
        GlobalConfig.reset(key)
        for handler_name in HandlerManager.get_handler_names():
            cfg = HandlerManager.get_handler_config(f'.{handler_name}')
            if cfg is not None and hasattr(cfg, 'reset'):
                cfg.reset(key)

    def __getattr__(self, key):
        # Attribute access for global and handler configs
        if key in GlobalConfig._settings or key in GlobalConfig._defaults:
            return GlobalConfig.get(key)
        cfg = HandlerManager.get_handler_config(f'.{key}')
        if cfg is not None:
            return cfg
        raise AttributeError(f"No global or handler config for key '{key}'")

    def __setattr__(self, key, value):
        # Attribute access for global and handler configs
        if key in GlobalConfig._settings or key in GlobalConfig._defaults:
            GlobalConfig.set(key, value)
        else:
            cfg = HandlerManager.get_handler_config(f'.{key}')
            if cfg is None:
                raise AttributeError(f"No config for handler '{key}'")
            if hasattr(cfg, 'set'):
                cfg.set(key, value)
            else:
                setattr(cfg, key, value)

    def __getitem__(self, key):
        # Dict-style access for global and handler configs
        if key in GlobalConfig._settings or key in GlobalConfig._defaults:
            return GlobalConfig.get(key)
        cfg = HandlerManager.get_handler_config(f'.{key}')
        if cfg is not None:
            return cfg
        raise KeyError(f"No global or handler config for key '{key}'")

    def __setitem__(self, key, value):
        # Dict-style access for global and handler configs
        if key in GlobalConfig._settings or key in GlobalConfig._defaults:
            GlobalConfig.set(key, value)
        else:
            cfg = HandlerManager.get_handler_config(f'.{key}')
            if cfg is None:
                raise KeyError(f"No config for handler '{key}'")
            if hasattr(cfg, 'set'):
                cfg.set(key, value)
            else:
                cfg[key] = value

    def __iter__(self):
        # Yield all global config keys and handler names
        yield from GlobalConfig._settings.keys()
        yield from GlobalConfig._defaults.keys()
        yield from HandlerManager.get_handler_names()

    def __len__(self):
        # Return the count of global config keys and handler names
        return len(set(list(GlobalConfig._settings.keys()) + list(GlobalConfig._defaults.keys()))) + len(HandlerManager.get_handler_names())




