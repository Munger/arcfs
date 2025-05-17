"""
global_config.py
Central configuration for ARCFS library, including buffer thresholds and future tunable options.

Author: Tim Hosking
Contact: https://github.com/Munger
License: MIT
"""

class GlobalConfig:
    _defaults = {
        "buffer_threshold": None,  # Will be dynamically computed if None
        "debug_level": 0,
        # Add more defaults here as needed
        # e.g. "foo": 42, "bar": "baz"
    }
    _settings = _defaults.copy()

    @classmethod
    def set(cls, key, value):
        cls._settings[key] = value

    @classmethod
    def get(cls, key):
        if key == "buffer_threshold":
            # Special handling: compute dynamic default if not set
            val = cls._settings.get(key, None)
            if val is not None:
                return val
            # Compute default buffer threshold
            try:
                import psutil
                total_mem = psutil.virtual_memory().total
            except ImportError:
                total_mem = 8 * 1024 ** 3
            except Exception:
                total_mem = 8 * 1024 ** 3
            threshold = min(max(total_mem // 32, 100 * 1024 ** 2), 2 * 1024 ** 3)
            return threshold
        return cls._settings.get(key, cls._defaults.get(key))

    @classmethod
    def reset(cls, key=None):
        if key is None:
            cls._settings = cls._defaults.copy()
        else:
            if key in cls._defaults:
                cls._settings[key] = cls._defaults[key]
            else:
                cls._settings.pop(key, None)

    @classmethod
    def set_debug_level(cls, value: int):
        cls.set("debug_level", int(value))

    @classmethod
    def get_debug_level(cls) -> int:
        return cls.get("debug_level")

    @classmethod
    def debug_print(cls, msg, level=1, exc=None):
        debug_level = cls.get_debug_level()
        if debug_level >= level:
            print(f"[ARCFS-DEBUG-{level}] {msg}")
            if exc is not None and debug_level >= 4:
                import traceback
                print(traceback.format_exc())

    @classmethod
    def get_buffer_threshold(cls):
        return cls.get("buffer_threshold")

    @classmethod
    def set_buffer_threshold(cls, value: int):
        cls.set("buffer_threshold", int(value))

    @classmethod
    def monitor_and_tune(cls):
        """Future: monitor system and auto-tune threshold."""
        pass
