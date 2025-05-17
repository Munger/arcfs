"""
ArchiveFSConfigManager: Unified configuration manager for ARCFS and all registered handlers.
"""
from typing import Any, Callable, Dict

class ConfigValue:
    def __init__(self, getter: Callable[[], Any], setter: Callable[[Any], None]):
        self._getter = getter
        self._setter = setter

    def get(self):
        return self._getter()

    def set(self, value):
        self._setter(value)


    def __init__(self):
        # Global options
        self._global_options = {
            'buffer_size': None,  # Use None for auto
            'temp_dir': None
        }
        self._handler_configs: Dict[str, Any] = {}  # ext -> config iface/class

    # --- Global config accessors ---
    def global_buffer_size(self):
        return ConfigValue(
            getter=lambda: self._global_options['buffer_size'],
            setter=lambda v: self._set_global_option('buffer_size', v)
        )

    def temp_dir(self):
        return ConfigValue(
            getter=lambda: self._global_options['temp_dir'],
            setter=lambda v: self._set_global_option('temp_dir', v)
        )

    def _set_global_option(self, key, value):
        self._global_options[key] = value
        # TODO: propagate to ConfigAPI if needed

    # --- Handler registration ---
    def _register_handler_config(self, ext: str, config_iface: Any):
        try:
            self._handler_configs[ext] = config_iface
        except Exception as e:
            # TODO: Replace with debug_print or new logging system
# ConfigAPI.debug_print(f"Failed to register handler config for {ext}", level=1, exc=e)
            raise

    # --- Handler-specific config access ---
    def _handler(self, ext: str):
        try:
            cfg = self._handler_configs.get(ext)
            if not cfg:
                # TODO: Replace with debug_print or new logging system
# ConfigAPI.debug_print(f"No config registered for {ext}", level=1)
                raise ValueError(f"No config registered for {ext}")
            return cfg
        except Exception as e:
            # TODO: Replace with debug_print or new logging system
# ConfigAPI.debug_print(f"Failed to get handler config for {ext}", level=1, exc=e)
            raise

    @property
    def zip(self):
        try:
            return self._handler('.zip')
        except Exception as e:
            # TODO: Replace with debug_print or new logging system
# ConfigAPI.debug_print(f"Failed to get zip handler config", level=1, exc=e)
            raise

    @property
    def tar(self):
        return self._handler('.tar')

    @property
    def gzip(self):
        return self._handler('.gz')

    @property
    def bzip2(self):
        return self._handler('.bz2')

    @property
    def xz(self):
        return self._handler('.xz')

# Singleton instance for ARCFS
config = ArchiveFSConfigManager()
