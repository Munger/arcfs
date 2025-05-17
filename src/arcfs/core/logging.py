"""
Logging and debug output for the Archive File System.
Handles debug/info/warning/error output, querying the config API for the current debug level.

Author: Tim Hosking
Contact: https://github.com/Munger
License: MIT
"""

from arcfs.api.config_api import ConfigAPI
import traceback


def debug_print(msg, level=1, exc=None):
    """
    Print debug output if the current debug level is >= level.
    If debug level is 4 or higher, also print the full stack traceback.

    Args:
        msg: Message to print
        level: Debug level threshold
        exc: Optional exception object (if provided, stack trace will be printed at debug_level >= 4)
    """
    debug_level = ConfigAPI.get_debug_level() if hasattr(ConfigAPI, 'get_debug_level') else 0
    if debug_level >= level:
        print(f"[ARCFS-DEBUG-{level}] {msg}")
        if exc is not None and debug_level >= 4:
            print(traceback.format_exc())
