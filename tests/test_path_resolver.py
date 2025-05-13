"""
Unit tests for the Archive File System.

Author: Tim Hosking
Contact: https://github.com/Munger
License: MIT
"""

import os
import sys
import tempfile
import shutil
import unittest
from pathlib import Path

# Add the src directory to the path if needed
src_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "../src"))
if src_dir not in sys.path:
    sys.path.insert(0, src_dir)

from arcfs import ArchiveFS
