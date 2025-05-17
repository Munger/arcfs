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

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../src')))

from arcfs import ArchiveFS
