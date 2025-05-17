"""
Unit tests for HybridBufferedFile (generic ARCFS buffering).

Author: Tim Hosking
Contact: https://github.com/Munger
License: MIT
"""

import sys, os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../src')))
import os
import tempfile
import unittest
from arcfs.core.buffering import HybridBufferedFile, HybridBufferedStream
from arcfs.api.config_api import ConfigAPI

class TestHybridBufferedStream(unittest.TestCase):
    def setUp(self):
        # Allow debug level to be set via environment variable for tests
        debug_level = os.environ.get('ARCFS_DEBUG_LEVEL')
        if debug_level is not None:
            ConfigAPI().set('debug_level', int(debug_level))

    def tearDown(self):
        # Reset debug level after each test
        ConfigAPI().set('debug_level', 0)

    def test_write_and_read_memory(self):
        buf = HybridBufferedStream(mode='w+b', max_memory_size=1024)
        data = b"abc123"
        buf.write(data)
        buf.seek(0)
        self.assertEqual(buf.read(), data)
        buf.close()

    def test_rollover_to_tempfile(self):
        buf = HybridBufferedStream(mode='w+b', max_memory_size=16)
        chunk = b"x" * 8
        buf.write(chunk)
        self.assertFalse(buf._using_tempfile)
        buf.write(chunk)
        self.assertTrue(buf._using_tempfile)
        buf.seek(0)
        self.assertEqual(buf.read(), chunk + chunk)
        buf.close()

    def test_large_write_and_get_bytes(self):
        buf = HybridBufferedStream(mode='w+b', max_memory_size=32)
        data = b"y" * 1024 * 1024
        buf.write(data)
        self.assertTrue(buf._using_tempfile)
        buf.seek(0)
        self.assertEqual(buf.read(), data)
        # get_bytes returns all data
        self.assertEqual(buf.get_bytes(), data)
        buf.close()

    def test_flush_and_close(self):
        buf = HybridBufferedStream(mode='w+b', max_memory_size=64)
        buf.write(b"test")
        buf.flush()
        buf.close()
        self.assertTrue(buf.closed)

    def test_global_flush_all(self):
        buf1 = HybridBufferedStream(mode='w+b', max_memory_size=32)
        buf2 = HybridBufferedStream(mode='w+b', max_memory_size=32)
        buf1.write(b"a")
        buf2.write(b"b")
        HybridBufferedStream.flush_all()
        self.assertTrue(buf1.closed)
        self.assertTrue(buf2.closed)

    def test_text_mode(self):
        buf = HybridBufferedStream(mode='w+t', max_memory_size=32)
        buf.write("hello world")
        buf.seek(0)
        self.assertEqual(buf.read(), "hello world")
        buf.close()

    def test_extreme_write_seek_read_interleaved(self):
        buf = HybridBufferedStream(mode='w+b', max_memory_size=32)
        buf.write(b"abcdefghij")
        buf.seek(0)
        self.assertEqual(buf.read(), b"abcdefghij")
        buf.seek(5)
        buf.write(b"XYZ")
        buf.seek(0)
        self.assertEqual(buf.read(), b"abcdeXYZij")
        buf.close()

    def test_extreme_rollover_boundary_and_read(self):
        buf = HybridBufferedStream(mode='w+b', max_memory_size=16)
        buf.write(b"a" * 15)
        buf.seek(0)
        self.assertEqual(buf.read(), b"a" * 15)
        buf.write(b"b" * 10)  # triggers rollover
        buf.seek(0)
        self.assertEqual(buf.read(), b"a" * 15 + b"b" * 10)
        buf.close()

    def test_extreme_write_read_seek_write_read(self):
        buf = HybridBufferedStream(mode='w+b', max_memory_size=16)
        buf.write(b"12345678")
        buf.seek(0)
        self.assertEqual(buf.read(), b"12345678")
        buf.seek(4)
        buf.write(b"ABCD")
        buf.seek(0)
        self.assertEqual(buf.read(), b"1234ABCD")
        buf.write(b"ZZZZZZZZZZZZZZZZ")  # triggers rollover
        buf.seek(0)
        self.assertTrue(buf._using_tempfile)
        self.assertEqual(buf.read(), b"1234ABCDZZZZZZZZZZZZZZZZ")
        buf.close()

    def test_extreme_partial_write_and_read(self):
        buf = HybridBufferedStream(mode='w+b', max_memory_size=16)
        buf.write(b"foo")
        buf.seek(0)
        self.assertEqual(buf.read(2), b"fo")
        # Overwrite at current pointer (should match BytesIO behavior)
        buf.write(b"barbazquxquux")  # triggers rollover
        buf.seek(0)
        self.assertEqual(buf.read(), b'fobarbazquxquux')
        # Now test append-after-seek-to-end
        buf = HybridBufferedStream(mode='w+b', max_memory_size=16)
        buf.write(b"foo")
        buf.seek(0)
        self.assertEqual(buf.read(2), b"fo")
        buf.seek(0, 2)  # seek to end
        buf.write(b"barbazquxquux")
        buf.seek(0)
        self.assertEqual(buf.read(), b"foobarbazquxquux")
        buf.close()

    def test_extreme_text_mode(self):
        buf = HybridBufferedStream(mode='w+t', max_memory_size=16)
        buf.write("hello world")
        buf.seek(0)
        self.assertEqual(buf.read(), "hello world")
        buf.seek(6)
        buf.write("ARCFS!")
        buf.seek(0)
        self.assertEqual(buf.read(), "hello ARCFS!")
        buf.write("x" * 100)  # triggers rollover
        buf.seek(0)
        self.assertTrue(buf._using_tempfile)
        self.assertIn("ARCFS!", buf.read())
        buf.close()

    def test_cleanup_tempfile_on_close(self):
        buf = HybridBufferedStream(mode='w+b', max_memory_size=8)
        buf.write(b"x" * 16)
        temp_path = buf._tempfile.name if buf._using_tempfile else None
        buf.close()
        if temp_path:
            self.assertFalse(os.path.exists(temp_path))

if __name__ == "__main__":
    unittest.main()
