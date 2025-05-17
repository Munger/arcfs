"""
Comprehensive API-level and stress tests for the Archive File System (ARCFS).

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
   import sys, os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../src')))

from arcfs import ArchiveFS
import threading
import random
import string
import time
import pytest

class TestArchiveFS(unittest.TestCase):
    """Test case for the ArchiveFS class."""
    
    def setUp(self):
        # Allow debug level to be set via environment variable for tests
        import os
        from arcfs.api.config_api import ConfigAPI
        debug_level = os.environ.get('ARCFS_DEBUG_LEVEL')
        if debug_level is not None:
            ConfigAPI().set('debug_level', int(debug_level))
        """Set up the test environment."""
        # Create a temporary directory for the tests
        import tempfile
        self.test_dir = tempfile.mkdtemp()  # Use system default temp directory
        
        # Create an ArchiveFS instance
        self.fs = ArchiveFS()
    
        # Create test files and directories
        self.create_test_files()
    
    def tearDown(self):
        # Reset debug level after each test
        from arcfs.api.config_api import ConfigAPI
        ConfigAPI().set('debug_level', 0)
        """Clean up the test environment."""
        # Remove the temporary directory
        shutil.rmtree(self.test_dir)
    
    def create_test_files(self):
        """Create test files and directories for the tests."""
        # Create a test file
        test_file = os.path.join(self.test_dir, "test.txt")
        with open(test_file, "w") as f:
            f.write("This is a test file.")
        
        # Create a test directory with files
        test_subdir = os.path.join(self.test_dir, "subdir")
        os.makedirs(test_subdir)
        
        for i in range(3):
            with open(os.path.join(test_subdir, f"file{i}.txt"), "w") as f:
                f.write(f"This is test file {i}.")
    
    def test_basic_file_operations(self):
        """Test basic file operations (read, write, exists, get_info)."""
        # Test reading a file
        test_file = os.path.join(self.test_dir, "test.txt")
        content = self.fs.files.read(test_file)
        self.assertEqual(content, "This is a test file.")
        
        # Test writing a file
        new_file = os.path.join(self.test_dir, "new.txt")
        self.fs.files.write(new_file, "This is a new file.")
        self.assertTrue(os.path.exists(new_file))
        
        # Test appending to a file
        self.fs.files.append(new_file, " Appended content.")
        content = self.fs.files.read(new_file)
        self.assertEqual(content, "This is a new file. Appended content.")
        
        # Test exists
        self.assertTrue(self.fs.files.exists(new_file))
        self.assertFalse(self.fs.files.exists(os.path.join(self.test_dir, "nonexistent.txt")))
        
        # Test get_info
        info = self.fs.files.get_info(new_file)
        self.assertFalse(info["is_dir"])
        self.assertEqual(info["size"], len("This is a new file. Appended content."))
    
    def test_directory_operations(self):
        """Test directory operations (mkdir, rmdir, list_dir, walk)."""
        # Test creating a directory
        new_dir = os.path.join(self.test_dir, "newdir")
        self.fs.dirs.mkdir(new_dir)
        self.assertTrue(os.path.exists(new_dir))
        self.assertTrue(os.path.isdir(new_dir))
        
        # Test creating nested directories
        nested_dir = os.path.join(self.test_dir, "nested/dirs/here")
        self.fs.dirs.mkdir(nested_dir, create_parents=True)
        self.assertTrue(os.path.exists(nested_dir))
        
        # Test listing a directory
        items = self.fs.dirs.list_dir(self.test_dir)
        self.assertIn("test.txt", items)
        self.assertIn("subdir", items)
        self.assertIn("newdir", items)
        
        # Test walking a directory
        roots = []
        dirnames = []
        filenames = []
        
        for root, dirs, files in self.fs.dirs.walk(self.test_dir):
            roots.append(root)
            dirnames.extend(dirs)
            filenames.extend(files)
        
        self.assertIn(self.test_dir, roots)
        self.assertIn("subdir", dirnames)
        self.assertIn("test.txt", filenames)
        self.assertIn("file0.txt", filenames)
        
        # Test removing a directory
        self.fs.dirs.rmdir(new_dir)
        self.assertFalse(os.path.exists(new_dir))
        
        # Test removing a directory with contents
        with self.assertRaises(OSError):
            self.fs.dirs.rmdir(os.path.join(self.test_dir, "subdir"))
        
        # Test recursive removal
        self.fs.dirs.rmdir(os.path.join(self.test_dir, "subdir"), recursive=True)
        self.assertFalse(os.path.exists(os.path.join(self.test_dir, "subdir")))

    def test_mkdir_p_in_archive(self):
        """Test mkdir -p (recursive parent creation) for nested directories inside a new archive."""
        zip_path = os.path.join(self.test_dir, "deepdir.zip")
        nested_dir = f"{zip_path}/a/b/c/d"
        # The archive does not exist yet
        self.assertFalse(os.path.exists(zip_path))
        # Create deeply nested directory inside the archive
        self.fs.dirs.mkdir(nested_dir, create_parents=True)
        # The archive should now exist
        self.assertTrue(os.path.exists(zip_path))
        # The deepest directory should be listable
        items = self.fs.dirs.list_dir(f"{zip_path}/a/b/c")
        self.assertIn("d", items)
        # Write a file into the deepest directory
        file_path = f"{zip_path}/a/b/c/d/testfile.txt"
        self.fs.files.write(file_path, "data")
        # Read back the file
        content = self.fs.files.read(file_path)
        self.assertEqual(content, "data")
    
    def test_zip_archive_operations(self):
        """Test operations on ZIP archives."""
        # Create a ZIP archive
        zip_path = os.path.join(self.test_dir, "test.zip")
        self.fs.files.create(zip_path)

        self.assertTrue(os.path.exists(zip_path))

        
        # Write files to the archive
        self.fs.files.write(f"{zip_path}/file1.txt", "File 1 in ZIP")
        self.fs.files.write(f"{zip_path}/file2.txt", "File 2 in ZIP")
        
        # Create a directory in the archive
        self.fs.dirs.mkdir(f"{zip_path}/subdir")
        self.fs.files.write(f"{zip_path}/subdir/file3.txt", "File 3 in ZIP subdir")
        
        # Debug: Print all entries in the ZIP file
        import zipfile
        print("\nDebug: Entries in ZIP file:")
        with zipfile.ZipFile(zip_path, 'r') as zf:
            for entry in zf.namelist():
                print(f"  - {entry}")
        
        # List the archive contents
        items = self.fs.dirs.list_dir(zip_path)
        print("\nDebug: list_dir result:", items)
        self.assertEqual(set(items), {"file1.txt", "file2.txt", "subdir"})
        
    def test_tar_archive_operations(self):
        """Test operations on TAR archives."""
        # Create a TAR archive
        tar_path = os.path.join(self.test_dir, "test.tar")
        self.fs.files.create(tar_path)
        self.assertTrue(os.path.exists(tar_path))
        
        # Write files to the archive
        self.fs.files.write(f"{tar_path}/file1.txt", "File 1 in TAR")
        self.fs.files.write(f"{tar_path}/file2.txt", "File 2 in TAR")
        
        # Create a directory in the archive
        self.fs.dirs.mkdir(f"{tar_path}/subdir")
        self.fs.files.write(f"{tar_path}/subdir/file3.txt", "File 3 in TAR subdir")
        
        # List the archive contents
        items = self.fs.dirs.list_dir(tar_path)
        self.assertEqual(set(items), {"file1.txt", "file2.txt", "subdir"})
        
        # Read a file from the archive
        content = self.fs.files.read(f"{tar_path}/file1.txt")
        self.assertEqual(content, "File 1 in TAR")
    
    def test_copy_operations(self):
        """Test copy operations between files and archives."""
        # Create source and destination archives
        src_zip = os.path.join(self.test_dir, "source.zip")
        dst_zip = os.path.join(self.test_dir, "dest.zip")
        
        self.fs.files.create(src_zip)
        self.fs.files.create(dst_zip)
        
        # Add files to the source archive
        self.fs.files.write(f"{src_zip}/file1.txt", "File 1 content")
        self.fs.files.write(f"{src_zip}/file2.txt", "File 2 content")
        
        # Copy a file from source to destination
        self.fs.files.copy(f"{src_zip}/file1.txt", f"{dst_zip}/file1_copy.txt")
        
        # Verify the copy
        self.assertTrue(self.fs.files.exists(f"{dst_zip}/file1_copy.txt"))
        content = self.fs.files.read(f"{dst_zip}/file1_copy.txt")
        self.assertEqual(content, "File 1 content")
        
        # Copy a regular file to an archive
        test_file = os.path.join(self.test_dir, "test.txt")
        self.fs.files.copy(test_file, f"{dst_zip}/test_copy.txt")
        
        # Verify the copy
        self.assertTrue(self.fs.files.exists(f"{dst_zip}/test_copy.txt"))
        content = self.fs.files.read(f"{dst_zip}/test_copy.txt")
        self.assertEqual(content, "This is a test file.")
    
    def test_compression_formats(self):
        """Test operations on compressed files (gzip, bzip2, xz)."""
        # Create and read compressed files
        for ext, content in [('.gz', 'Compressed content'), ('.bz2', 'BZip2 content'), ('.xz', 'XZ content')]:
            path = os.path.join(self.test_dir, f"test{ext}")
            self.fs.files.write(path, content)
            read_back = self.fs.files.read(path)
            self.assertEqual(read_back, content)
        # Large file stress
        for ext in ['.gz', '.bz2', '.xz']:
            path = os.path.join(self.test_dir, f"large{ext}")
            data = ''.join(random.choices(string.ascii_letters, k=2*1024*1024))
            self.fs.files.write(path, data)
            self.assertEqual(self.fs.files.read(path), data)
        # Unicode filenames
        for ext in ['.gz', '.bz2', '.xz']:
            path = os.path.join(self.test_dir, f"𝄞_music{ext}")
            self.fs.files.write(path, "music")
            self.assertEqual(self.fs.files.read(path), "music")
        # Corrupted files
        for ext in ['.gz', '.bz2', '.xz']:
            path = os.path.join(self.test_dir, f"corrupt{ext}")
            with open(path, "wb") as f:
                f.write(b"not a valid archive")
            with self.assertRaises(Exception):
                self.fs.files.read(path)
    
    def test_batch_session(self):
        """Test batch operations for better performance and correctness."""
        zip_path = os.path.join(self.test_dir, "batch.zip")
        self.fs.files.create(zip_path)
        with self.fs.batch_session() as session:
            session.mkdir(f"{zip_path}/batch_dir")
            for i in range(50):
                session.write(f"{zip_path}/batch_dir/file{i}.txt", f"Batch file {i}")
        self.assertTrue(self.fs.files.exists(f"{zip_path}/batch_dir"))
        for i in range(50):
            file_path = f"{zip_path}/batch_dir/file{i}.txt"
            self.assertTrue(self.fs.files.exists(file_path))
            self.assertEqual(self.fs.files.read(file_path), f"Batch file {i}")

    def test_nested_archives(self):
        """Test operations on nested archives, including deep nesting and unicode."""
        outer_zip = os.path.join(self.test_dir, "outer.zip")
        self.fs.files.create(outer_zip)
        inner_zip = f"{outer_zip}/𝄞_inner.zip"
        self.fs.files.create(inner_zip)
        file_path = f"{inner_zip}/test.txt"
        self.fs.files.write(file_path, "Nested archive test")
        self.assertTrue(self.fs.files.exists(file_path))
        self.assertEqual(self.fs.files.read(file_path), "Nested archive test")
        # Deep nesting
        deep_path = outer_zip
        for i in range(5):
            deep_path = f"{deep_path}/deep{i}.zip"
            self.fs.files.create(deep_path)
        file_path = f"{deep_path}/deepfile.txt"
        self.fs.files.write(file_path, "deeply nested")
        self.assertEqual(self.fs.files.read(file_path), "deeply nested")


def test_concurrent_access():
    """Stress test for concurrent archive operations."""
    import tempfile
    import shutil
    temp_dir = tempfile.mkdtemp()
    fs = ArchiveFS()
    zip_path = os.path.join(temp_dir, "concurrent.zip")
    fs.files.create(zip_path)
    def write_and_read(idx):
        fname = f"{zip_path}/file{idx}.txt"
        fs.files.write(fname, f"content {idx}")
        assert fs.files.read(fname) == f"content {idx}"
    threads = [threading.Thread(target=write_and_read, args=(i,)) for i in range(20)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    shutil.rmtree(temp_dir)

if __name__ == "__main__":
    import pytest
    pytest.main([__file__])
