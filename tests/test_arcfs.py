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

class TestArchiveFS(unittest.TestCase):
    """Test case for the ArchiveFS class."""
    
    def setUp(self):
        """Set up the test environment."""
        # Create a temporary directory for the tests
        self.test_dir = tempfile.mkdtemp()
        
        # Create an ArchiveFS instance
        self.fs = ArchiveFS()
        
        # Create test files and directories
        self.create_test_files()
    
    def tearDown(self):
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
        content = self.fs.read(test_file)
        self.assertEqual(content, "This is a test file.")
        
        # Test writing a file
        new_file = os.path.join(self.test_dir, "new.txt")
        self.fs.write(new_file, "This is a new file.")
        self.assertTrue(os.path.exists(new_file))
        
        # Test appending to a file
        self.fs.append(new_file, " Appended content.")
        content = self.fs.read(new_file)
        self.assertEqual(content, "This is a new file. Appended content.")
        
        # Test exists
        self.assertTrue(self.fs.exists(new_file))
        self.assertFalse(self.fs.exists(os.path.join(self.test_dir, "nonexistent.txt")))
        
        # Test get_info
        info = self.fs.get_info(new_file)
        self.assertFalse(info["is_dir"])
        self.assertEqual(info["size"], len("This is a new file. Appended content."))
    
    def test_directory_operations(self):
        """Test directory operations (mkdir, rmdir, list_dir, walk)."""
        # Test creating a directory
        new_dir = os.path.join(self.test_dir, "newdir")
        self.fs.mkdir(new_dir)
        self.assertTrue(os.path.exists(new_dir))
        self.assertTrue(os.path.isdir(new_dir))
        
        # Test creating nested directories
        nested_dir = os.path.join(self.test_dir, "nested/dirs/here")
        self.fs.mkdir(nested_dir, create_parents=True)
        self.assertTrue(os.path.exists(nested_dir))
        
        # Test listing a directory
        items = self.fs.list_dir(self.test_dir)
        self.assertIn("test.txt", items)
        self.assertIn("subdir", items)
        self.assertIn("newdir", items)
        
        # Test walking a directory
        roots = []
        dirnames = []
        filenames = []
        
        for root, dirs, files in self.fs.walk(self.test_dir):
            roots.append(root)
            dirnames.extend(dirs)
            filenames.extend(files)
        
        self.assertIn(self.test_dir, roots)
        self.assertIn("subdir", dirnames)
        self.assertIn("test.txt", filenames)
        self.assertIn("file0.txt", filenames)
        
        # Test removing a directory
        self.fs.rmdir(new_dir)
        self.assertFalse(os.path.exists(new_dir))
        
        # Test removing a directory with contents
        with self.assertRaises(OSError):
            self.fs.rmdir(os.path.join(self.test_dir, "subdir"))
        
        # Test recursive removal
        self.fs.rmdir(os.path.join(self.test_dir, "subdir"), recursive=True)
        self.assertFalse(os.path.exists(os.path.join(self.test_dir, "subdir")))
    
    def test_zip_archive_operations(self):
        """Test operations on ZIP archives."""
        # Create a ZIP archive
        zip_path = os.path.join(self.test_dir, "test.zip")
        self.fs.create_archive(zip_path)
        self.assertTrue(os.path.exists(zip_path))
        
        # Write files to the archive
        self.fs.write(f"{zip_path}/file1.txt", "File 1 in ZIP")
        self.fs.write(f"{zip_path}/file2.txt", "File 2 in ZIP")
        
        # Create a directory in the archive
        self.fs.mkdir(f"{zip_path}/subdir")
        self.fs.write(f"{zip_path}/subdir/file3.txt", "File 3 in ZIP subdir")
        
        # List the archive contents
        items = self.fs.list_dir(zip_path)
        self.assertEqual(set(items), {"file1.txt", "file2.txt", "subdir"})
        
        # Read a file from the archive
        content = self.fs.read(f"{zip_path}/file1.txt")
        self.assertEqual(content, "File 1 in ZIP")
        
        # Check if entries exist
        self.assertTrue(self.fs.exists(f"{zip_path}/file1.txt"))
        self.assertTrue(self.fs.exists(f"{zip_path}/subdir"))
        self.assertTrue(self.fs.is_dir(f"{zip_path}/subdir"))
        
        # Remove a file from the archive
        self.fs.remove(f"{zip_path}/file2.txt")
        self.assertFalse(self.fs.exists(f"{zip_path}/file2.txt"))
        
        # Extract the archive
        extract_dir = os.path.join(self.test_dir, "extracted")
        self.fs.extract_all(zip_path, extract_dir)
        self.assertTrue(os.path.exists(os.path.join(extract_dir, "file1.txt")))
        self.assertTrue(os.path.exists(os.path.join(extract_dir, "subdir/file3.txt")))
    
    def test_tar_archive_operations(self):
        """Test operations on TAR archives."""
        # Create a TAR archive
        tar_path = os.path.join(self.test_dir, "test.tar")
        self.fs.create_archive(tar_path)
        self.assertTrue(os.path.exists(tar_path))
        
        # Write files to the archive
        self.fs.write(f"{tar_path}/file1.txt", "File 1 in TAR")
        self.fs.write(f"{tar_path}/file2.txt", "File 2 in TAR")
        
        # Create a directory in the archive
        self.fs.mkdir(f"{tar_path}/subdir")
        self.fs.write(f"{tar_path}/subdir/file3.txt", "File 3 in TAR subdir")
        
        # List the archive contents
        items = self.fs.list_dir(tar_path)
        self.assertEqual(set(items), {"file1.txt", "file2.txt", "subdir"})
        
        # Read a file from the archive
        content = self.fs.read(f"{tar_path}/file1.txt")
        self.assertEqual(content, "File 1 in TAR")
    
    def test_copy_operations(self):
        """Test copy operations between files and archives."""
        # Create source and destination archives
        src_zip = os.path.join(self.test_dir, "source.zip")
        dst_zip = os.path.join(self.test_dir, "dest.zip")
        
        self.fs.create_archive(src_zip)
        self.fs.create_archive(dst_zip)
        
        # Add files to the source archive
        self.fs.write(f"{src_zip}/file1.txt", "File 1 content")
        self.fs.write(f"{src_zip}/file2.txt", "File 2 content")
        
        # Copy a file from source to destination
        self.fs.copy(f"{src_zip}/file1.txt", f"{dst_zip}/file1_copy.txt")
        
        # Verify the copy
        self.assertTrue(self.fs.exists(f"{dst_zip}/file1_copy.txt"))
        content = self.fs.read(f"{dst_zip}/file1_copy.txt")
        self.assertEqual(content, "File 1 content")
        
        # Copy a regular file to an archive
        test_file = os.path.join(self.test_dir, "test.txt")
        self.fs.copy(test_file, f"{dst_zip}/test_copy.txt")
        
        # Verify the copy
        self.assertTrue(self.fs.exists(f"{dst_zip}/test_copy.txt"))
        content = self.fs.read(f"{dst_zip}/test_copy.txt")
        self.assertEqual(content, "This is a test file.")
    
    def test_compression_formats(self):
        """Test operations on compressed files (gzip, bzip2, xz)."""
        # Create a compressed file
        gz_path = os.path.join(self.test_dir, "test.txt.gz")
        self.fs.write(gz_path, "Compressed content")
        
        # Read the compressed file
        content = self.fs.read(gz_path)
        self.assertEqual(content, "Compressed content")
        
        # Test bzip2
        bz2_path = os.path.join(self.test_dir, "test.txt.bz2")
        self.fs.write(bz2_path, "BZip2 content")
        content = self.fs.read(bz2_path)
        self.assertEqual(content, "BZip2 content")
        
        # Test xz
        xz_path = os.path.join(self.test_dir, "test.txt.xz")
        self.fs.write(xz_path, "XZ content")
        content = self.fs.read(xz_path)
        self.assertEqual(content, "XZ content")
    
    def test_batch_session(self):
        """Test batch operations for better performance."""
        # Create a test archive
        zip_path = os.path.join(self.test_dir, "batch.zip")
        self.fs.create_archive(zip_path)
        
        # Perform batch operations
        with self.fs.batch_session() as session:
            session.mkdir(f"{zip_path}/batch_dir")
            
            for i in range(5):
                session.write(f"{zip_path}/batch_dir/file{i}.txt", f"Batch file {i}")
        
        # Verify the results
        self.assertTrue(self.fs.exists(f"{zip_path}/batch_dir"))
        for i in range(5):
            file_path = f"{zip_path}/batch_dir/file{i}.txt"
            self.assertTrue(self.fs.exists(file_path))
            self.assertEqual(self.fs.read(file_path), f"Batch file {i}")
    
    def test_nested_archives(self):
        """Test operations on nested archives."""
        # Create outer archive
        outer_zip = os.path.join(self.test_dir, "outer.zip")
        self.fs.create_archive(outer_zip)
        
        # Create inner archive inside outer archive
        inner_zip = f"{outer_zip}/inner.zip"
        self.fs.create_archive(inner_zip)
        
        # Write a file to the inner archive
        file_path = f"{inner_zip}/test.txt"
        self.fs.write(file_path, "Nested archive test")
        
        # Verify the file exists and has the correct content
        self.assertTrue(self.fs.exists(file_path))
        content = self.fs.read(file_path)
        self.assertEqual(content, "Nested archive test")
        
        # List the contents of the inner archive
        items = self.fs.list_dir(inner_zip)
        self.assertEqual(items, ["test.txt"])


if __name__ == "__main__":
    unittest.main()
