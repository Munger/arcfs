#!/usr/bin/env python3
"""
ArcFS Example Script

This script demonstrates the basic usage of the ArcFS library.

Author: Tim Hosking
GitHub: https://github.com/Munger
"""



import os
import sys
import argparse
from arcfs import ArchiveFS


def list_contents(fs, path):
    """List the contents of a path, including archive entries."""
    print(f"
Listing contents of: {path}")
    print("-" * 50)
    
    try:
        # Get info about the path
        info = fs.get_info(path)
        if info['is_dir']:
            # List directory contents
            items = fs.list_dir(path)
            for item in items:
                item_path = f"{path}/{item}"
                item_info = fs.get_info(item_path)
                if item_info['is_dir']:
                    print(f"{item}/")
                else:
                    size = item_info['size']
                    print(f"{item} ({size} bytes)")
        else:
            # Display file info
            print(f"File: {os.path.basename(path)}")
            print(f"Size: {info['size']} bytes")
            print(f"Modified: {info['modified']}")
    except FileNotFoundError:
        print(f"Path not found: {path}")
    except Exception as e:
        print(f"Error: {e}")


def read_file(fs, path):
    """Read and display the contents of a file."""
    print(f"
Reading file: {path}")
    print("-" * 50)
    
    try:
        # Read the file contents
        content = fs.read(path)
        
        # Display the file content (limit to 500 chars if too large)
        if len(content) > 500:
            print(content[:500] + "... (truncated)")
        else:
            print(content)
    except FileNotFoundError:
        print(f"File not found: {path}")
    except Exception as e:
        print(f"Error: {e}")


def write_file(fs, path, content):
    """Write content to a file."""
    print(f"
Writing to file: {path}")
    print("-" * 50)
    
    try:
        # Write the content to the file
        fs.write(path, content)
        print(f"Successfully wrote {len(content)} bytes to {path}")
    except Exception as e:
        print(f"Error: {e}")


def extract_archive(fs, archive_path, target_dir):
    """Extract an entire archive to a directory."""
    print(f"
Extracting archive: {archive_path} to {target_dir}")
    print("-" * 50)
    
    try:
        # Extract the archive
        fs.extract_all(archive_path, target_dir)
        print(f"Successfully extracted {archive_path} to {target_dir}")
    except Exception as e:
        print(f"Error: {e}")


def create_and_populate_archive(fs, archive_path):
    """Create a new archive and add some files to it."""
    print(f"
Creating archive: {archive_path}")
    print("-" * 50)
    
    try:
        # Create the archive
        fs.create_archive(archive_path)
        print(f"Created archive: {archive_path}")
        
        # Add some files to the archive
        fs.write(f"{archive_path}/file1.txt", "This is file 1 content")
        fs.write(f"{archive_path}/file2.txt", "This is file 2 content")
        
        # Create a subdirectory and add a file
        fs.mkdir(f"{archive_path}/subdir")
        fs.write(f"{archive_path}/subdir/file3.txt", "This is file 3 in a subdirectory")
        
        print(f"Added files to {archive_path}")
    except Exception as e:
        print(f"Error: {e}")


def copy_between_archives(fs, src_archive, dst_archive):
    """Copy files between archives."""
    print(f"
Copying between archives: {src_archive} -> {dst_archive}")
    print("-" * 50)
    
    try:
        # Create destination archive if it doesn't exist
        if not fs.exists(dst_archive):
            fs.create_archive(dst_archive)
        
        # List files in source archive
        items = fs.list_dir(src_archive)
        
        # Copy each item
        for item in items:
            src_path = f"{src_archive}/{item}"
            dst_path = f"{dst_archive}/{item}"
            
            if fs.is_dir(src_path):
                print(f"Copying directory: {item}")
                fs.copy(src_path, dst_path)
            else:
                print(f"Copying file: {item}")
                fs.copy(src_path, dst_path)
        
        print(f"Successfully copied contents from {src_archive} to {dst_archive}")
    except Exception as e:
        print(f"Error: {e}")


def batch_operations(fs, archive_path):
    """Demonstrate batch operations for better performance."""
    print(f"
Performing batch operations on: {archive_path}")
    print("-" * 50)
    
    try:
        # Use a batch session to group operations
        with fs.batch_session() as session:
            # Perform multiple operations
            session.mkdir(f"{archive_path}/batch_dir")
            
            for i in range(5):
                content = f"This is batch file {i} content"
                session.write(f"{archive_path}/batch_dir/file{i}.txt", content)
            
            print(f"Added 5 files to {archive_path}/batch_dir in a batch")
        
        # List the batch directory to verify
        list_contents(fs, f"{archive_path}/batch_dir")
    except Exception as e:
        print(f"Error: {e}")


def main():
    """Main function demonstrating ArcFS features."""
    parser = argparse.ArgumentParser(description="ArcFS Example Script")
    parser.add_argument("--create", help="Create and populate an archive")
    parser.add_argument("--list", help="List contents of a path")
    parser.add_argument("--read", help="Read a file")
    parser.add_argument("--write", nargs=2, metavar=("PATH", "CONTENT"), help="Write content to a file")
    parser.add_argument("--extract", nargs=2, metavar=("ARCHIVE", "TARGET_DIR"), help="Extract an archive")
    parser.add_argument("--copy", nargs=2, metavar=("SRC", "DST"), help="Copy between archives")
    parser.add_argument("--batch", help="Perform batch operations on an archive")
    parser.add_argument("--demo", action="store_true", help="Run a full demonstration")
    
    args = parser.parse_args()
    
    # Create an ArchiveFS instance
    fs = ArchiveFS()
    
    # Process the arguments
    if args.list:
        list_contents(fs, args.list)
    elif args.read:
        read_file(fs, args.read)
    elif args.write:
        write_file(fs, args.write[0], args.write[1])
    elif args.extract:
        extract_archive(fs, args.extract[0], args.extract[1])
    elif args.create:
        create_and_populate_archive(fs, args.create)
    elif args.copy:
        copy_between_archives(fs, args.copy[0], args.copy[1])
    elif args.batch:
        batch_operations(fs, args.batch)
    elif args.demo:
        # Run a full demonstration
        test_archive = "test_archive.zip"
        test_archive2 = "test_archive2.tar.gz"
        extract_dir = "extracted_files"
        
        # Create and populate the test archive
        create_and_populate_archive(fs, test_archive)
        
        # List the contents
        list_contents(fs, test_archive)
        
        # Read a file from the archive
        read_file(fs, f"{test_archive}/file1.txt")
        
        # Create and populate another archive
        create_and_populate_archive(fs, test_archive2)
        
        # Copy between archives
        copy_between_archives(fs, test_archive, test_archive2)
        
        # List the contents of the second archive
        list_contents(fs, test_archive2)
        
        # Perform batch operations
        batch_operations(fs, test_archive)
        
        # Extract the archive
        if not os.path.exists(extract_dir):
            os.makedirs(extract_dir)
        extract_archive(fs, test_archive, extract_dir)
        
        print("
Demonstration complete!")
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
