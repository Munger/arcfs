# ArcFS: Transparent Archive File System

ArcFS is a Python library that allows you to work with archives as if they were directories. It provides a seamless way to read, write, and manage files within archives without manually handling compression or decompression.

## Features

- **Transparent Archive Access**: Treat archives as virtual directories
- **Nested Archive Support**: Access files within archives that are inside other archives
- **Automatic Compression/Decompression**: Work with compressed files without manual processing
- **Streaming I/O**: Efficient streaming for large files without unnecessary extraction
- **Familiar API**: Standard file system operations that work across regular files and archives
- **Batch Operations**: Optimize multiple operations on the same archive

## Installation

```bash
pip install arcfs
```

## Quick Start

```python
from arcfs import ArchiveFS

# Initialize file system
fs = ArchiveFS()

# Read a file from an archive
with fs.open("data.tar.gz/config/settings.json") as f:
    content = f.read()
    
# Write to a file in an archive (will create the archive if it doesn't exist)
fs.write("backup.zip/data/log.txt", "Log entry")

# List files in an archive
files = fs.list_dir("data.tar.gz/config")

# Copy files between archives
fs.copy("data.tar.gz/config/settings.json", "backup.zip/config/settings.json")
```

## Path Specification

ArcFS uses path specifications that include archive components. Archives in the path are treated as virtual directories.

Examples:
- `archive.tar.gz/file.txt` - File inside an archive
- `archive.zip/folder/file.txt` - File inside a folder within an archive
- `archive.tar.gz/folder/nested.zip/file.txt` - File inside a nested archive
- `file.txt.gz` - Compressed file (treated as a regular file with transparent decompression)

## API Reference

### Core File Operations

#### `open(path, mode='r')`
Opens a file or archive entry with specified mode.

```python
# Open a file for reading
with fs.open("archive.tar.gz/config.ini", "r") as f:
    content = f.read()
    
# Open a file for writing
with fs.open("archive.zip/logs/app.log", "w") as f:
    f.write("Log entry")
```

Parameters:
- `path` (str): Path to the file, including any archive components
- `mode` (str): File mode ('r', 'w', 'a', 'rb', 'wb', etc.)

Returns:
- A file-like object with standard methods (read, write, seek, tell)

#### `read(path, binary=False)`
Reads entire file contents.

```python
# Read text file
content = fs.read("archive.tar.gz/config.ini")

# Read binary file
data = fs.read("archive.zip/image.png", binary=True)
```

Parameters:
- `path` (str): Path to the file
- `binary` (bool): If True, return bytes instead of string

Returns:
- File contents as string or bytes

#### `write(path, data, binary=False)`
Writes data to a file or archive entry.

```python
# Write text
fs.write("archive.tar.gz/config.ini", "key=value")

# Write binary data
fs.write("archive.zip/image.png", image_data, binary=True)
```

Parameters:
- `path` (str): Path to the file
- `data` (str or bytes): Data to write
- `binary` (bool): If True, write bytes instead of string

#### `append(path, data, binary=False)`
Appends data to existing file or archive entry.

```python
fs.append("archive.tar.gz/log.txt", "New log entry\n")
```

Parameters:
- `path` (str): Path to the file
- `data` (str or bytes): Data to append
- `binary` (bool): If True, append bytes instead of string

#### `exists(path)`
Checks if file or directory exists at specified path.

```python
if fs.exists("archive.tar.gz/config"):
    # Process directory
    
if fs.exists("archive.tar.gz/config/settings.json"):
    # Process file
```

Parameters:
- `path` (str): Path to check

Returns:
- Boolean indicating if path exists

#### `remove(path)`
Deletes file or empty directory.

```python
fs.remove("archive.tar.gz/old_config.ini")
```

Parameters:
- `path` (str): Path to remove

#### `copy(src_path, dst_path)`
Copies file or directory tree between locations.

```python
# Copy between archives
fs.copy("archive1.tar.gz/config", "archive2.zip/config")

# Copy from archive to regular file system
fs.copy("archive.tar.gz/config/settings.json", "local_settings.json")

# Copy from regular file system to archive
fs.copy("local_settings.json", "archive.tar.gz/config/settings.json")
```

Parameters:
- `src_path` (str): Source path
- `dst_path` (str): Destination path

#### `move(src_path, dst_path)`
Moves file or directory tree between locations.

```python
fs.move("archive.tar.gz/old_dir", "archive.tar.gz/new_dir")
```

Parameters:
- `src_path` (str): Source path
- `dst_path` (str): Destination path

#### `get_info(path)`
Returns metadata about file or directory.

```python
info = fs.get_info("archive.tar.gz/config/settings.json")
print(f"Size: {info['size']}, Modified: {info['modified']}")
```

Parameters:
- `path` (str): Path to get information about

Returns:
- Dictionary with metadata (size, timestamps, type, etc.)

### Directory Operations

#### `mkdir(path, create_parents=False)`
Creates directory or virtual directory within archive.

```python
# Create single directory
fs.mkdir("archive.tar.gz/logs")

# Create directory with parents
fs.mkdir("archive.tar.gz/data/config/v2", create_parents=True)
```

Parameters:
- `path` (str): Path to create
- `create_parents` (bool): If True, create parent directories if they don't exist

#### `rmdir(path, recursive=False)`
Removes directory.

```python
# Remove empty directory
fs.rmdir("archive.tar.gz/empty_dir")

# Remove directory and all contents
fs.rmdir("archive.tar.gz/data", recursive=True)
```

Parameters:
- `path` (str): Path to remove
- `recursive` (bool): If True, recursively remove all contents

#### `list_dir(path)`
Lists contents of directory or archive.

```python
# List files in directory
files = fs.list_dir("archive.tar.gz/config")
for file in files:
    print(file)
```

Parameters:
- `path` (str): Path to list

Returns:
- List of names of files and subdirectories

#### `walk(path)`
Generator yielding (root, dirs, files) tuples for directory tree.

```python
# Walk directory tree
for root, dirs, files in fs.walk("archive.tar.gz"):
    print(f"Directory: {root}")
    print(f"  Subdirectories: {dirs}")
    print(f"  Files: {files}")
```

Parameters:
- `path` (str): Starting path for walk

Returns:
- Generator yielding (root, dirs, files) tuples

#### `glob(pattern)`
Returns paths matching pattern.

```python
# Find all JSON files
json_files = fs.glob("archive.tar.gz/**/*.json")

# Find specific files across archives
config_files = fs.glob("**/*.tar.gz/config/settings.ini")
```

Parameters:
- `pattern` (str): Glob pattern to match

Returns:
- List of matching paths

### Stream Operations

#### `open_stream(path, mode='r')`
Returns file-like object optimized for streaming.

```python
# Stream large file
with fs.open_stream("archive.tar.gz/large_file.dat", "rb") as stream:
    while chunk := stream.read(8192):
        process_chunk(chunk)
```

Parameters:
- `path` (str): Path to the file
- `mode` (str): File mode ('r', 'w', 'a', 'rb', 'wb', etc.)

Returns:
- Stream object with file-like interface

#### `pipe(src_path, dst_path, buffer_size=8192)`
Streams data from source to destination.

```python
# Copy large file efficiently
fs.pipe("archive.tar.gz/large_file.dat", "backup.zip/large_file.dat")
```

Parameters:
- `src_path` (str): Source path
- `dst_path` (str): Destination path
- `buffer_size` (int): Size of buffer for streaming

### Archive-Specific Operations

#### `create_archive(path, archive_type='auto')`
Creates a new empty archive.

```python
# Create new archive
fs.create_archive("new_archive.zip")

# Create specific archive type
fs.create_archive("data.tar.gz", archive_type="tar.gz")
```

Parameters:
- `path` (str): Path to the new archive
- `archive_type` (str): Type of archive to create (auto=detect from extension)

#### `extract_all(archive_path, target_dir)`
Extracts entire archive to target directory.

```python
# Extract all contents
fs.extract_all("archive.tar.gz", "extracted_files")
```

Parameters:
- `archive_path` (str): Path to the archive
- `target_dir` (str): Directory to extract to

#### `compress_dir(source_dir, archive_path)`
Creates archive from directory contents.

```python
# Create archive from directory
fs.compress_dir("my_folder", "my_folder.zip")
```

Parameters:
- `source_dir` (str): Source directory
- `archive_path` (str): Path to the new archive

### Batch Operations

#### `batch_session()`
Returns a session object for grouping operations.

```python
# Batch multiple operations on the same archive
with fs.batch_session() as session:
    session.write("archive.tar.gz/file1.txt", "Content 1")
    session.write("archive.tar.gz/file2.txt", "Content 2")
    session.mkdir("archive.tar.gz/new_dir")
    # Archive only rebuilt once at session end
```

Returns:
- Session object with the same interface as ArchiveFS

#### `transaction(paths)`
Context manager ensuring atomicity for operations.

```python
# Ensure all operations succeed or all fail
with fs.transaction(["archive1.tar.gz", "archive2.zip"]):
    fs.copy("archive1.tar.gz/config.ini", "archive2.zip/config.ini")
    fs.remove("archive1.tar.gz/old_file.txt")
```

Parameters:
- `paths` (list): List of paths that will be modified in the transaction

### Utility Functions

#### `get_supported_formats()`
Returns list of supported archive and compression formats.

```python
formats = fs.get_supported_formats()
print("Supported archive formats:", formats)
```

Returns:
- Dictionary with supported formats

#### `is_archive_path(path)`
Checks if path contains archive components.

```python
if fs.is_archive_path("archive.tar.gz/file.txt"):
    print("Path contains archive components")
```

Parameters:
- `path` (str): Path to check

Returns:
- Boolean indicating if path contains archive components

#### `split_archive_path(path)`
Splits path into components.

```python
components = fs.split_archive_path("archive.tar.gz/dir/file.txt")
print(components)  # ('archive.tar.gz', ['dir', 'file.txt'])
```

Parameters:
- `path` (str): Path to split

Returns:
- Tuple of (physical_path, archive_components)

#### `join_path(components)`
Joins path components, handling archive boundaries.

```python
path = fs.join_path(["archive.tar.gz", "dir", "file.txt"])
print(path)  # 'archive.tar.gz/dir/file.txt'
```

Parameters:
- `components` (list): Path components to join

Returns:
- Joined path string

### Configuration

#### `config()`
Returns the configuration manager for ArcFS, allowing you to get and set global or handler-specific options.

```python
# Set global memory buffer size
fs.config().global_buffer_size.set(100*1024*1024)

# Set temporary directory for large files
fs.config().temp_dir.set("/tmp/arcfs")

# Set buffer size for ZIP archives only
fs.config().zip.buffer_size.set(8 * 1024 * 1024)  # 8MB buffer for ZIP

# Set buffer size for TAR archives only
fs.config().tar.buffer_size.set(32 * 1024 * 1024)  # 32MB buffer for TAR

# Get current buffer size for GZIP handler
print(fs.config().gzip.buffer_size.get())
```

Parameters:
- Use `.global_buffer_size` and `.temp_dir` for global settings.
- Use `.zip`, `.tar`, `.gzip`, `.bzip2`, `.xz` for handler-specific settings.

See the codebase or API docs for all available options.

#### `set_archive_handler(extension, handler_class)`
Registers custom handler for specific archive format.

```python
# Register custom handler
fs.set_archive_handler(".rar", RarArchiveHandler)
```

Parameters:
- `extension` (str): File extension for the archive format
- `handler_class` (class): Class implementing the ArchiveHandler interface

## Supported Archive Formats

- **ZIP**: .zip
- **TAR**: .tar (uncompressed)
- **TAR with compression**: .tar.gz, .tgz, .tar.bz2, .tbz2, .tar.xz, .txz
- **Simple compression**: .gz, .bz2, .xz

## Examples

### Working with Nested Archives

```python
from arcfs import ArchiveFS

fs = ArchiveFS()

# Read a file from a nested archive
content = fs.read("outer.tar.gz/data/inner.zip/config.ini")

# Write to a nested archive
fs.write("outer.tar.gz/data/inner.zip/new_file.txt", "Hello world")

# List files in a nested archive
files = fs.list_dir("outer.tar.gz/data/inner.zip")
```

### Batch Processing

```python
from arcfs import ArchiveFS

fs = ArchiveFS()

# Process multiple files efficiently
with fs.batch_session() as session:
    # Read data from one archive
    data = session.read("source.tar.gz/data.json")
    
    # Process data
    processed_data = process_data(data)
    
    # Write to another archive
    session.write("dest.zip/processed_data.json", processed_data)
    
    # Archive modifications batched for efficiency
```

### Streaming Large Files

```python
from arcfs import ArchiveFS

fs = ArchiveFS()

# Stream a large file from one archive to another
fs.pipe("source.tar.gz/large_file.dat", "dest.zip/large_file.dat")

# Process a large file in chunks
with fs.open_stream("archive.tar.gz/large_file.dat", "rb") as stream:
    while chunk := stream.read(8192):
        # Process each chunk without loading entire file
        process_chunk(chunk)
```

## Advanced Usage

### Custom Archive Handlers

```python
from arcfs import ArchiveFS, ArchiveHandler

# Create custom handler for a specialized format
class MyCustomArchiveHandler(ArchiveHandler):
    def open_entry(self, path, mode):
        # Custom implementation
        ...
    
    def list_entries(self):
        # Custom implementation
        ...
    
    # Implement other required methods

# Register the custom handler
fs = ArchiveFS()
fs.set_archive_handler(".custom", MyCustomArchiveHandler)

# Now you can use the custom format
fs.write("archive.custom/file.txt", "Hello world")
```

### Memory Optimization

```python
from arcfs import ArchiveFS

# Configure memory limits for large file processing
fs = ArchiveFS()
fs.configure(max_buffer_size=50*1024*1024)  # 50MB max buffer

# Process very large archives
for root, dirs, files in fs.walk("huge_archive.tar.gz"):
    for file in files:
        # Files are streamed, not loaded entirely into memory
        with fs.open_stream(f"{root}/{file}", "rb") as f:
            process_file_stream(f)
```

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## Author: Tim Hosking - https://github.com/Munger

## License

This project is licensed under the MIT License - see the LICENSE file for details.
