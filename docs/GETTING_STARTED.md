# Installation and Getting Started with ArcFS

This guide will help you install ArcFS and get started with using the library in your projects.

## Installation

### From PyPI (Recommended)

The easiest way to install ArcFS is using pip:

```bash
pip install arcfs
```

### From Source

If you want to install from source, you can clone the repository and install using pip:

```bash
git clone https://github.com/Munger/arcfs.git
cd arcfs
pip install -e .
```

## Quick Start

Here's a quick example to get you started with ArcFS:

```python
from arcfs import ArchiveFS

# Create an ArchiveFS instance
fs = ArchiveFS()

# Create a ZIP archive
fs.create_archive("example.zip")

# Write some files to the archive
fs.write("example.zip/hello.txt", "Hello, World!")
fs.write("example.zip/data.json", '{"key": "value"}')

# Create a directory in the archive
fs.mkdir("example.zip/subdir")
fs.write("example.zip/subdir/nested.txt", "This is a nested file")

# Read files from the archive
content = fs.read("example.zip/hello.txt")
print(content)  # Outputs: Hello, World!

# List contents of the archive
items = fs.list_dir("example.zip")
print(items)  # Outputs: ['hello.txt', 'data.json', 'subdir']

# List contents of a subdirectory in the archive
subitems = fs.list_dir("example.zip/subdir")
print(subitems)  # Outputs: ['nested.txt']

# Copy a file from the archive to the local filesystem
fs.copy("example.zip/hello.txt", "local_hello.txt")

# Copy a file from one archive to another
fs.create_archive("another.zip")
fs.copy("example.zip/data.json", "another.zip/data_copy.json")

# Working with compressed files
fs.write("data.txt.gz", "This is compressed with gzip")
compressed_content = fs.read("data.txt.gz")
print(compressed_content)  # Outputs: This is compressed with gzip
```

## Working with Nested Archives

ArcFS can transparently handle nested archives:

```python
from arcfs import ArchiveFS

fs = ArchiveFS()

# Create an outer archive
fs.create_archive("outer.zip")

# Create an inner archive inside the outer archive
fs.create_archive("outer.zip/inner.tar.gz")

# Write a file to the inner archive
fs.write("outer.zip/inner.tar.gz/nested.txt", "This is deeply nested")

# Read the nested file
content = fs.read("outer.zip/inner.tar.gz/nested.txt")
print(content)  # Outputs: This is deeply nested
```

## Batch Operations

For better performance when doing multiple operations on the same archive, use batch sessions:

```python
from arcfs import ArchiveFS

fs = ArchiveFS()

# Create an archive
fs.create_archive("batch_example.zip")

# Perform multiple operations in a batch
with fs.batch_session() as session:
    session.mkdir("batch_example.zip/docs")
    
    for i in range(5):
        session.write(f"batch_example.zip/docs/doc{i}.txt", f"Document {i} content")
    
    session.mkdir("batch_example.zip/images")
    session.write("batch_example.zip/images/placeholder.txt", "Image placeholder")

# The archive is only rebuilt once at the end of the session
```

## Supported Archive Formats

ArcFS supports the following archive formats:

- ZIP (.zip, .jar, .war, .ear, .apk)
- TAR (.tar)
- TAR with compression (.tar.gz, .tgz, .tar.bz2, .tbz2, .tar.xz, .txz)
- GZIP (.gz)
- BZIP2 (.bz2)
- XZ (.xz)

## Advanced Configuration

You can configure ArcFS for advanced usage:

```python
from arcfs import ArchiveFS

fs = ArchiveFS()

# Configure memory limits and temporary directory
fs.configure(
    max_buffer_size=100 * 1024 * 1024,  # 100MB max buffer
    temp_dir="/tmp/arcfs"               # Custom temp directory
)
```

## Custom Archive Handlers

If you need to support additional archive formats, you can create custom handlers:

```python
from arcfs import ArchiveFS
from arcfs.archive_handlers import ArchiveHandler

# Create a custom handler for a specialized format
class MyCustomArchiveHandler(ArchiveHandler):
    # Implement the required methods
    # ...

# Register the custom handler
fs = ArchiveFS()
fs.set_archive_handler(".custom", MyCustomArchiveHandler)

# Now you can use the custom format
fs.write("archive.custom/file.txt", "Hello world")
```

## Error Handling

ArcFS uses standard Python exceptions for error handling:

```python
from arcfs import ArchiveFS

fs = ArchiveFS()

try:
    content = fs.read("nonexistent.zip/file.txt")
except FileNotFoundError:
    print("The file or archive doesn't exist")
except IsADirectoryError:
    print("The path is a directory, not a file")
except NotADirectoryError:
    print("The path is a file, not a directory")
except PermissionError:
    print("No permission to access the file or archive")
except Exception as e:
    print(f"An error occurred: {e}")
```

## Next Steps

- Look at the [examples](./examples/) directory for more complex examples
- Check out the detailed [API documentation](./README.md#api-reference)
- Explore the [tests](./tests/) to understand how different functions work
- Contribute to the project on GitHub!
