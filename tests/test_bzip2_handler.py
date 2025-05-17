"""
Unit tests for the ARCFS Bzip2 handler.

Author: Tim Hosking
Contact: https://github.com/Munger
License: MIT
"""
import os
import sys, os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../src')))
import tempfile
import shutil
import pytest
from arcfs.arcfs import ArchiveFS
import bz2






@pytest.fixture(scope="function")
def temp_dir():
    d = tempfile.mkdtemp()
    yield d
    shutil.rmtree(d)

@pytest.mark.parametrize("content", [b"", b"x", b"A"*4096, os.urandom(1024*1024)])
def test_bzip2_write_read_binary(temp_dir, content):
    path = os.path.join(temp_dir, "test.bz2")
    fs = ArchiveFS()
    fs.files.create(path)
    with fs.files.open(path, mode="wb") as f:
        f.write(bz2.compress(content))
    # ready to read from archive using fs.files API
    with fs.files.open(path, mode="rb") as f:
        assert bz2.decompress(f.read()) == content

@pytest.mark.parametrize("text", ["", "a", "hello world", "𝄞 unicode", "x"*10000])
def test_bzip2_write_read_text(temp_dir, text):
    path = os.path.join(temp_dir, "test.bz2")
    fs = ArchiveFS()
    fs.files.create(path)
    with fs.files.open(path, mode="wb") as f:
        f.write(bz2.compress(text.encode("utf-8")))
    # ready to read from archive using fs.files API
    with fs.files.open(path, mode="rb") as f:
        assert bz2.decompress(f.read()).decode("utf-8") == text

def test_bzip2_seek_and_tell(temp_dir):
    path = os.path.join(temp_dir, "test.bz2")
    fs = ArchiveFS()
    fs.files.create(path)
    data = b"0123456789"
    with fs.files.open(path, mode="wb") as f:
        f.write(bz2.compress(data))
    # ready to read from archive using fs.files API
    with fs.files.open(path, mode="rb") as f:
        f.seek(5)
        assert f.tell() == 5
        assert bz2.decompress(f.read(2)) == b"56"
        f.seek(0)
        assert bz2.decompress(f.read(3)) == b"012"

def test_bzip2_empty_file(temp_dir):
    path = os.path.join(temp_dir, "empty.bz2")
    fs = ArchiveFS()
    fs.files.create(path)
    with fs.files.open(path, mode="wb") as f:
        pass
    # ready to read from archive using fs.files API
    with fs.files.open(path, mode="rb") as f:
        assert f.read() == b""

def test_bzip2_corrupted_file(temp_dir):
    path = os.path.join(temp_dir, "corrupt.bz2")
    with open(path, "wb") as f:
        f.write(b"not a bzip2 file")
    fs = ArchiveFS()
    # ready to read from archive using fs.files API
    with pytest.raises(Exception):
        with fs.files.open(path, mode="rb") as f:
            bz2.decompress(f.read())

def test_bzip2_large_file_stress(temp_dir):
    path = os.path.join(temp_dir, "large.bz2")
    fs = ArchiveFS()
    data = os.urandom(8 * 1024 * 1024)  # 8MB
    fs.files.create(path)
    with fs.files.open(path, mode="wb") as f:
        f.write(bz2.compress(data))
    # ready to read from archive using fs.files API
    with fs.files.open(path, mode="rb") as f:
        assert bz2.decompress(f.read()) == data

def test_bzip2_unicode_filename(temp_dir):
    filename = "𝄞_music.bz2"
    path = os.path.join(temp_dir, filename)
    fs = ArchiveFS()
    fs.files.create(path)
    with fs.files.open(path, mode="wb") as f:
        f.write(bz2.compress(b"music"))
    # ready to read from archive using fs.files API
    with fs.files.open(path, mode="rb") as f:
        assert bz2.decompress(f.read()) == b"music"

def test_bzip2_permission_error(temp_dir):
    path = os.path.join(temp_dir, "readonly.bz2")
    fs = ArchiveFS()
    fs.files.create(path)
    with fs.files.open(path, mode="wb") as f:
        f.write(bz2.compress(b"data"))
    os.chmod(path, 0o444)
    fs.files.create(path)
    with pytest.raises(Exception):
        with fs.files.open(path, mode="wb") as f:
            f.write(bz2.compress(b"fail"))
    os.chmod(path, 0o666)

def test_bzip2_multiple_open_close(temp_dir):
    path = os.path.join(temp_dir, "multi.bz2")
    fs = ArchiveFS()
    for _ in range(10):
        fs.files.create(path)
        with fs.files.open(path, mode="wb") as f:
            f.write(bz2.compress(b"x" * 100))
        # ready to read from archive using fs.files API
        with fs.files.open(path, mode="rb") as f:
            assert bz2.decompress(f.read()) == b"x" * 100
