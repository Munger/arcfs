"""
Unit tests for the ARCFS Xz handler.

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
import lzma






@pytest.fixture(scope="function")
def temp_dir():
    d = tempfile.mkdtemp()
    yield d
    shutil.rmtree(d)

@pytest.mark.parametrize("content", [b"", b"x", b"A"*4096, os.urandom(1024*1024)])
def test_xz_write_read_binary(temp_dir, content):
    path = os.path.join(temp_dir, "test.xz")
    fs = ArchiveFS()
    fs.files.create(path)
    with fs.files.open(path, 'wb') as f:
        with lzma.LZMAFile(f, 'w') as xz:
            xz.write(content)
    with fs.files.open(path, 'rb') as f:
        with lzma.LZMAFile(f, 'r') as xz:
            assert xz.read() == content

@pytest.mark.parametrize("text", ["", "a", "hello world", "𝄞 unicode", "x"*10000])
def test_xz_write_read_text(temp_dir, text):
    path = os.path.join(temp_dir, "test.xz")
    fs = ArchiveFS()
    fs.files.create(path)
    with fs.files.open(path, 'wb') as f:
        with lzma.LZMAFile(f, 'w') as xz:
            xz.write(text.encode("utf-8"))
    with fs.files.open(path, 'rb') as f:
        with lzma.LZMAFile(f, 'r') as xz:
            assert xz.read().decode("utf-8") == text

def test_xz_seek_and_tell(temp_dir):
    path = os.path.join(temp_dir, "test.xz")
    fs = ArchiveFS()
    fs.files.create(path)
    data = b"0123456789"
    with fs.files.open(path, 'wb') as f:
        with lzma.LZMAFile(f, 'w') as xz:
            xz.write(data)
    with fs.files.open(path, 'rb') as f:
        with lzma.LZMAFile(f, 'r') as xz:
            xz.seek(5)
            assert xz.tell() == 5
            assert xz.read(2) == b"56"
            xz.seek(0)
            assert xz.read(3) == b"012"

def test_xz_empty_file(temp_dir):
    path = os.path.join(temp_dir, "empty.xz")
    fs = ArchiveFS()
    fs.files.create(path)
    with fs.files.open(path, 'wb') as f:
        with lzma.LZMAFile(f, 'w') as xz:
            pass
    with fs.files.open(path, 'rb') as f:
        with lzma.LZMAFile(f, 'r') as xz:
            assert xz.read() == b""

def test_xz_corrupted_file(temp_dir):
    path = os.path.join(temp_dir, "corrupt.xz")
    with open(path, "wb") as f:
        f.write(b"not an xz file")
    fs = ArchiveFS()
    with pytest.raises(lzma.LZMAError):
        with fs.files.open(path, 'rb') as f:
            with lzma.LZMAFile(f, 'r') as xz:
                xz.read()

def test_xz_large_file_stress(temp_dir):
    path = os.path.join(temp_dir, "large.xz")
    fs = ArchiveFS()
    data = os.urandom(8 * 1024 * 1024)  # 8MB
    fs.files.create(path)
    with fs.files.open(path, 'wb') as f:
        with lzma.LZMAFile(f, 'w') as xz:
            xz.write(data)
    with fs.files.open(path, 'rb') as f:
        with lzma.LZMAFile(f, 'r') as xz:
            assert xz.read() == data

def test_xz_unicode_filename(temp_dir):
    filename = "𝄞_music.xz"
    path = os.path.join(temp_dir, filename)
    fs = ArchiveFS()
    fs.files.create(path)
    with fs.files.open(path, 'wb') as f:
        with lzma.LZMAFile(f, 'w') as xz:
            xz.write(b"music")
    with fs.files.open(path, 'rb') as f:
        with lzma.LZMAFile(f, 'r') as xz:
            assert xz.read() == b"music"

def test_xz_permission_error(temp_dir):
    path = os.path.join(temp_dir, "readonly.xz")
    fs = ArchiveFS()
    fs.files.create(path)
    with fs.files.open(path, 'wb') as f:
        with lzma.LZMAFile(f, 'w') as xz:
            xz.write(b"data")
    os.chmod(path, 0o444)
    fs.files.create(path)
    with pytest.raises(PermissionError):
        with fs.files.open(path, 'wb') as f:
            with lzma.LZMAFile(f, 'w') as xz:
                xz.write(b"fail")
    os.chmod(path, 0o666)

def test_xz_multiple_open_close(temp_dir):
    path = os.path.join(temp_dir, "multi.xz")
    fs = ArchiveFS()
    for _ in range(10):
        fs.files.create(path)
        with fs.files.open(path, 'wb') as f:
            with lzma.LZMAFile(f, 'w') as xz:
                xz.write(b"x" * 100)
        with fs.files.open(path, 'rb') as f:
            with lzma.LZMAFile(f, 'r') as xz:
                assert xz.read() == b"x" * 100
