"""
Unit tests for the ARCFS Zip handler.

Author: Tim Hosking
Contact: https://github.com/Munger
License: MIT
"""
import os
import tempfile
import sys, os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../src')))
import shutil
import zipfile
import pytest
from arcfs.arcfs import ArchiveFS


def make_zip(fs, path, files):
    """Create a ZIP archive at 'path' with the given files using ARCFS API only."""
    fs.files.create(path)
    with fs.files.open(path, 'wb') as zf:
        import zipfile
        with zipfile.ZipFile(zf, 'w') as zip_file:
            for name, content in files.items():
                zip_file.writestr(name, content)


@pytest.fixture(scope="function")
def temp_dir():
    d = tempfile.mkdtemp()
    yield d
    shutil.rmtree(d)

@pytest.mark.parametrize("files", [
    {"a.txt": b"", "b.txt": b"x", "c.txt": b"A"*4096},
    {"d.txt": os.urandom(1024*1024)},
])
def test_zip_write_read_binary(temp_dir, files):
    path = os.path.join(temp_dir, "test.zip")
    fs = ArchiveFS()
    fs.files.create(path)
    with fs.files.open(path, 'wb') as zf:
        with zipfile.ZipFile(zf, 'w') as zip_file:
            for name, content in files.items():
                zip_file.writestr(name, content)
    # ready to read from archive using fs.files API
    with fs.files.open(path, 'r') as zf:
        with zipfile.ZipFile(zf, 'r') as zip_file:
            for name, content in files.items():
                with zip_file.open(name, 'r') as stream:
                    assert stream.read() == content

def test_zip_seek_and_tell(temp_dir):
    path = os.path.join(temp_dir, "test.zip")
    fs = ArchiveFS()
    fs.files.create(path)
    files = {"seek.txt": b"0123456789"}
    with fs.files.open(path, 'wb') as zf:
        with zipfile.ZipFile(zf, 'w') as zip_file:
            zip_file.writestr("seek.txt", files["seek.txt"])
    make_zip(fs, path, files)
    # ready to read from archive using fs.files API
    with fs.files.open(path, 'r') as zf:
        with fs.files.open(path, 'rb') as archive:
            with zipfile.ZipFile(archive, 'r') as zip_file:
                with zip_file.open("seek.txt", 'r') as stream:
                    stream.seek(5)
                    assert stream.tell() == 5
                    assert stream.read(2) == b"56"
                    stream.seek(0)
                    assert stream.read(3) == b"012"

def test_zip_empty_file(temp_dir):
    path = os.path.join(temp_dir, "empty.zip")
    fs = ArchiveFS()
    fs.files.create(path)
    with fs.files.open(path, 'wb') as zf:
        with zipfile.ZipFile(zf, 'w') as zip_file:
            zip_file.writestr("empty.txt", b"")
    # ready to read from archive using fs.files API
    with fs.files.open(path, 'r') as zf:
        with zipfile.ZipFile(zf, 'r') as zip_file:
            with zip_file.open("empty.txt", 'r') as stream:
                assert stream.read() == b""

def test_zip_corrupted_file(temp_dir):
    path = os.path.join(temp_dir, "corrupt.zip")
    with ArchiveFS().files.open(path, "wb") as f:
        f.write(b"not a zip file")
    fs = ArchiveFS()
    # ready to read from archive using fs.files API
    with pytest.raises(Exception):
        with fs.files.open(path, 'r') as zf:
            with zipfile.ZipFile(zf, 'r') as zip_file:
                zip_file.read("fail.txt")

def test_zip_large_file_stress(temp_dir):
    path = os.path.join(temp_dir, "large.zip")
    fs = ArchiveFS()
    data = os.urandom(8 * 1024 * 1024)  # 8MB
    fs.files.create(path)
    with fs.files.open(path, 'wb') as zf:
        with zipfile.ZipFile(zf, 'w') as zip_file:
            zip_file.writestr("large.bin", data)
    # ready to read from archive using fs.files API
    with fs.files.open(path, 'r') as zf:
        with zipfile.ZipFile(zf, 'r') as zip_file:
            with zip_file.open("large.bin", 'r') as stream:
                assert stream.read() == data

def test_zip_unicode_filename(temp_dir):
    filename = "𝄞_music.txt"
    path = os.path.join(temp_dir, "unicode.zip")
    fs = ArchiveFS()
    fs.files.create(path)
    with fs.files.open(path, 'wb') as zf:
        with zipfile.ZipFile(zf, 'w') as zip_file:
            zip_file.writestr(filename, b"music")
    # ready to read from archive using fs.files API
    with fs.files.open(path, 'r') as zf:
        with zipfile.ZipFile(zf, 'r') as zip_file:
            with zip_file.open(filename, 'r') as stream:
                assert stream.read() == b"music"

def test_zip_permission_error(temp_dir):
    path = os.path.join(temp_dir, "readonly.zip")
    fs = ArchiveFS()
    fs.files.create(path)
    with fs.files.open(path, 'wb') as zf:
        with zipfile.ZipFile(zf, 'w') as zip_file:
            zip_file.writestr("data.txt", b"data")
    fs.files.chmod(path, 0o444)
    fs.files.create(path)
    with pytest.raises(Exception):
        make_zip(fs, path, {"fail.txt": b"fail"})
    fs.files.chmod(path, 0o666)

def test_zip_multiple_open_close(temp_dir):
    path = os.path.join(temp_dir, "multi.zip")
    fs = ArchiveFS()
    files = {"x.txt": b"x" * 100}
    for _ in range(10):
        fs.files.create(path)
        make_zip(fs, path, files)
        # ready to read from archive using fs.files API
        with fs.files.open(path, 'rb') as archive:
            with zipfile.ZipFile(archive, 'r') as zip_file:
                with zip_file.open("x.txt", 'r') as stream:
                    assert stream.read() == b"x" * 100
