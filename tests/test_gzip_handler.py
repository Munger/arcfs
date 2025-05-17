"""
Unit tests for the ARCFS Gzip handler.

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
from arcfs.handlers.gzip_handler import GzipHandler, GzipStream
from arcfs.api.files_api import FilesAPI
from arcfs.api.dirs_api import DirsAPI

class DummyFS:
    def __init__(self, root):
        self.files = FilesAPI()
        self.dirs = DirsAPI()

@pytest.fixture(scope="function")
def temp_dir():
    d = tempfile.mkdtemp()
    yield d
    shutil.rmtree(d)

@pytest.mark.parametrize("content", [b"", b"x", b"A"*4096, os.urandom(1024*1024)])
def test_gzip_write_read_binary(temp_dir, content):
    path = os.path.join(temp_dir, "test.gz")
    fs = DummyFS(temp_dir)
    handler = GzipHandler(path, mode="w", fs=fs)
    with handler.open_entry(handler.base_name, mode="w") as f:
        f.write(content)
    handler = GzipHandler(path, mode="r", fs=fs)
    with handler.open_entry(handler.base_name, mode="r") as f:
        assert f.read() == content

@pytest.mark.parametrize("text", ["", "a", "hello world", "𝄞 unicode", "x"*10000])
def test_gzip_write_read_text(temp_dir, text):
    path = os.path.join(temp_dir, "test.gz")
    fs = DummyFS(temp_dir)
    handler = GzipHandler(path, mode="w", fs=fs)
    with handler.open_entry(handler.base_name, mode="w") as f:
        f.write(text.encode("utf-8"))
    handler = GzipHandler(path, mode="r", fs=fs)
    with handler.open_entry(handler.base_name, mode="r") as f:
        assert f.read().decode("utf-8") == text

def test_gzip_seek_and_tell(temp_dir):
    path = os.path.join(temp_dir, "test.gz")
    fs = DummyFS(temp_dir)
    handler = GzipHandler(path, mode="w", fs=fs)
    data = b"0123456789"
    with handler.open_entry(handler.base_name, mode="w") as f:
        f.write(data)
    handler = GzipHandler(path, mode="r", fs=fs)
    with handler.open_entry(handler.base_name, mode="r") as f:
        f.seek(5)
        assert f.tell() == 5
        assert f.read(2) == b"56"
        f.seek(0)
        assert f.read(3) == b"012"

def test_gzip_empty_file(temp_dir):
    path = os.path.join(temp_dir, "empty.gz")
    fs = DummyFS(temp_dir)
    handler = GzipHandler(path, mode="w", fs=fs)
    with handler.open_entry(handler.base_name, mode="w") as f:
        pass
    handler = GzipHandler(path, mode="r", fs=fs)
    with handler.open_entry(handler.base_name, mode="r") as f:
        assert f.read() == b""

def test_gzip_corrupted_file(temp_dir):
    path = os.path.join(temp_dir, "corrupt.gz")
    with open(path, "wb") as f:
        f.write(b"not a gzip file")
    fs = DummyFS(temp_dir)
    handler = GzipHandler(path, mode="r", fs=fs)
    with pytest.raises(Exception):
        with handler.open_entry(handler.base_name, mode="r") as f:
            f.read()

def test_gzip_large_file_stress(temp_dir):
    path = os.path.join(temp_dir, "large.gz")
    fs = DummyFS(temp_dir)
    data = os.urandom(8 * 1024 * 1024)  # 8MB
    handler = GzipHandler(path, mode="w", fs=fs)
    with handler.open_entry(handler.base_name, mode="w") as f:
        f.write(data)
    handler = GzipHandler(path, mode="r", fs=fs)
    with handler.open_entry(handler.base_name, mode="r") as f:
        assert f.read() == data

def test_gzip_unicode_filename(temp_dir):
    filename = "𝄞_music.gz"
    path = os.path.join(temp_dir, filename)
    fs = DummyFS(temp_dir)
    handler = GzipHandler(path, mode="w", fs=fs)
    with handler.open_entry(handler.base_name, mode="w") as f:
        f.write(b"music")
    handler = GzipHandler(path, mode="r", fs=fs)
    with handler.open_entry(handler.base_name, mode="r") as f:
        assert f.read() == b"music"

def test_gzip_permission_error(temp_dir):
    path = os.path.join(temp_dir, "readonly.gz")
    fs = DummyFS(temp_dir)
    handler = GzipHandler(path, mode="w", fs=fs)
    with handler.open_entry(handler.base_name, mode="w") as f:
        f.write(b"data")
    os.chmod(path, 0o444)
    handler = GzipHandler(path, mode="w", fs=fs)
    with pytest.raises(Exception):
        with handler.open_entry(handler.base_name, mode="w") as f:
            f.write(b"fail")
    os.chmod(path, 0o666)

def test_gzip_multiple_open_close(temp_dir):
    path = os.path.join(temp_dir, "multi.gz")
    fs = DummyFS(temp_dir)
    for _ in range(10):
        handler = GzipHandler(path, mode="w", fs=fs)
        with handler.open_entry(handler.base_name, mode="w") as f:
            f.write(b"x" * 100)
        handler = GzipHandler(path, mode="r", fs=fs)
        with handler.open_entry(handler.base_name, mode="r") as f:
            assert f.read() == b"x" * 100
