"""
Unit tests for the ARCFS Tar handler.

Author: Tim Hosking
Contact: https://github.com/Munger
License: MIT
"""
import os
import pytest
from arcfs.arcfs import ArchiveFS

@pytest.fixture(scope="function")
def fs(tmp_path):
    return ArchiveFS()


@pytest.mark.parametrize("files", [
    {"a.txt": b"", "b.txt": b"x", "c.txt": b"A"*4096},
    {"d.txt": os.urandom(1024*1024)},
])
def test_tar_write_read_binary(fs, tmp_path, files):
    archive = str(tmp_path / "test.tar")
    # Write all files
    for name, content in files.items():
        fs.files.write(f"{archive}/{name}", content, binary=True)
    # Read all files
    for name, content in files.items():
        assert fs.files.read(f"{archive}/{name}", binary=True) == content


def test_tar_seek_and_tell(fs, tmp_path):
    archive = str(tmp_path / "test.tar")
    filename = "seek.txt"
    content = b"0123456789"
    fs.files.write(f"{archive}/{filename}", content, binary=True)
    with fs.files.open(f"{archive}/{filename}", mode="rb") as f:
        f.seek(5)
        assert f.tell() == 5
        assert f.read(2) == b"56"
        f.seek(0)
        assert f.read(3) == b"012"


def test_tar_empty_file(fs, tmp_path):
    archive = str(tmp_path / "empty.tar")
    filename = "empty.txt"
    fs.files.write(f"{archive}/{filename}", b"", binary=True)
    assert fs.files.read(f"{archive}/{filename}", binary=True) == b""


def test_tar_corrupted_file(fs, tmp_path):
    archive = str(tmp_path / "corrupt.tar")
    with open(archive, "wb") as f:
        f.write(b"not a tar file")
    with pytest.raises(Exception):
        fs.files.read(f"{archive}/fail.txt", binary=True)


def test_tar_large_file_stress(fs, tmp_path):
    archive = str(tmp_path / "large.tar")
    data = os.urandom(8 * 1024 * 1024)  # 8MB
    filename = "large.bin"
    fs.files.write(f"{archive}/{filename}", data, binary=True)
    assert fs.files.read(f"{archive}/{filename}", binary=True) == data


def test_tar_unicode_filename(fs, tmp_path):
    archive = str(tmp_path / "unicode.tar")
    filename = "𝄞_music.txt"
    fs.files.write(f"{archive}/{filename}", b"music", binary=True)
    assert fs.files.read(f"{archive}/{filename}", binary=True) == b"music"


def test_tar_permission_error(fs, tmp_path):
    archive = str(tmp_path / "readonly.tar")
    filename = "data.txt"
    fs.files.write(f"{archive}/{filename}", b"data", binary=True)
    os.chmod(archive, 0o444)
    try:
        with pytest.raises(Exception):
            fs.files.write(f"{archive}/fail.txt", b"fail", binary=True)
    finally:
        os.chmod(archive, 0o666)


def test_tar_multiple_open_close(fs, tmp_path):
    archive = str(tmp_path / "multi.tar")
    filename = "x.txt"
    content = b"x" * 100
    for _ in range(10):
        fs.files.write(f"{archive}/{filename}", content, binary=True)
        assert fs.files.read(f"{archive}/{filename}", binary=True) == content

