"""Unit tests for the protocols module."""

import os
import tempfile
from pathlib import Path
import pytest
from unittest.mock import patch, MagicMock

from photoclassify.protocols import (
    calculate_hash,
    check_disk_space,
    cp,
    mv,
    rm,
)


@pytest.fixture
def temp_file():
    """Create a temporary file with known content."""
    with tempfile.NamedTemporaryFile(delete=False) as f:
        f.write(b"test content")
        return f.name


@pytest.fixture
def temp_dir():
    """Create a temporary directory."""
    with tempfile.TemporaryDirectory() as d:
        yield d


class TestCalculateHash:
    """Test cases for calculate_hash function."""

    def test_calculate_hash_success(self, temp_file):
        """Test successful hash calculation."""
        # Known MD5 hash for "test content"
        expected_hash = "6f8db599de986fab7a21625b7916589c"
        assert calculate_hash(temp_file) == expected_hash

    def test_calculate_hash_nonexistent_file(self):
        """Test hash calculation for non-existent file."""
        assert calculate_hash("nonexistent.txt") is None

    def test_calculate_hash_empty_file(self, temp_dir):
        """Test hash calculation for empty file."""
        empty_file = os.path.join(temp_dir, "empty.txt")
        with open(empty_file, "w") as f:
            pass
        assert calculate_hash(empty_file) == "d41d8cd98f00b204e9800998ecf8427e"


class TestCheckDiskSpace:
    """Test cases for check_disk_space function."""

    @patch("os.statvfs")
    def test_check_disk_space_sufficient(self, mock_statvfs):
        """Test when sufficient disk space is available."""
        mock_statvfs.return_value.f_bavail = 1000
        mock_statvfs.return_value.f_frsize = 1024
        assert check_disk_space("/test/path", 500 * 1024) is True

    @patch("os.statvfs")
    def test_check_disk_space_insufficient(self, mock_statvfs):
        """Test when insufficient disk space is available."""
        mock_statvfs.return_value.f_bavail = 100
        mock_statvfs.return_value.f_frsize = 1024
        assert check_disk_space("/test/path", 500 * 1024) is False

    def test_check_disk_space_invalid_path(self):
        """Test with invalid path."""
        assert check_disk_space("/nonexistent/path", 1024) is False


class TestFileOperations:
    """Test cases for file operations (cp, mv, rm)."""

    def test_cp_success(self, temp_file, temp_dir):
        """Test successful file copy."""
        dest = os.path.join(temp_dir, "copied.txt")
        cp(temp_file, dest)
        assert os.path.exists(dest)
        assert calculate_hash(temp_file) == calculate_hash(dest)

    def test_cp_nonexistent_source(self, temp_dir):
        """Test copy with non-existent source file."""
        dest = os.path.join(temp_dir, "dest.txt")
        with pytest.raises(Exception):
            cp("nonexistent.txt", dest)

    def test_mv_success(self, temp_file, temp_dir):
        """Test successful file move."""
        dest = os.path.join(temp_dir, "moved.txt")
        mv(temp_file, dest)
        assert not os.path.exists(temp_file)
        assert os.path.exists(dest)

    def test_mv_nonexistent_source(self, temp_dir):
        """Test move with non-existent source file."""
        dest = os.path.join(temp_dir, "dest.txt")
        mv("nonexistent.txt", dest)  # Should log error but not raise
        assert not os.path.exists(dest)

    def test_rm_success(self, temp_file):
        """Test successful file removal."""
        rm(temp_file)
        assert not os.path.exists(temp_file)

    def test_rm_nonexistent_file(self):
        """Test removal of non-existent file."""
        rm("nonexistent.txt")  # Should log error but not raise
