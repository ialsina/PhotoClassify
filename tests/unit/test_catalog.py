"""Unit tests for the catalog module."""

import os
import sqlite3
import tempfile
from datetime import datetime
from pathlib import Path
import pytest
from unittest.mock import patch, MagicMock

from photoclassify.catalog import Catalog, EmptyDatabaseError


@pytest.fixture
def temp_db():
    """Create a temporary database for testing."""
    with tempfile.NamedTemporaryFile(suffix=".sqlite", delete=False) as f:
        db_path = f.name
    yield db_path
    if os.path.exists(db_path):
        os.remove(db_path)


@pytest.fixture
def catalog(temp_db):
    """Create a Catalog instance with a temporary database."""
    return Catalog(db_path=temp_db)


class TestCatalogInitialization:
    """Test cases for Catalog initialization."""

    def test_singleton_pattern(self):
        """Test that Catalog follows singleton pattern."""
        cat1 = Catalog()
        cat2 = Catalog()
        assert cat1 is cat2

    def test_initialization_with_db_path(self, temp_db):
        """Test initialization with custom database path."""
        cat = Catalog(db_path=temp_db)
        assert cat.db_path == temp_db

    def test_database_creation(self, catalog):
        """Test that database tables are created on initialization."""
        # Verify tables exist
        cursor = catalog.conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tables = {row[0] for row in cursor.fetchall()}
        expected_tables = {"Hash", "Directories", "DirKinds", "TimeLoc", "Locations"}
        assert tables.issuperset(expected_tables)


class TestCatalogOperations:
    """Test cases for Catalog operations."""

    def test_add_entry(self, catalog):
        """Test adding entries to a table."""
        # Add a test entry
        catalog.add("Hash", [{"path": "/test/path", "hash": "testhash"}])

        # Verify entry was added
        cursor = catalog.conn.cursor()
        cursor.execute("SELECT * FROM Hash WHERE path = ?", ("/test/path",))
        result = cursor.fetchone()
        assert result is not None
        assert result[0] == "/test/path"
        assert result[1] == "testhash"

    def test_find_entry(self, catalog):
        """Test finding entries by path or hash."""
        # Add test entries
        catalog.add(
            "Hash",
            [
                {"path": "/test/path1", "hash": "hash1"},
                {"path": "/test/path2", "hash": "hash2"},
            ],
        )

        # Test finding by path
        result = catalog.find("/test/path1")
        assert result is not None
        assert result["path"] == "/test/path1"
        assert result["hash"] == "hash1"

        # Test finding by hash
        result = catalog.find("hash2")
        assert result is not None
        assert result["path"] == "/test/path2"
        assert result["hash"] == "hash2"

    def test_find_duplicates(self, catalog):
        """Test finding duplicate entries."""
        # Add entries with duplicate hashes
        catalog.add(
            "Hash",
            [
                {"path": "/test/path1", "hash": "duplicate"},
                {"path": "/test/path2", "hash": "duplicate"},
                {"path": "/test/path3", "hash": "unique"},
            ],
        )

        # Test grouped duplicates
        duplicates = catalog.find_duplicates(grouped=True)
        assert len(duplicates) == 1
        assert len(duplicates[0]) == 2
        assert "/test/path1" in duplicates[0]
        assert "/test/path2" in duplicates[0]

        # Test flat duplicates
        flat_duplicates = catalog.find_duplicates(grouped=False)
        assert len(flat_duplicates) == 2
        assert "/test/path1" in flat_duplicates
        assert "/test/path2" in flat_duplicates

    def test_update_paths(self, catalog, temp_dir):
        """Test updating paths in the catalog."""
        # Create test files
        test_files = []
        for i in range(3):
            path = os.path.join(temp_dir, f"test{i}.txt")
            with open(path, "w") as f:
                f.write(f"test content {i}")
            test_files.append(path)

        # Update catalog with paths
        catalog.update_paths(test_files)

        # Verify entries were added
        for path in test_files:
            result = catalog.find(path)
            assert result is not None
            assert result["path"] == path

    def test_get_directories(self, catalog):
        """Test getting directories by kind."""
        # Add test directories
        catalog.add(
            "Directories",
            [
                {
                    "path": "/test/dir1",
                    "last_modified": datetime.now(),
                    "mirror": 1,
                    "kind": "LOCAL",
                },
                {
                    "path": "/test/dir2",
                    "last_modified": datetime.now(),
                    "mirror": 1,
                    "kind": "REMOTE",
                },
            ],
        )

        # Test getting all directories
        all_dirs = catalog.get_directories()
        assert len(all_dirs) == 2

        # Test getting directories by kind
        local_dirs = catalog.get_directories("LOCAL")
        assert len(local_dirs) == 1
        assert local_dirs[0] == "/test/dir1"

    def test_empty_database_error(self, catalog):
        """Test EmptyDatabaseError is raised when appropriate."""
        # Add and then remove all entries
        catalog.add("Hash", [{"path": "/test/path", "hash": "testhash"}])
        catalog.cursor.execute("DELETE FROM Hash")
        catalog.conn.commit()

        # Test operations that should raise EmptyDatabaseError
        with pytest.raises(EmptyDatabaseError):
            catalog.find_duplicates()

    def test_synchronize(self, catalog, temp_dir):
        """Test synchronizing with configuration file."""
        # Create test config file
        config_path = os.path.join(temp_dir, "config.yaml")
        with open(config_path, "w") as f:
            f.write(
                """
            LOCAL:
              paths:
                - /test/path1
                - /test/path2
            REMOTE:
              paths:
                - /test/path3
            """
            )

        # Test synchronization
        catalog.synchronize(config_path)

        # Verify entries were added
        assert len(catalog.get_directories("LOCAL")) == 2
        assert len(catalog.get_directories("REMOTE")) == 1
