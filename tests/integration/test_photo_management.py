"""Integration tests for photo management functionality."""

import os
import shutil
import tempfile
from pathlib import Path
import pytest
from datetime import datetime

from photoclassify.catalog import Catalog
from photoclassify.protocols import calculate_hash, cp, mv, rm


@pytest.fixture
def test_env():
    """Create a test environment with sample photos and directories."""
    with tempfile.TemporaryDirectory() as temp_dir:
        # Create source and destination directories
        src_dir = os.path.join(temp_dir, "source")
        dst_dir = os.path.join(temp_dir, "destination")
        os.makedirs(src_dir)
        os.makedirs(dst_dir)

        # Create sample photos
        photos = []
        for i in range(3):
            photo_path = os.path.join(src_dir, f"photo{i}.jpg")
            with open(photo_path, "wb") as f:
                f.write(os.urandom(1024))  # Random content
            photos.append(photo_path)

        # Create a duplicate photo
        dup_path = os.path.join(src_dir, "duplicate.jpg")
        shutil.copy2(photos[0], dup_path)
        photos.append(dup_path)

        yield {
            "temp_dir": temp_dir,
            "src_dir": src_dir,
            "dst_dir": dst_dir,
            "photos": photos,
        }


@pytest.fixture
def catalog(test_env):
    """Create a Catalog instance for testing."""
    db_path = os.path.join(test_env["temp_dir"], "test.db")
    return Catalog(db_path=db_path)


class TestPhotoManagement:
    """Integration tests for photo management features."""

    def test_photo_cataloging(self, test_env, catalog):
        """Test cataloging photos and finding duplicates."""
        # Catalog the photos
        catalog.update_paths(test_env["photos"])

        # Verify photos are in catalog
        for photo in test_env["photos"]:
            entry = catalog.find(photo)
            assert entry is not None
            assert entry["path"] == photo
            assert entry["hash"] == calculate_hash(photo)

        # Verify duplicates are found
        duplicates = catalog.find_duplicates(grouped=True)
        assert len(duplicates) == 1
        assert len(duplicates[0]) == 2
        assert test_env["photos"][0] in duplicates[0]
        assert test_env["photos"][-1] in duplicates[0]

    def test_photo_organization(self, test_env, catalog):
        """Test organizing photos into directories."""
        # Create directory structure
        organized_dir = os.path.join(test_env["temp_dir"], "organized")
        os.makedirs(organized_dir)

        # Organize photos by date
        for photo in test_env["photos"]:
            # Simulate file modification time
            mod_time = datetime.now().timestamp()
            os.utime(photo, (mod_time, mod_time))

            # Create date-based directory
            date_str = datetime.fromtimestamp(mod_time).strftime("%Y-%m-%d")
            date_dir = os.path.join(organized_dir, date_str)
            os.makedirs(date_dir, exist_ok=True)

            # Move photo to date directory
            new_path = os.path.join(date_dir, os.path.basename(photo))
            mv(photo, new_path)

        # Update catalog with new organization
        catalog.update_paths(
            [os.path.join(organized_dir, f) for f in os.listdir(organized_dir)]
        )

        # Verify organization
        directories = catalog.get_directories()
        assert len(directories) > 0
        for dir_path in directories:
            assert os.path.exists(dir_path)
            assert os.path.isdir(dir_path)

    def test_photo_synchronization(self, test_env, catalog):
        """Test synchronizing photos between directories."""
        # Create configuration file
        config_path = os.path.join(test_env["temp_dir"], "config.yaml")
        with open(config_path, "w") as f:
            f.write(
                f"""
            SOURCE:
              paths:
                - {test_env["src_dir"]}
            DESTINATION:
              paths:
                - {test_env["dst_dir"]}
            """
            )

        # Synchronize catalog
        catalog.synchronize(config_path)

        # Copy photos to destination
        for photo in test_env["photos"]:
            dst_path = os.path.join(test_env["dst_dir"], os.path.basename(photo))
            cp(photo, dst_path)

        # Update catalog with destination paths
        catalog.update_paths(
            [
                os.path.join(test_env["dst_dir"], f)
                for f in os.listdir(test_env["dst_dir"])
            ]
        )

        # Verify synchronization
        src_entries = [
            e for e in catalog.get_paths() if e.startswith(test_env["src_dir"])
        ]
        dst_entries = [
            e for e in catalog.get_paths() if e.startswith(test_env["dst_dir"])
        ]
        assert len(src_entries) == len(dst_entries)

        # Verify file integrity
        for src_path, dst_path in zip(sorted(src_entries), sorted(dst_entries)):
            assert calculate_hash(src_path) == calculate_hash(dst_path)

    def test_photo_cleanup(self, test_env, catalog):
        """Test cleaning up duplicate and unused photos."""
        # Catalog all photos
        catalog.update_paths(test_env["photos"])

        # Find duplicates
        duplicates = catalog.find_duplicates(grouped=True)
        assert len(duplicates) == 1

        # Remove duplicates
        for dup_group in duplicates:
            # Keep the first photo, remove others
            for dup_path in dup_group[1:]:
                rm(dup_path)
                # Update catalog
                catalog.cursor.execute("DELETE FROM Hash WHERE path = ?", (dup_path,))
                catalog.conn.commit()

        # Verify cleanup
        remaining_duplicates = catalog.find_duplicates()
        assert len(remaining_duplicates) == 0

        # Verify remaining photos
        remaining_paths = catalog.get_paths()
        assert (
            len(remaining_paths) == len(test_env["photos"]) - 1
        )  # One duplicate removed
        for path in remaining_paths:
            assert os.path.exists(path)
