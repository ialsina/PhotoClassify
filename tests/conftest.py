"""Shared fixtures for testing."""

import os
import tempfile
from pathlib import Path
import pytest


@pytest.fixture(scope="session")
def test_data_dir():
    """Create a temporary directory for test data."""
    with tempfile.TemporaryDirectory() as temp_dir:
        yield temp_dir


@pytest.fixture(scope="session")
def sample_photos(test_data_dir):
    """Create sample photo files for testing."""
    photos = []
    for i in range(5):
        photo_path = os.path.join(test_data_dir, f"photo{i}.jpg")
        with open(photo_path, "wb") as f:
            f.write(os.urandom(1024))  # Random content
        photos.append(photo_path)
    return photos


@pytest.fixture(scope="session")
def sample_config(test_data_dir):
    """Create a sample configuration file."""
    config_path = os.path.join(test_data_dir, "config.yaml")
    with open(config_path, "w") as f:
        f.write(
            """
        LOCAL:
          paths:
            - /test/local1
            - /test/local2
        REMOTE:
          paths:
            - /test/remote1
            - /test/remote2
        """
        )
    return config_path


@pytest.fixture(scope="function")
def temp_db():
    """Create a temporary database file."""
    with tempfile.NamedTemporaryFile(suffix=".sqlite", delete=False) as f:
        db_path = f.name
    yield db_path
    if os.path.exists(db_path):
        os.remove(db_path)


@pytest.fixture(scope="function")
def mock_logger():
    """Create a mock logger for testing."""
    import logging

    logger = logging.getLogger("test")
    logger.setLevel(logging.DEBUG)
    return logger
