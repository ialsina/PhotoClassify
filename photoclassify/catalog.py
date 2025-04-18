"""Photo catalog management system.

This module provides a SQLite-based catalog system for managing photo collections.
It handles photo metadata, directory organization, and provides functionality for
finding duplicates and managing photo locations.

Key features:
- SQLite database for efficient photo metadata storage
- Directory organization and tagging
- Duplicate photo detection
- Location tracking
- Singleton pattern for database connection management
"""

from collections import defaultdict, OrderedDict
from datetime import datetime
from itertools import product
import os
from pathlib import Path
import sqlite3
import typing as T
import warnings

from .config import APP_DATA_PATH
from .log import get_logger

# Default database path in application data directory
DEFAULT_DB_PATH = APP_DATA_PATH / "photocatalog.sqlite"


class EmptyDatabaseError(Exception):
    """Exception raised when attempting to operate on an empty database."""

    pass


class Catalog:
    """Photo catalog management system using SQLite.

    This class implements a singleton pattern to manage a single database connection
    throughout the application lifecycle. It provides methods for:
    - Photo metadata storage and retrieval
    - Directory organization and tagging
    - Duplicate photo detection
    - Location tracking

    The database schema includes tables for:
    - Hash: File paths and their MD5 hashes
    - Directories: Directory paths and metadata
    - DirKinds: Directory type definitions
    - TimeLoc: Timestamp and location data
    - Locations: Location definitions

    Attributes:
        _instance: Singleton instance of the Catalog class
        _DB_TABLE_COLUMN: Database schema definition
        db_path: Path to the SQLite database file
        verbose: Enable verbose logging
    """

    _instance = None  # Singleton instance
    _DB_TABLE_COLUMN = OrderedDict(
        {
            "Hash": (
                ("path", "TEXT NOT NULL UNIQUE"),
                ("hash", "VARCHAR(32) NOT NULL"),
            ),
            "Directories": (
                ("path", "TEXT NOT NULL UNIQUE"),
                ("last_modified", "DATETIME NOT NULL"),
                ("mirror", "INTEGER NOT NULL"),
                ("kind", "TEXT NOT NULL REFERENCES PathTags(name) ON DELETE CASCADE"),
            ),
            "DirKinds": (("name", "TEXT NOT NULL UNIQUE"),),
            "TimeLoc": (
                (
                    "path",
                    "TEXT NOT NULL REFERENCES Directories(path) ON DELETE CASCADE",
                ),
                ("timestamp", "DATETIME NOT NULL"),
                (
                    "location",
                    "INTEGER NOT NULL REFERENCES Locations(id) ON DELETE CASCADE",
                ),
            ),
            "Locations": (
                ("id", "INTEGER PRIMARY KEY AUTOINCREMENT"),
                ("name", "TEXT NOT NULL UNIQUE"),
            ),
        }
    )

    def __new__(cls, *args, **kwargs):
        """Ensure a single instance (singleton).

        This method implements the singleton pattern to ensure only one instance
        of the Catalog class exists throughout the application lifecycle.

        Returns:
            The singleton instance of the Catalog class
        """
        if cls._instance is None:
            cls._instance = super(Catalog, cls).__new__(cls)
        return cls._instance

    def __init__(self, db_path: T.Optional[Path] = None, verbose=True):
        """Initialize the catalog with database connection.

        Args:
            db_path: Optional path to the SQLite database file
            verbose: Enable verbose logging

        Note:
            If db_path is not provided, uses DEFAULT_DB_PATH
        """
        if not hasattr(self, "initialized"):
            self.db_path = db_path or DEFAULT_DB_PATH
            self.verbose = verbose
            self.logger = get_logger(__name__)
            self.initialized = True
            self._conn = None
            self._cursor = None

    def __getattr__(self, name, /):
        """Delegate unknown attributes to the database cursor.

        This allows direct access to database cursor methods through the Catalog instance.

        Args:
            name: Name of the attribute to access

        Returns:
            The requested attribute from the database cursor

        Raises:
            AttributeError: If the attribute doesn't exist on the cursor
        """
        return getattr(self.cursor, name)

    def __len__(self):
        """Return the number of entries in the Hash table.

        Returns:
            Number of photo entries in the catalog
        """
        return self.cursor.execute("SELECT COUNT(*) FROM Hash").fetchone()[0]

    def add(self, table, entries):
        """Add entries to a specified table.

        Args:
            table: Name of the table to add entries to
            entries: List of dictionaries containing column-value pairs

        Note:
            Handles both single entries and lists of entries
        """
        if not entries:
            return

        # Handle single entry case
        if isinstance(entries, dict):
            entries = [entries]

        # Get column names from schema
        columns = [col[0] for col in self._DB_TABLE_COLUMN[table]]

        # Prepare SQL statement
        placeholders = ", ".join(["?" for _ in columns])
        sql = f"INSERT OR REPLACE INTO {table} ({', '.join(columns)}) VALUES ({placeholders})"

        # Execute for each entry
        for entry in entries:
            values = [entry.get(col) for col in columns]
            self.cursor.execute(sql, values)

        self.conn.commit()

    def create(self):
        """Create the database schema if it doesn't exist.

        This method creates all necessary tables according to the schema defined
        in _DB_TABLE_COLUMN if they don't already exist.
        """
        for table, columns in self._DB_TABLE_COLUMN.items():
            # Create table if it doesn't exist
            sql = f"CREATE TABLE IF NOT EXISTS {table} ({', '.join(f'{col} {typ}' for col, typ in columns)})"
            self.cursor.execute(sql)

        self.conn.commit()

    def find(self, identifier):
        """Find a photo entry by path or hash.

        Args:
            identifier: Path or hash to search for

        Returns:
            Dictionary containing the photo entry if found, None otherwise
        """
        # Try to find by path first
        result = self.cursor.execute(
            "SELECT * FROM Hash WHERE path = ?", (str(identifier),)
        ).fetchone()

        if result:
            return dict(zip(["path", "hash"], result))

        # Try to find by hash
        result = self.cursor.execute(
            "SELECT * FROM Hash WHERE hash = ?", (identifier,)
        ).fetchone()

        if result:
            return dict(zip(["path", "hash"], result))

        return None

    def _hash_to_path(self):
        """Create a mapping of hashes to paths.

        Returns:
            Dictionary mapping MD5 hashes to file paths
        """
        return dict(self.cursor.execute("SELECT hash, path FROM Hash").fetchall())

    def get_directories(self, *kinds):
        """Get directories of specified kinds.

        Args:
            *kinds: Variable number of directory kinds to filter by

        Returns:
            List of directory paths matching the specified kinds
        """
        if not kinds:
            return [
                row[0]
                for row in self.cursor.execute(
                    "SELECT path FROM Directories"
                ).fetchall()
            ]

        placeholders = ", ".join(["?" for _ in kinds])
        return [
            row[0]
            for row in self.cursor.execute(
                f"SELECT path FROM Directories WHERE kind IN ({placeholders})", kinds
            ).fetchall()
        ]

    def get_paths(self):
        """Get all photo paths in the catalog.

        Returns:
            List of all photo paths in the catalog
        """
        return [
            row[0] for row in self.cursor.execute("SELECT path FROM Hash").fetchall()
        ]

    def find_duplicates(self, grouped: bool = True):
        """Find duplicate photos in the catalog.

        Args:
            grouped: If True, return duplicates grouped by hash

        Returns:
            List of duplicate photo entries, optionally grouped by hash
        """
        # Find all hashes that appear more than once
        duplicates = self.cursor.execute(
            """
            SELECT hash, COUNT(*) as count
            FROM Hash
            GROUP BY hash
            HAVING count > 1
        """
        ).fetchall()

        if not duplicates:
            return []

        if grouped:
            # Return duplicates grouped by hash
            result = []
            for hash_, _ in duplicates:
                paths = [
                    row[0]
                    for row in self.cursor.execute(
                        "SELECT path FROM Hash WHERE hash = ?", (hash_,)
                    ).fetchall()
                ]
                result.append(paths)
            return result
        else:
            # Return flat list of duplicate paths
            return [
                path
                for hash_, _ in duplicates
                for path, in self.cursor.execute(
                    "SELECT path FROM Hash WHERE hash = ?", (hash_,)
                ).fetchall()
            ]

    def find_idle(self, grouped: bool = False):
        """Find photos that haven't been accessed recently.

        Args:
            grouped: If True, group results by directory

        Returns:
            List of idle photo paths, optionally grouped by directory
        """
        # Get current timestamp
        now = datetime.now()

        # Find photos not accessed in the last 30 days
        idle = self.cursor.execute(
            """
            SELECT path, last_modified
            FROM Hash
            WHERE last_modified < ?
        """,
            (now,),
        ).fetchall()

        if not idle:
            return []

        if grouped:
            # Group by directory
            result = defaultdict(list)
            for path, _ in idle:
                directory = os.path.dirname(path)
                result[directory].append(path)
            return dict(result)
        else:
            return [path for path, _ in idle]

    def select(self, table, *cols):
        """Execute a SELECT query on the specified table.

        Args:
            table: Table to query
            *cols: Columns to select (defaults to all columns)

        Returns:
            List of rows matching the query
        """
        columns = ", ".join(cols) if cols else "*"
        return self.cursor.execute(f"SELECT {columns} FROM {table}").fetchall()

    def update_paths(self, paths, default_mirror=1, default_tag="LOCAL"):
        """Update the catalog with new photo paths.

        Args:
            paths: List of paths to add/update
            default_mirror: Default mirror value for new directories
            default_tag: Default tag for new directories
        """
        # Ensure directory kinds exist
        self.add("DirKinds", [{"name": default_tag}])

        # Update directories
        directories = []
        for path in paths:
            directory = os.path.dirname(path)
            directories.append(
                {
                    "path": directory,
                    "last_modified": datetime.now(),
                    "mirror": default_mirror,
                    "kind": default_tag,
                }
            )
        self.add("Directories", directories)

        # Update hashes
        hashes = []
        for path in paths:
            hashes.append({"path": path, "hash": self._calculate_hash(path)})
        self.add("Hash", hashes)

        self.conn.commit()

    def update_path_tags(self, tags):
        """Update directory tags in the catalog.

        Args:
            tags: Dictionary mapping paths to their new tags
        """
        for path, tag in tags.items():
            self.cursor.execute(
                "UPDATE Directories SET kind = ? WHERE path = ?", (tag, path)
            )
        self.conn.commit()

    def synchronize(
        self, config_file="paths.yaml", default_mirror=1, default_tag="LOCAL"
    ):
        """Synchronize the catalog with a configuration file.

        Args:
            config_file: Path to the configuration file
            default_mirror: Default mirror value for new directories
            default_tag: Default tag for new directories
        """
        # Load configuration
        with open(config_file) as f:
            config = yaml.safe_load(f)

        # Update paths from configuration
        paths = []
        for section in config.values():
            paths.extend(section.get("paths", []))
        self.update_paths(paths, default_mirror, default_tag)

        # Update tags from configuration
        tags = {}
        for section_name, section in config.items():
            for path in section.get("paths", []):
                tags[path] = section_name
        self.update_path_tags(tags)

        self.conn.commit()


def denull(value):
    """Convert None values to empty strings for database storage.

    Args:
        value: Value to convert

    Returns:
        Empty string if value is None, otherwise the original value
    """
    return "" if value is None else value


if __name__ == "__main__":
    cat = Catalog()
