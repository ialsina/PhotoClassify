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


DEFAULT_DB_PATH = APP_DATA_PATH / "photocatalog.sqlite"

class EmptyDatabaseError(Exception): pass

class Catalog:
    _instance = None  # Singleton instance
    _DB_TABLE_COLUMN = OrderedDict({
        'Hash': (
            ('path', "TEXT NOT NULL UNIQUE"),
            ('hash', "VARCHAR(32) NOT NULL"),
        ),
        'Directories': (
            ('path', "TEXT NOT NULL UNIQUE"),
            ('last_modified', "DATETIME NOT NULL"),
            ('mirror', "INTEGER NOT NULL"),
            ('kind', "TEXT NOT NULL REFERENCES PathTags(name) ON DELETE CASCADE"),
        ),
        'DirKinds': (
            ('name', "TEXT NOT NULL UNIQUE"),
        ),
        'TimeLoc': (
            ('path', "TEXT NOT NULL REFERENCES Directories(path) ON DELETE CASCADE"),
            ('timestamp', "DATETIME NOT NULL"),
            ('location', "INTEGER NOT NULL REFERENCES Locations(id) ON DELETE CASCADE"),
        ),
        'Locations': (
            ('id', "INTEGER PRIMARY KEY AUTOINCREMENT"),
            ('name', "TEXT NOT NULL UNIQUE"),
        ),
    })


    def __new__(cls, *args, **kwargs):
        """Ensure a single instance (singleton)."""
        if cls._instance is None:
            cls._instance = super(Catalog, cls).__new__(cls)
        return cls._instance

    def __init__(self, db_path: T.Optional[Path] = None, verbose=True):
        self.db_path = db_path or DEFAULT_DB_PATH
        self.logger = get_logger(datetime.now().isoformat("_"))
        self.open = True
        self.verbose = verbose
        self.conn = sqlite3.connect(self.db_path)
        self.create()
        print(f"Database online: {self.db_path}")

    def __getattr__(self, name, /):
        try:
            return getattr(self.conn, name)
        except AttributeError:
            super().__getattribute__(name)
    
    def __len__(self):
        return len(self.get_paths())

    def add(self, table, entries):
        if not isinstance(entries, list) or not all(isinstance(entry, tuple) for entry in entries):
            raise TypeError("Entries must be a list of tuples.")
        valid_names = sorted(tup for tup in self._DB_TABLE_COLUMN.keys())
        if not table in valid_names:
            sorted_valid_names = ', '.join(f'"{element}"' for element in valid_names)
            raise ValueError(
                f"'{table}' not a valid table. Pick one of: {sorted_valid_names}."
            )
        cur = self.cursor()
        columns = len(self._DB_TABLE_COLUMN[table])
        placeholders = ", ".join(["?"] * columns)
        cmd = f"INSERT OR IGNORE INTO {table} VALUES ({placeholders});"
        self.conn.executemany(cmd, entries)
        self.conn.commit()
        self.logger.info(f"Added {len(entries)} entries to table '{table}'.")

    def create(self):
        for table, coltup in self._DB_TABLE_COLUMN.items():
            cmd = f"CREATE TABLE IF NOT EXISTS {table} ("
            for i, (col, colargs) in enumerate(coltup):
                cmd += f"{col} {colargs}"
                if i != len(coltup) - 1:
                    cmd += ", "
            cmd += ");"
            self.cursor().execute(cmd)
        self.commit()

    def find(self, identifier):
        """Find the path marked as ARCHIVE for a given file identifier (path, hash, or (date, filename))."""
        if (
            isinstance(identifier, tuple)
            and len(identifier) == 2
        ):
            # (date, filename) tuple: Search by timestamp and filename
            date, filename = identifier
            date = datetime.fromisoformat(date)
            raise NotImplementedError
            cmd = """
                SELECT path FROM Directories
                WHERE path LIKE ? AND last_modified >= ? AND last_modified < ?
                AND kind = 'ARCHIVE';
            """
            start_of_day = datetime.combine(date, datetime.min.time())
            end_of_day = start_of_day.replace(hour=23, minute=59, second=59, microsecond=999999)
            search_pattern = f"%{filename}"
            results = self.conn.execute(cmd, (search_pattern, start_of_day, end_of_day)).fetchall()
        elif isinstance(identifier, str):
            cmd = """
                SELECT path, hash FROM Hash
                WHERE (path LIKE ? OR hash = ?);
            """
            results = self.conn.execute(cmd, (f"%{identifier}%", f"{identifier}%")).fetchall()
        else:
            raise ValueError(
                "Invalid identifier. Must be path, hash, or (date, filename) tuple."
            )
        if not results:
            return None
        results = [result[0] for result in results]
        if len(results) == 1:
            return results[0]
        result_archive = [
            result
            for result
            in results
            if any(
                os.path.commonprefix((dir_, result)) == dir_
                for dir_
                in self.get_directories("ARCHIVE")
            )
        ]
        if len(result_archive) == 1:
            return result_archive[0]
        raise ValueError(
            f"Identifier '{identifier}' found {len(results)} results. Please, be more specific."
        )

    def _hash_to_path(self):
        rows = self.select("Hash")
        hash_to_path = defaultdict(list)
        for path, hsh in rows:
            hash_to_path[hsh].append(path)
        return hash_to_path

    def get_directories(self, *kinds):
        directories = dict(self.select("Directories", "path", "kind"))
        if not kinds:
            return directories
        return [directory for directory, kind in directories.items() if kind in kinds]

    def get_paths(self):
        return self.select("Hash", "path")

    def find_duplicates(self, grouped: bool = True):
        """Returns paths that share the same, non-unique hash. If grouped, return a list 
        of tuples (len greater than 1) of paths representing the same hash. 
        Else, equivalent result but flattened.
        """
        hash_to_path = self._hash_to_path()
        duplicates = [paths for paths in hash_to_path.values() if len(paths) > 1]
        if grouped:
            return duplicates
        return [path for paths in duplicates for path in paths]

    def find_idle(self, grouped: bool = False):
        """Returns paths whose hash is not in a path marked as 'ARCHIVE'. If grouped, 
        return a list of tuples (len equal or greater than 1) of paths representing the same hash. 
        Else, equivalent result but flattened.
        """
        hash_to_path = self._hash_to_path()
        archive_directories = [
            path
            for path, kind
            in self.get_directories().items()
            if kind == "ARCHIVE"
        ]
        if not archive_directories:
            warnings.warn(
                "No directories marked as 'ARCHIVE'."
            )
        idle = [
            paths
            for paths
            in hash_to_path.values()
            if not any(
                os.path.commonprefix((dir_, path)) == dir_
                for dir_, path
                in product(archive_directories, paths)
            )
        ]
        if grouped:
            return idle
        return [path for paths in idle for path in paths]
        

    def select(self, table, *cols):
        cur = self.cursor()
        if not cols:
            col_str = "*"
        else:
            col_str = ", ".join(cols)
        cur.execute(f"SELECT {col_str} FROM {table};")
        data = cur.fetchall()
        if all(len(element) == 1 for element in data):
            return [element[0] for element in data]
        return data

    def update_paths(self, paths, default_mirror=1, default_tag="LOCAL"):
        """Synchronize the Directories table with the current filesystem state."""
        # Ensure default tag exists in PathTags
        self.add("PathTags", [(default_tag,)])

        stored_paths = {row[0]: row[1] for row in self.select("Directories", "path", "last_modified")}
        current_paths = {path: os.path.getmtime(path) for path in paths}

        # Update modified files
        for path, last_modified in current_paths.items():
            if path in stored_paths:
                if stored_paths[path] != last_modified:
                    self.update("Directories", {"last_modified": last_modified}, {"path": path})
            else:
                self.add("Directories", [(path, last_modified, default_mirror, default_tag)])

        # Remove deleted files
        for path in stored_paths:
            if path not in current_paths:
                cmd = "DELETE FROM Directories WHERE path = ?"
                self.conn.execute(cmd, (path,))
                self.logger.info(f"Removed deleted path: {path}")

        self.conn.commit()
        self.logger.info(f"Directories table synchronized: {len(current_paths)} files.")

    def update_path_tags(self, tags):
        """Synchronize the PathTags table with the provided tag names."""
        stored_tags = {row[0] for row in self.select("PathTags", "name")}
        new_tags = set(tags)

        # Add new tags
        tags_to_add = new_tags - stored_tags
        self.add("PathTags", [(tag,) for tag in tags_to_add])

        # Remove obsolete tags
        tags_to_remove = stored_tags - new_tags
        for tag in tags_to_remove:
            cmd = "DELETE FROM PathTags WHERE name = ?"
            self.conn.execute(cmd, (tag,))


    def synchronize(self, config_file="paths.yaml", default_mirror=1, default_tag="LOCAL"):
        """Synchronize the database with the filesystem."""
        # Retrieve paths from configuration
        paths = get_paths(config_file=config_file)
        self.update_paths(paths, default_mirror=default_mirror, default_tag=default_tag)

        # Example additional tag logic
        additional_tags = ["LOCAL", "REMOTE", "ARCHIVE"]
        self.update_path_tags(additional_tags)

        # Update hashes for new or changed files
        stored_paths = {row[0] for row in self.select("Hash", "path")}
        new_paths = set(paths) - stored_paths

        entries_to_add = [
            (path, calculate_hash(path)) for path in tqdm(new_paths, desc="Processing files")
        ]
        self.add("Hash", [entry for entry in entries_to_add if entry[1]])  # Skip files with hash errors.

        # Log duplicates
        duplicates = self.get_duplicates()
        if duplicates:
            self.logger.info(f"Found {len(duplicates)} duplicate sets.")

def denull(value):
    if value is None:
        return 0.
    elif value == 'NULL':
        return 0.
    else:
        return value

if __name__ == "__main__":
    cat = Catalog()
