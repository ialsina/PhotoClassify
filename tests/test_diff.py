import os
from pathlib import Path
import random
import tempfile
import unittest
from pprint import pprint

from photoclassify.photopath import PhotoPath
from photoclassify.diff import (
    _get_paths,
    _same_name,
    _same_size,
    compare_hash,
    compare_stream,
    find_candidates,
)


class DiffTestCase(unittest.TestCase):
    def setUp(self):
        directory = tempfile.TemporaryDirectory()
        path1 = Path(directory.name) / "f1"
        path2 = Path(directory.name) / "f2"
        path3 = Path(directory.name) / "f3"
        self.directory = directory
        self.path1 = path1
        self.path2 = path2
        self.path3 = path3
        with open(path1, "wb") as f1, open(path2, "wb") as f2, open(path3, "wb") as f3:
            bytes_1 = random.randbytes(2048)
            bytes_2 = random.randbytes(2048)
            f1.write(bytes_1)
            f2.write(bytes_1)
            f3.write(bytes_2)

    def test_hash_comparison(self):
        self.assertTrue(compare_hash(self.path1, self.path2))
        self.assertFalse(compare_hash(self.path1, self.path3))

    def test_stream_comparison(self):
        self.assertTrue(compare_stream(self.path1, self.path2))
        self.assertFalse(compare_stream(self.path1, self.path3))

    def tearDown(self):
        self.directory.cleanup()


class CandidateFindingTestCase(unittest.TestCase):
    def setUp(self):
        directory1 = tempfile.TemporaryDirectory()
        directory2 = tempfile.TemporaryDirectory()
        path1 = Path(directory1.name)
        path2 = Path(directory2.name)
        path1 = Path.home().absolute() / "tmp" / "d1"
        path2 = Path.home().absolute() / "tmp" / "d2"
        path1.mkdir(parents=True, exist_ok=True)
        path2.mkdir(parents=True, exist_ok=True)
        fotopaths1 = []
        fotopaths2 = []
        for i in range(1, 6):
            fotopath1 = PhotoPath(
                parent=path1, stem=f"testfile{i}", suffix=".txt", counter=0
            )
            content1 = random.randbytes(2048)
            fotopaths1.append(fotopath1)
            with open(fotopath1.path, "wb") as f1:
                f1.write(content1)
            for j in range(1, 4):
                fotopath2 = PhotoPath(
                    parent=path2, stem=f"testfile{i}", suffix=".txt", counter=j
                )
                fotopaths2.append(fotopath2)
                match i:
                    case 1:
                        content2 = content1
                    case 2:
                        content2 = content1 if j == 0 else random.randbytes(2048)
                    case 3:
                        content2 = content1 if j == 0 else random.randbytes(4096)
                    case 4:
                        content2 = random.randbytes(2048)
                    case 5:
                        content2 = random.randbytes(4096)
                    case _:
                        raise AssertionError
                with open(fotopath2.path, "wb") as f2:
                    f2.write(content2)
        self.directory1 = directory1
        self.directory2 = directory2
        self.path1 = path1
        self.path2 = path2
        self.fotopaths1 = fotopaths1
        self.fotopaths2 = fotopaths2

    def test_name_comparison(self):
        funs = [_same_name]
        paths_origin, paths_destination = _get_paths(self.path1, self.path2)
        manual_matches = {}
        for path in paths_origin:
            manual_matches[path] = [
                pd
                for pd in paths_destination
                if path.stem.replace("_x00", "") in pd.stem
            ]
        tested_matches = find_candidates(paths_origin, paths_destination, _same_name)
        success_name_comparison = tested_matches == manual_matches
        with open(Path.home() / "tmp" / "test_name_comparison.txt", "w") as wf:
            pprint(manual_matches, stream=wf)
            pprint(tested_matches, stream=wf)
        self.assertTrue(success_name_comparison)

    def cleanUp(self):
        pass
