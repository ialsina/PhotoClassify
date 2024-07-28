#!/usr/bin/python3
import hashlib
from concurrent.futures import ProcessPoolExecutor, as_completed
from functools import lru_cache
from pathlib import Path
from typing import Callable, Sequence, Optional

import numpy as np
import matplotlib.pyplot as plt
from tqdm import tqdm

from photoclassify.config import config as cfg
from photoclassify.photopath import PhotoPath

class RelationPath(Path):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._candidates = []
        self._twins = []

    @property
    def candidates(self):
        return self._candidates

    @candidates.setter
    def candidates(self, value):
        self._candidates = value

    @property
    def twins(self):
        return self._twins

    @twins.setter
    def twins(self, value):
        self._twins = value


def _ensure_paths(origin, destination):
    if origin is None:
        origin = cfg.path.origin
    if destination is None:
        destination = cfg.path.destination
    if isinstance(origin, str):
        origin = Path(origin)
    if isinstance(destination, str):
        destination = Path(destination)
    return origin, destination

def _get_paths(
    origin: Optional[Path] = None,
    destination: Optional[Path] = None
):
    origin, destination = _ensure_paths(origin, destination)
    paths1 = []
    paths2 = []
    print("Collecting paths...")
    try:
        for root, _, files in origin.walk():
            for file in files:
                paths1.append(RelationPath(root / file))
        for root, _, files in destination.walk():
            for file in files:
                paths2.append(root / file)
    except Exception as exc: # pylint: disable=W0718
        print(f"Error while collecting paths: {exc}")
    return paths1, paths2


def _same_size(path1: Path, path2: Path):
    return path1.stat().st_size == path2.stat().st_size

def _same_ctime(path1: Path, path2: Path):
    return path1.stat().st_ctime == path2.stat().st_ctime

def _find_add_candidates(
    paths1: Sequence[RelationPath],
    paths2: Sequence[Path],
    *funs: Callable[[Path, Path], bool]
) -> None:
    """
    Find and add candidate files from the destination paths that match the original paths based on specified criteria.

    Args:
        paths1 (Sequence[RelationPath]): List of original file paths.
        paths2 (Sequence[Path]): List of destination file paths.
        *funs (Callable[[Path, Path], bool]): Functions to determine if a file in the destination is a candidate.
    """
    files_dest = {fpath.name: fpath for fpath in paths2}
    print("Finding candidates...")

    for fpath in tqdm(paths1, leave=False):
        candidates = []
        dest_path = files_dest.get(fpath.name)
        if dest_path and all(fun(fpath, dest_path) for fun in funs):
            candidates.append(dest_path)
        fpath.candidates = candidates

def _add_twins(
    paths1: Sequence[RelationPath],
    fun_compare: Callable,
    *args,
    **kwargs,
) -> None:
    """
    Find and add twin files using parallel processing.

    Args:
        paths1 (Sequence[RelationPath]): List of original file paths.
        paths2 (Sequence[Path]): List of destination file paths.
    """
    print("Finding twins...")
    for p1 in tqdm(paths1):
        for p2 in p1.candidates:
            if fun_compare(p1, p2, *args, **kwargs):
                p1.twins.append(p2)

def _add_twins_parallel(
    paths1: Sequence[RelationPath],
    fun_compare: Callable,
    *args,
    max_workers: Optional[int] = None,
    **kwargs,
) -> None:
    """
    Find and add twin files using parallel processing.

    Args:
        paths1 (Sequence[RelationPath]): List of original file paths.
        paths2 (Sequence[Path]): List of destination file paths.
    """
    print("Finding twins in parallel...")
    with ProcessPoolExecutor(max_workers=max_workers) as executor:
        futures = {
            executor.submit(fun_compare, p1, p2, *args, **kwargs): (p1, p2)
            for p1
            in paths1
            for p2
            in p1.candidates
        }
        for future in tqdm(as_completed(futures), total=len(futures), leave=False):
            p1, p2 = futures[future]
            if future.result():
                p1.twins.append(p2)


def compare_hash(file1: Path, file2: Path) -> bool:
    """
    Compare two files by their SHA256 hash.

    Args:
        file1 (Path): The first file to compare.
        file2 (Path): The second file to compare.

    Returns:
        bool: True if files are identical, False otherwise.
    """
    return calculate_file_hash(file1) == calculate_file_hash(file2)


@lru_cache(maxsize=None)
def calculate_file_hash(filepath: Path) -> str:
    """
    Calculate the SHA256 hash of a file.

    Args:
        filepath (Path): Path to the file.

    Returns:
        str: SHA256 hash of the file.
    """
    sha256 = hashlib.sha256()
    with open(filepath, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            sha256.update(chunk)
    return sha256.hexdigest()

def compare_stream(file1: Path, file2: Path, chunk_size: int = 8192) -> bool:
    """
    Compare two files by streaming their contents and comparing chunks.

    Args:
        file1 (Path): The first file to compare.
        file2 (Path): The second file to compare.
        chunk_size (int): Size of the chunks to read from each file.

    Returns:
        bool: True if files are identical, False otherwise.
    """
    try:
        with open(file1, "rb") as f1, open(file2, "rb") as f2:
            while True:
                chunk1 = f1.read(chunk_size)
                chunk2 = f2.read(chunk_size)

                if chunk1 != chunk2:
                    return False

                if not chunk1:  # End of both files
                    return True
    except FileNotFoundError as e:
        print(f"Error: {e}")
        return False
    except IOError as e:
        print(f"Error reading files: {e}")
        return False

def find_files_with_copy(
        origin: Path,
        destination: Path,
        parallel=True,
        max_workers=None,
    ) -> Sequence[Path]:
    """
    Find files in the origin directory that have identical copies in the destination directory.

    Parameters
    ----------
    origin : Path
        Path to the origin directory.
    destination : Path
        Path to the destination directory.
    parallel : bool, optional
        Whether to use parallel processing, default is True.
    max_workers : int or None, optional
        Maximum number of worker processes for parallel processing, default is None.

    Returns
    -------
    list of Path
        List of Path objects representing files in the origin directory that have copies in the destination directory.
    """
    paths_origin, paths_destination = _get_paths(origin, destination)
    _find_add_candidates(paths_origin, paths_destination, PhotoPath.same_name, _same_size)
    if parallel:
        _add_twins_parallel(paths_origin, compare_stream, max_workers=max_workers)
    else:
        _add_twins(paths_origin, compare_stream)
    paths_origin_with_copy = [Path(p) for p in paths_origin if any(p.twins)]
    return paths_origin_with_copy

def find_files_without_copy(
        origin: Path,
        destination: Path,
        parallel=True,
        max_workers=None,
    ) -> Sequence[Path]:
    """
    Find files in the origin directory that do not have identical copies in the destination directory.

    Parameters
    ----------
    origin : Path
        Path to the origin directory.
    destination : Path
        Path to the destination directory.
    parallel : bool, optional
        Whether to use parallel processing, default is True.
    max_workers : int or None, optional
        Maximum number of worker processes for parallel processing, default is None.

    Returns
    -------
    list of Path
        List of Path objects representing files in the origin directory that do not have copies in the destination directory.
    """
    paths_origin, _ = _get_paths(origin, destination)
    paths_with_copy = find_files_with_copy(
        origin=origin, destination=destination, parallel=parallel, max_workers=max_workers
    )
    return [path for path in paths_origin if path not in paths_with_copy]

def _make_histogram(
    paths1,
    paths2,
    nbins=100,
    split_input=True,
    filter_output=True,
    stacked=True,
):
    def _add_hist(ax, lst, label):
        ax.hist(
            [
                np.array([p.stat().st_size for p in lst_el]) / 1e6
                for lst_el
                in lst
            ],
            bins=nbins,
            label=label,
            stacked=stacked,
        )
    fig, ax = plt.subplots(2, sharex=True)
    input_twins = [p for p in paths1 if p.twins]
    input_no_twins = [p for p in paths1 if p not in input_twins]
    output_twins = [p for p in paths2 if any(p in po.twins for po in paths1)]
    output_no_twins = [p for p in paths2 if p not in output_twins]
    if split_input:
        _add_hist(ax[0], [input_twins, input_no_twins], label=["Twins", "No-twins"])
    else:
        _add_hist(ax[0], [paths1], label=[""])
    if filter_output:
        _add_hist(ax[1], [output_twins], label="")
    else:
        _add_hist(ax[1], [output_twins, output_no_twins], label=["Twins", "Unrelated"])
    ax[1].set_xlabel("Size (MB)")
    ax[0].set_ylabel("File count (input)")
    ax[1].set_ylabel("File count (output)")
    return fig


def make_histogram(
    origin: Path,
    destination: Path,
    fname: Optional[str | Path] = None,
    parallel: bool = True,
    max_workers: Optional[int] = None,
    nbins: int = 100,
    split_input: bool = True,
    filter_output: bool = True,
    stacked: bool = True,
):
    """
    Create and save histograms of file sizes for files in the origin and destination directories.

    Parameters
    ----------
    origin : Path
        Path to the origin directory.
    destination : Path
        Path to the destination directory.
    fname : str or Path
        File path where the histogram image will be saved.
    nbins : int, optional
        Number of bins for the histogram, default is 100.
    split_input : bool, optional
        Whether to split input files into twins and non-twins, default is True.
    filter_output : bool, optional
        Whether to filter output files, default is True.
    stacked : bool, optional
        Whether to stack histograms, default is True.
    """
    paths_origin, paths_destination = _get_paths(origin, destination)
    _find_add_candidates(paths_origin, paths_destination, PhotoPath.same_name, _same_size)
    if parallel:
        _add_twins_parallel(paths_origin, compare_stream, max_workers=max_workers)
    else:
        _add_twins(paths_origin, compare_stream)
    fig = _make_histogram(
        paths_origin,
        paths_destination,
        nbins=nbins,
        split_input=split_input,
        filter_output=filter_output,
        stacked=stacked,
    )
    if fname:
        fig.savefig(fname)
    return fig


def _write(
    paths: Sequence[RelationPath],
    fname: str | Path,
    which: str = "both",
    line_numbers: bool = False,
):
    def writelines(buffer, sequence):
        if line_numbers:
            buffer.writelines(
                f"{i:>4d}. {p}\n" for i, p in enumerate(sequence, start=1)
            )
        else:
            buffer.writelines(f"{p}\n" for p in sequence)
    def writesequence(buffer, sequence, name):
        buffer.write(f"\n\n{name}:\n{'=' * (len(name) + 1)}\n")
        writelines(buffer, sequence)

    if which.lower() not in {"c", "b", "t", "candidates", "twins", "both"}:
        raise ValueError(
            "'which' must be one of: 'candidates', 'twins', 'both'."
        )

    with open(fname, "w", encoding="utf-8") as wf:
        if which.lower() in {"c", "b", "candidates", "both"}:
            with_candidates = [p for p in paths if any(p.candidates)]
            without_candidates = [p for p in paths if not any(p.candidates)]
            writesequence(wf, with_candidates, "With candidates")
            writesequence(wf, without_candidates, "Without candidates")
        if which.lower() in {"t", "b", "twins", "both"}:
            with_twins = [p for p in paths if any(p.twins)]
            without_twins = [p for p in paths if not any(p.twins)]
            writesequence(wf, with_twins, "With twins")
            writesequence(wf, without_twins, "Without twins")

def report(
    origin: Path,
    destination: Path,
    fname: str | Path,
    which: str = "both",
    parallel=True,
    max_workers=None,
) -> None:
    paths_origin, paths_destination = _get_paths(origin, destination)
    _find_add_candidates(paths_origin, paths_destination, PhotoPath.same_name, _same_size)
    if parallel:
        _add_twins_parallel(paths_origin, compare_stream, max_workers=max_workers)
    else:
        _add_twins(paths_origin, compare_stream)
    _write(paths_origin, fname, which=which, line_numbers=True)

