#!/usr/bin/python3

import hashlib
from collections import defaultdict
from concurrent.futures import ProcessPoolExecutor, as_completed
from functools import lru_cache
import os
from pathlib import Path
from typing import Callable, Sequence, Mapping, Optional, Tuple

import numpy as np
import matplotlib.pyplot as plt
from tqdm import tqdm

from photoclassify.config import config as cfg
from photoclassify.photopath import PhotoPath


def _ensure_paths(origin, destination):
    if origin is None:
        origin = cfg.path.origin
    if destination is None:
        destination = cfg.path.destination
    if isinstance(origin, str):
        origin = Path(origin)
    if isinstance(destination, str):
        destination = Path(destination)
    assert isinstance(origin, Path) and isinstance(destination, Path)
    return origin, destination

def _get_photopaths(
    origin: Optional[Path] = None,
    destination: Optional[Path] = None
) -> Tuple[Sequence[PhotoPath], Sequence[PhotoPath]]:
    origin, destination = _ensure_paths(origin, destination)
    paths1 = []
    paths2 = []
    print("Collecting paths...")
    print(origin)
    print(destination)
    for root, _, files in os.walk(origin):
        for file in files:
            paths1.append(PhotoPath.from_path(Path(root) / file))
    for root, _, files in os.walk(destination):
        for file in files:
            paths2.append(PhotoPath.from_path(Path(root) / file))
    return paths1, paths2


def _same_name(path1: PhotoPath, path2: PhotoPath):
    return path1.same_name(path2)

def _same_size(path1: Path, path2: Path):
    return path1.stat().st_size == path2.stat().st_size

def _same_ctime(path1: Path, path2: Path):
    return path1.stat().st_ctime == path2.stat().st_ctime

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

FUN_FILTER = [
    _same_name,
    _same_size,
]
FUN_COMPARE = compare_stream

def find_candidates(
    paths1: Sequence[PhotoPath],
    paths2: Sequence[PhotoPath],
    *funs: Sequence[Callable[[PhotoPath, PhotoPath], bool]],
) -> Mapping[PhotoPath, Sequence[PhotoPath]]:
    """
    Find and add candidate files from the destination paths that match the original paths based on specified criteria.

    Args:
        paths1 (Sequence[Path]): List of original file paths.
        paths2 (Sequence[Path]): List of destination file paths.
        *funs (Callable[[Path, Path], bool]): Functions to determine if a file in the destination is a candidate.
    """
    files_dest = {fpath.name: fpath for fpath in paths2}
    candidates = defaultdict(list)
    # WARN: Poor performance
    for orig_path in tqdm(paths1, leave=False):
        for dest_path in paths2:
            if all(fun(orig_path, dest_path) for fun in funs):
                candidates[orig_path].append(dest_path)

    return dict(candidates)

def find_twins(
    paths1: Sequence[PhotoPath],
    paths2: Sequence[PhotoPath],
    fun_filter: Callable | Sequence[Callable] | None,
    fun_compare: Callable,
    *args,
    **kwargs,
) -> Mapping[PhotoPath, Sequence[PhotoPath]]:
    """
    Find and add twin files using parallel processing.

    Args:
        paths1 (Sequence[Path]): List of original file paths.
        paths2 (Sequence[Path]): List of destination file paths.
    """
    if fun_filter is not None:
        print("Finding candidates...")
        candidates = find_candidates(paths1, paths2, *FUN_FILTER)
    else:
        candidates = {p1: paths2 for p1 in paths1}
    print("Finding twins...")

    twins = defaultdict(list)
    for p1 in tqdm(paths1):
        for p2 in candidates:
            if fun_compare(p1, p2, *args, **kwargs):
                twins[p1].append(p2)

    return dict(twins)

def find_twins_parallel(
    paths1: Sequence[PhotoPath],
    paths2: Sequence[PhotoPath],
    fun_filter: Callable | Sequence[Callable] | None,
    fun_compare: Callable,
    *args,
    max_workers: Optional[int] = None,
    **kwargs,
) -> Mapping[PhotoPath, Sequence[PhotoPath]]:
    """
    Find and add twin files using parallel processing.

    Args:
        paths1 (Sequence[PhotoPath]): List of original file paths.
        paths2 (Sequence[PhotoPath]): List of destination file paths.
    """
    if fun_filter is not None:
        print("Finding candidates...")
        candidates = find_candidates(paths1, paths2, *FUN_FILTER)
    else:
        candidates = {p1: paths2 for p1 in paths1}

    print("Finding twins in parallel...")

    twins = defaultdict(list)
    # BUG: Gives KeyError
    with ProcessPoolExecutor(max_workers=max_workers) as executor:
        futures = {
            executor.submit(fun_compare, p1, p2, *args, **kwargs): (p1, p2)
            for p1
            in paths1
            for p2
            in candidates[p1]
        }
        for future in tqdm(as_completed(futures), total=len(futures), leave=False):
            if future.result():
                p1, p2 = futures[future]
                twins[p1].append(p2)

    return dict(twins)


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
    paths_origin, paths_destination = _get_photopaths(origin, destination)
    if parallel:
        twins = find_twins_parallel(paths_origin, paths_destination, FUN_FILTER, FUN_COMPARE, max_workers=max_workers)
    else:
        twins = find_twins(paths_origin, paths_destination, FUN_FILTER, FUN_COMPARE)
    paths_origin_with_copy = [p.path for p in paths_origin if twins.get(p, [])]
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
    paths_origin, _ = _get_photopaths(origin, destination)
    paths_origin = [p.path for p in paths_origin]
    paths_with_copy = find_files_with_copy(
        origin=origin, destination=destination, parallel=parallel, max_workers=max_workers
    )
    return [path for path in paths_origin if path not in paths_with_copy]

def _make_histogram(
    paths1,
    paths2,
    twins: Mapping[Path, Sequence[Path]],
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
    input_twins = [p for p in paths1 if twins.get(p, [])]
    input_no_twins = [p for p in paths1 if p not in input_twins]
    # EQUIVALENT:
    # output_twins = [p for p in paths2 if any(
    #     p in twins.get(po, []) for po in paths1
    # )]
    output_twins = [p for p in paths2 if any(p in lst for lst in twins.values())]
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
    paths_origin, paths_destination = _get_photopaths(origin, destination)
    if parallel:
        twins = find_twins_parallel(paths_origin, paths_destination, FUN_FILTER, FUN_COMPARE, max_workers=max_workers)
    else:
        twins = find_twins(paths_origin, paths_destination, FUN_FILTER, FUN_COMPARE)
    fig = _make_histogram(
        paths_origin,
        paths_destination,
        twins=twins,
        nbins=nbins,
        split_input=split_input,
        filter_output=filter_output,
        stacked=stacked,
    )
    if fname:
        fig.savefig(fname)
    return fig


def _write_twins(
    twins: Mapping[Path, Sequence[Path]],
    fname: str | Path,
    line_numbers: bool = False,
    level_two: bool = False,
):
    def get_line(number, path, l2):
        main_line = (
            f"\t{number:>4d}. {path}"
            if number
            else "\t" + path
        )
        secondary_lines = (
            "\n" + "\n".join(f"\t\t\t\t- {el}" for el in getattr(path, l2))
            if l2
            else ""
        )
        return main_line + secondary_lines + "\n"

    def writelines(buffer, sequence, l2):
        if line_numbers:
            buffer.writelines(
                get_line(i, path, l2) for i, path in enumerate(sequence, start=1)
            )
        else:
            buffer.writelines(
                get_line(None, path, l2) for path in sequence
            )

    def writesequence(buffer, sequence, name, l2):
        buffer.write(f"\n\n{name}:\n{'=' * (len(name) + 1)}\n")
        writelines(buffer, sequence, l2)

    with open(fname, "w", encoding="utf-8") as wf:
        with_twins = [path for path, lst in twins.items() if lst]
        without_twins = [path for path, lst in twins.items() if not lst]
        writesequence(
            wf, with_twins, "With twins",
            "twins" if level_two else None
        )
        writesequence(wf, without_twins, "Without twins", None)

def report(
    origin: Path,
    destination: Path,
    fname: str | Path,
    which: str = "both",
    parallel=True,
    level_two: bool = False,
    max_workers=None,
) -> None:

    if which.lower() not in {"c", "b", "t", "candidates", "twins", "both"}:
        raise ValueError(
            "'which' must be one of: 'candidates', 'twins', 'both'."
        )

    paths_origin, paths_destination = _get_photopaths(origin, destination)

    if which.lower() in {"c", "b", "candidates", "both"}:
        candidates = find_candidates(paths_origin, paths_destination, FUN_FILTER)
        _write_twins(candidates, fname, line_numbers=True, level_two=level_two)
    if which.lower() in {"t", "b", "twins", "both"}:
        if parallel:
            twins = find_twins_parallel(paths_origin, paths_destination, FUN_FILTER, FUN_COMPARE, max_workers=max_workers)
        else:
            twins = find_twins(paths_origin, paths_destination, FUN_FILTER, FUN_COMPARE)
        _write_twins(twins, fname, line_numbers=True, level_two=level_two)


if __name__ == "__main__":
    import sys
    try:
        origin, destination, fname = sys.argv[1:]
    except ValueError:
        print(
            "Please, call using 'python3 diff.py <ORIGIN> <DESTINATION> <FNAME>'."
        )
        sys.exit(1)

    report(
        origin=Path(origin),
        destination=Path(destination),
        fname=Path(fname),
        which="both",
        parallel=False,
        max_workers=None,
        level_two=True,
    )

