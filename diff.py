#!/usr/bin/python3
from concurrent.futures import ProcessPoolExecutor, as_completed
from pathlib import Path
from typing import Callable, Sequence

import numpy as np
import matplotlib.pyplot as plt
from tqdm import tqdm
from config import config as cfg

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


def _get_paths():
    paths1 = []
    paths2 = []
    print("Collecting paths...")
    try:
        for root, _, files in cfg.path.origin.walk():
            for file in files:
                paths1.append(RelationPath(root / file))
        for root, _, files in cfg.path.destination.walk():
            for file in files:
                paths2.append(root / file)
    except Exception as exc: # pylint: disable=W0718
        print(f"Error while collecting paths: {exc}")
    return paths1, paths2

def _same_name(path1: Path, path2: Path):
    return path1.name == path2.name

def _same_size(path1: Path, path2: Path):
    return path1.stat().st_size == path2.stat().st_size

def _same_ctime(path1: Path, path2: Path):
    return path1.stat().st_ctime == path2.stat().st_ctime

def find_add_candidates(
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

def add_twins_parallel(paths1: Sequence[RelationPath]) -> None:
    """
    Find and add twin files using parallel processing.

    Args:
        paths1 (Sequence[RelationPath]): List of original file paths.
        paths2 (Sequence[Path]): List of destination file paths.
    """
    print("Finding twins in parallel...")
    with ProcessPoolExecutor(max_workers=8) as executor:
        futures = {
            executor.submit(compare_files, p1, p2): (p1, p2)
            for p1
            in paths1
            for p2
            in p1.candidates
        }
        for future in tqdm(as_completed(futures), total=len(futures), leave=False):
            p1, p2 = futures[future]
            if future.result():
                p1.twins.append(p2)


def compare_files(file1: Path, file2: Path) -> bool:
    """
    Compare two files byte-by-byte.

    Args:
        file1 (Path): The first file to compare.
        file2 (Path): The second file to compare.

    Returns:
        bool: True if files are identical, False otherwise.
    """
    with open(file1, "rb") as f1, open(file2, "rb") as f2:
        return f1.read() == f2.read()

def save_histograms(
    paths1,
    paths2,
    filepath="hists.png",
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
        _add_hist(ax[0], [input_twins + input_no_twins], label=[""])
    if filter_output:
        _add_hist(ax[1], [output_twins], label="")
    else:
        _add_hist(ax[1], [output_twins, output_no_twins], label=["Twins", "Unrelated"])
    ax[1].set_xlabel("Size (MB)")
    ax[0].set_ylabel("File count (input)")
    ax[1].set_ylabel("File count (output)")
    fig.savefig(filepath)

if __name__ == "__main__":
    paths_orig, paths_dest = _get_paths()
    find_add_candidates(paths_orig, paths_dest, _same_name, _same_size)
    add_twins_parallel(paths_orig)
    save_histograms(paths_orig, paths_dest, split_input=True, filter_output=True)
    with_candidates = [p for p in paths_orig if any(p.candidates)]
    without_candidates = [p for p in paths_orig if not any(p.candidates)]
    with_twins = [p for p in paths_orig if any(p.twins)]
    without_twins = [p for p in paths_orig if not any(p.twins)]
