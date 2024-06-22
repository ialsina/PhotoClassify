#!/usr/bin/python3
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime, timedelta
from difflib import diff_bytes, unified_diff 
from collections import defaultdict
from pathlib import Path
import shutil
from typing import Callable, Dict, List, Optional, Mapping, Sequence

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
    
    def clean_candidates(self):
        for cand in self._candidates:
            raise NotImplementedError
    
    @property
    def twins(self):
        return self._twins
    
    @twins.setter
    def twins(self, value):
        self._twins = value


def _get_paths():
    paths_orig = []
    paths_dest = []
    print("Collecting paths...")
    try:
        for root, _, files in cfg.path.origin.walk():
            for file in files:
                paths_orig.append(RelationPath(root / file))
        for root, _, files in cfg.path.destination.walk():
            for file in files:
                paths_dest.append(root / file)
    except Exception as exc:
        print(f"Error while collecting paths: {exc}")
    return paths_orig, paths_dest

def _same_name(path1: Path, path2: Path):
    return path1.name == path2.name

def _same_size(path1: Path, path2: Path):
    return path1.stat().st_size == path2.stat().st_size

def _same_ctime(path1: Path, path2: Path):
    return path1.stat().st_ctime == path2.stat().st_ctime

def find_add_candidates(paths_orig: Sequence[RelationPath], paths_dest: Sequence[Path], *funs: Callable[[Path, Path], bool]) -> None:
    """
    Find and add candidate files from the destination paths that match the original paths based on specified criteria.

    Args:
        paths_orig (Sequence[RelationPath]): List of original file paths.
        paths_dest (Sequence[Path]): List of destination file paths.
        *funs (Callable[[Path, Path], bool]): Functions to determine if a file in the destination is a candidate.
    """
    files_dest = {fpath.name: fpath for fpath in paths_dest}
    print("Finding candidates...")

    for fpath in tqdm(paths_orig, leave=False):
        candidates = []
        dest_path = files_dest.get(fpath.name)
        if dest_path and all(fun(fpath, dest_path) for fun in funs):
            candidates.append(dest_path)
        fpath.candidates = candidates

def add_twins_parallel(paths_orig: Sequence[RelationPath], paths_dest: Sequence[Path]) -> None:
    """
    Find and add twin files using parallel processing.

    Args:
        paths_orig (Sequence[RelationPath]): List of original file paths.
        paths_dest (Sequence[Path]): List of destination file paths.
    """
    print("Finding twins in parallel...")
    with ThreadPoolExecutor(max_workers=15) as executor:
        futures = {executor.submit(compare_files, fpath, pd): (fpath, pd) for fpath in paths_orig for pd in fpath.candidates}
        for future in tqdm(as_completed(futures), total=len(futures), leave=False):
            fpath, pd = futures[future]
            if future.result():
                fpath.twins.append(pd)


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
    paths_orig,
    paths_dest,
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
    input_twins = [p for p in paths_orig if p.twins]
    input_no_twins = [p for p in paths_orig if p not in input_twins]
    output_twins = [p for p in paths_dest if any(p in po.twins for po in paths_orig)]
    output_no_twins = [p for p in paths_dest if p not in output_twins]
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
    add_twins_parallel(paths_orig, paths_dest)
    save_histograms(paths_orig, paths_dest, filter_input=True, filter_output=True)
    with_candidates = [p for p in paths_orig if any(p.candidates)]
    without_candidates = [p for p in paths_orig if not any(p.candidates)]
    with_twins = [p for p in paths_orig if any(p.twins)]
    without_twins = [p for p in paths_orig if not any(p.twins)]