#!/usr/bin/python3
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
    for root, dirs, files in cfg.path.origin.walk():
        for file in files:
            paths_orig.append(RelationPath(root / file))
    for root, dirs, files in cfg.path.destination.walk():
        for file in files:
            paths_dest.append(root / file)
    return paths_orig, paths_dest

def _add_candidates(paths_orig: Sequence[Path], paths_dest: Sequence[Path], *funs):
    files_dest = [fpath.name for fpath in paths_dest]
    stats_orig = {path: path.stat() for path in paths_orig}
    stats_dest = {path: path.stat() for path in paths_dest}

    print("Finding candidates...")
    for fpath in tqdm(paths_orig, leave=False):
        if fpath.name in files_dest:
            fpath.candidates = [
                path_dest
                for path_dest
                in paths_dest
                if all(fun(fpath, path_dest) for fun in funs)
            ]
        else:
            fpath.candidates = []

def _add_twins(paths_orig: Sequence[Path], paths_dest: Sequence[Path]):
    print("Finding twins...")
    for fpath in tqdm(paths_orig, leave=False, disable=False):
        for pd in fpath.candidates:
            with open(fpath, "rb") as a, open(pd, "rb") as b:
                delta = diff_bytes(unified_diff, a, b)
                try:
                    next(delta)
                except StopIteration:
                    fpath.twins.append(pd)

def _same_name(path1: Path, path2: Path):
    return path1.name == path2.name

def _same_size(path1: Path, path2: Path):
    return path1.stat().st_size == path2.stat().st_size

def _same_ctime(path1: Path, path2: Path):
    return path1.stat().st_ctime == path2.stat().st_ctime

def _save_hists(filepath="hists.png", nbins=100, filter_output=True):
    global paths_orig, paths_dest
    fig, ax = plt.subplots(2, sharex=True)
    if filter_output:
        paths_dest = [
            p
            for p
            in paths_dest
            if any(p in po.candidates for po in paths_orig)
        ]
    ax[0].hist(np.array([p.stat().st_size for p in paths_orig]) / 1e6, bins=nbins)
    ax[1].hist(np.array([p.stat().st_size for p in paths_dest]) / 1e6, bins=nbins)
    ax[1].set_xlabel("Size (MB)")
    ax[0].set_ylabel("File count (input files)")
    ax[1].set_ylabel("File count (output files)")
    fig.savefig(filepath)

if __name__ == "__main__":
    paths_orig, paths_dest = _get_paths()
    _add_candidates(paths_orig, paths_dest, _same_name, _same_size)
    _add_twins(paths_orig, paths_dest)
    _save_hists(filter_output=True)
    with_candidates = [p for p in paths_orig if any(p.candidates)]
    without_candidates = [p for p in paths_orig if not any(p.candidates)]
    with_twins = [p for p in paths_orig if any(p.twins)]
    without_twins = [p for p in paths_orig if not any(p.twins)]