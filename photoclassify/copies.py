#!/usr/bin/python3
from concurrent.futures import ProcessPoolExecutor, as_completed
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
import os
import shutil
from typing import Callable, Dict, List, Set, Optional, Tuple
from pathlib import Path

from tqdm import tqdm

from photoclassify.config import get_config, write_date, Config
from photoclassify.diff import compare_stream
from photoclassify.photopath import PhotoPath


MAX_RENAME_ALLOWED = 20

class CopyStatus(Enum):
    SUCCESS = 1
    EXISTING = 2
    RENAMED = 3
    ERROR = 4

@dataclass
class CopyResult:
    """
    Data class to store the results of a file copy operation.

    Attributes
    ----------
    dates : Dict[Path, str]
        A dictionary where keys are origin file paths and values are dates.
    successful : List[Tuple[str, str]]
        A list of successfully copied file paths.
    existing : List[Tuple[str, str]]
        A list of file paths that already existed in the destination.
    renamed: List[Tuple[str, str]]
        A list of file paths that didn't exist in the destination, but that
        weren't copied because that would have produced a name collision.
    unsuccessful : List[Tuple[str, str]]
        A list of file paths that failed to copy.
    exceptions : List[Exception]
        A list of exceptions encountered during copying.
    """
    dates: Dict[Path, str]
    successful: List[Tuple[str, str]] = field(default_factory=list)
    existing: List[Tuple[str, str]] = field(default_factory=list)
    renamed: List[Tuple[str, str]] = field(default_factory=list)
    unsuccessful: List[Tuple[str, str]] = field(default_factory=list)
    exceptions: Set[Exception] = field(default_factory=set)

    @staticmethod
    def _tuples_str_generator(sequence_of_tuples):
        return '\n\t'.join(f"{str(ofp)} -> {str(dfp)}" for ofp, dfp in sequence_of_tuples)

    def report(
        self,
        verbose: int = 3,
        stdout: Callable = print,
    ):
        """
        Prints a report of the copy operation results.

        Parameters
        ----------
        verbose : int, optional
            Level of verbosity for the report (default is 3). The levels are:
                - 1: Basic termination message.
                - 2: Include counts of successful, existing, and unsuccessful copies.
                - 3: Include details of existing and unsuccessful files, and errors encountered.
        stdout : Callable, optional
            Function to use for printing the report (default is `print`).
        """
        total_len = len(self.dates)
        stdout('Copy terminated')
        if verbose >= 2:
            stdout(f'Successful copies: {len(self.successful)} out of {total_len}')
            if len(self.existing) > 0:
                stdout('***')
                stdout(f'Existing files: {len(self.existing)} out of {total_len}')
        if verbose >= 3:
            stdout(self._tuples_str_generator(self.existing))
        if verbose >= 2:
            if len(self.renamed) > 0:
                stdout('***')
                stdout(f'Renamed files: {len(self.renamed)} out of {total_len}')
                stdout(self._tuples_str_generator(self.renamed))
        if verbose >= 2:
            if len(self.unsuccessful) > 0:
                stdout('***')
                stdout(f'Unsuccessful copies: {len(self.unsuccessful)} out of {total_len}')
                stdout(self._tuples_str_generator(self.unsuccessful))
        if verbose >= 3:
            stdout('> Errors encountered:')
            stdout('\n\t'.join(str(e) for e in self.exceptions))
        stdout('***')

def _get_target_path(datestamp: str, cfg: Config):
    """
    Get the target path for a given datestamp.

    Parameters
    ----------
    datestamp : str
        The datestamp string in 'YYYYMMDD' format.
    cfg : Config
        The config object

    Returns
    -------
    Path
        The target path for the given datestamp.
    """
    if not cfg.path.quarters:
        return cfg.path.destination / datestamp
    date = datetime.strptime(datestamp, r"%Y%m%d")
    quarter = f"{date.year:4d}Q{(date.month - 1) // 3 + 1:1d}"
    return cfg.path.destination / quarter / datestamp


def get_filepaths(cfg: Config) -> Tuple[List[Path], Dict[str, List[Path]], Dict[Path, str]]:
    """
    Retrieves image file paths from the origin directory,
    classifies them by creation date, and stores them in a dictionary.

    Parameters
    ----------
    first_date : datetime, optional
        The earliest date to consider for classification.
        If not passed, taken from the configuration.

    Returns
    -------
    Tuple[List[Path], Dict[str, List[Path]]]
        A tuple containing:
            - imgpaths: A list of all image file paths found.
            - imgdates: A dictionary where keys are dates (as strings in 'YYYYMMDD' format)
                and values are lists of image file paths.
    """
    first_date = cfg.date.first_date
    imgpaths = []
    for root, _, files in os.walk(cfg.path.origin):
        for file in files:
            imgpaths.append(Path(root) / file)

    imgdates = defaultdict(list)
    for imgpath in imgpaths:
        dt = datetime.fromtimestamp(imgpath.stat().st_ctime)
        modified_dt = dt - timedelta(hours=cfg.date.day_starts_at)
        # Pictures too old to classify (ideally already classified)
        if modified_dt < first_date:
            continue
        datestamp = modified_dt.strftime(r"%Y%m%d")
        imgdates[datestamp].append(imgpath)

    imgdates_flat = {
        origin_fpath: date
        for date, origin_fpaths in imgdates.items()
        for origin_fpath in origin_fpaths
    }
    return imgpaths, dict(imgdates), imgdates_flat


def _create_directories(dates: List[str], cfg: Config):
    """
    Creates directories for each date key in the imgdates dictionary.

    Parameters
    ----------
    imgdates : Dict[str, List[str]]
        A dictionary where keys are dates and values are lists of image file paths.

    Raises
    ------
    FileNotFoundError
        If a directory cannot be created.
    """
    if not (
        isinstance(dates, list)
        and all(isinstance(element, str) for element in dates)
    ):
        raise TypeError(
            "dates should be of type List[str]."
        )
    for date in dates:
        target_path = _get_target_path(date, cfg)
        target_path.mkdir(parents=True, exist_ok=True)

def _copy_file_task(origin_fpath, destin_path):
    """Helper function for copy"""
    try:
        destin_fpath = destin_path / origin_fpath.name
        if not destin_fpath.exists():
            shutil.copy2(origin_fpath, destin_path)
            return CopyStatus.SUCCESS, origin_fpath, destin_fpath, None
        if compare_stream(origin_fpath, destin_fpath):
            return CopyStatus.EXISTING, origin_fpath, destin_fpath, None
        for _ in range(MAX_RENAME_ALLOWED):
            destin_fpath = PhotoPath.from_path(destin_fpath).next.path
            if not destin_fpath.exists():
                shutil.copy2(origin_fpath, destin_fpath)
                return CopyStatus.RENAMED, origin_fpath, destin_fpath, None
        return CopyStatus.ERROR, origin_fpath, None, RuntimeError("Too many rename attempts")
    except (FileNotFoundError, PermissionError, OSError) as exc:
        return CopyStatus.ERROR, origin_fpath, None, exc

def _copy_imgdates(
    imgdates: Dict[Path, str],
    cfg: Config,
):
    """
    Copies image files to a destination path organized by date.

    Parameters
    ----------
    imgdates : Dict[str, List[Path]]
        A dictionary where values are dates and keys are file paths.
    max_workers : int, optional
        The maximum number of worker processes to use. Defaults to the number of processors on the machine.

    Returns
    -------
    CopyResult
        A dataclass containing the results of the copy operation.
    """

    result = CopyResult(dates=imgdates)

    tasks = []
    # values of imgdates are lists
    with tqdm(total=len(tasks), desc="Copying files", unit="file") as pbar:
        for origin_fpath, datestamp in imgdates.items():
            destin_fpath = _get_target_path(datestamp, cfg)
            status, origin_fpath, destin_fpath, exc = _copy_file_task(origin_fpath, destin_fpath)
            if status == CopyStatus.SUCCESS:
                result.successful.append((origin_fpath, destin_fpath))
            elif status == CopyStatus.EXISTING:
                result.existing.append((origin_fpath, destin_fpath))
            elif status == CopyStatus.RENAMED:
                result.renamed.append((origin_fpath, destin_fpath))
            elif status == CopyStatus.ERROR:
                result.unsuccessful.append((origin_fpath, destin_fpath))
                result.exceptions.add(exc)
            pbar.update()

    return result

def _copy_imgdates_parallel(
    imgdates: Dict[Path, str],
    cfg: Config,
    max_workers: Optional[int] = None
):
    """
    Copies image files to a destination path organized by date.

    Parameters
    ----------
    imgdates : Dict[Path, ]
        A dictionary where values are dates and keys are file paths.
    max_workers : int, optional
        The maximum number of worker processes to use. Defaults to the number of processors on the machine.

    Returns
    -------
    CopyResult
        A dataclass containing the results of the copy operation.
    """

    # KEYS: origin_filepath. VALUES: date
    result = CopyResult(dates=imgdates)

    tasks = []
    # values of imgdates are lists
    with ProcessPoolExecutor(max_workers=max_workers) as executor:
        for origin_fpath, datestamp in imgdates.items():
            destin_fpath = _get_target_path(datestamp, cfg)
            task = executor.submit(_copy_file_task, origin_fpath, destin_fpath)
            tasks.append(task)

        with tqdm(total=len(tasks), desc="Copying files", unit="file") as pbar:
            for future in as_completed(tasks):
                status, origin_fpath, destin_fpath, exc = future.result()
                if status == CopyStatus.SUCCESS:
                    result.successful.append((origin_fpath, destin_fpath))
                elif status == CopyStatus.EXISTING:
                    result.existing.append((origin_fpath, destin_fpath))
                elif status == CopyStatus.RENAMED:
                    result.renamed.append((origin_fpath, destin_fpath))
                elif status == CopyStatus.ERROR:
                    result.unsuccessful.append((origin_fpath, destin_fpath))
                    result.exceptions.add(exc)
                pbar.update()

    return result

def copy_photographs(
    cfg: Config,
    parallel: bool = True,
    max_workers: Optional[int] = None,
    stdout: Optional[Callable] = None
) -> None:
    """
    Copies photographs according to the provided configuration.

    Parameters
    ----------
    cfg : Config
        Configuration object

    Returns
    -------
    None

    Raises
    ------
    FileNotFoundError
        If any required directories or files are not found.
    """
    _, dates, date_paths = get_filepaths(cfg)
    _create_directories(list(dates), cfg)
    if stdout is None:
        stdout = print
    if cfg.copy.verbose >= 1:
        stdout(
            'Copying files:\n'
            f'\t         FROM: {str(cfg.path.origin):<30s}\n'
            f'\t           TO: {str(cfg.path.destination):<30s}\n'
            f'\tSTARTING DATE: {cfg.date.first_date.strftime(r"%d-%m-%Y"):<30s}'
        )
    if parallel:
        copy_result = _copy_imgdates_parallel(date_paths, cfg, max_workers=max_workers)
    else:
        copy_result = _copy_imgdates(date_paths, cfg)
    copy_result.report(verbose=cfg.copy.verbose)
    if cfg.date.auto_date:
        write_date()


if __name__ == "__main__":
    copy_photographs(get_config())

