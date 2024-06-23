#!/usr/bin/python3
from concurrent.futures import ProcessPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime, timedelta
from collections import defaultdict
import shutil
from typing import Callable, Dict, List, Optional

from tqdm import tqdm

try:
    from .config import get_config, write_date, Config
except ImportError:
    from config import get_config, write_date, Config


@dataclass
class CopyResult:
    """
    Data class to store the results of a file copy operation.

    Attributes
    ----------
    total : Dict[str, str]
        A dictionary where keys are origin file paths and values are dates.
    successful : List[str]
        A list of successfully copied file paths.
    existing : List[str]
        A list of file paths that already existed in the destination.
    unsuccessful : List[str]
        A list of file paths that failed to copy.
    exceptions : List[Exception]
        A list of exceptions encountered during copying.
    """
    total: Dict[str, str]
    successful: List[str]
    existing: List[str]
    unsuccessful: List[str]
    exceptions: List[Exception]

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
        total_len = len(self.total)
        stdout('Copy terminated')
        if verbose >= 2:
            stdout(f'Successful copies: {len(self.successful)} out of {total_len}')
            if len(self.existing) > 0:
                stdout('***')
                stdout(f'Existing files: {len(self.existing)} out of {total_len}')
        if verbose >= 3:
            stdout('\n\t'.join(self.existing))
        if verbose >= 2:
            if len(self.unsuccessful) > 0:
                stdout('***')
                stdout(f'Unsuccessful copies: {len(self.unsuccessful)} out of {total_len}')
                stdout('\n\t'.join(self.unsuccessful))
        if verbose >= 3:
            stdout('> Errors encountered:')
            stdout('\n\t'.join([str(e) for e in self.exceptions]))
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


def get_filepaths(cfg: Config):
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
    for root, _, files in cfg.path.origin.walk():
        for file in files:
            imgpaths.append(root / file)

    imgdates = defaultdict(list)
    for imgpath in imgpaths:
        dt = datetime.fromtimestamp(imgpath.stat().st_ctime)
        modified_dt = dt - timedelta(hours=cfg.date.day_starts_at)
        # Pictures too old to classify (ideally already classified)
        if modified_dt < first_date:
            continue
        datestamp = modified_dt.strftime(r"%Y%m%d")
        imgdates[datestamp].append(imgpath)
    return imgpaths, imgdates


def _create_directories(imgdates: Dict[str, List[str]], cfg: Config):
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
    for datestamp in imgdates.keys():
        target_path = _get_target_path(datestamp, cfg)
        target_path.mkdir(parents=True, exist_ok=True)


def _copy_file_task(origin_fpath, destin_path):
    """Helper function for copy"""
    try:
        destin_fpath = destin_path / origin_fpath.name
        if not destin_fpath.exists():
            shutil.copy2(origin_fpath, destin_path)
            return 'success', origin_fpath, None
        return 'existing', origin_fpath, None
    except (FileNotFoundError, PermissionError, OSError) as exc:
        return 'error', origin_fpath, exc

def _copy_imgdates(imgdates, cfg: Config, parallel: bool = True, max_workers: Optional[int] = None):
    """
    Copies image files to a destination path organized by date.

    Parameters
    ----------
    imgdates : Dict[str, List[Path]]
        A dictionary where keys are dates and values are lists of origin file paths.
    max_workers : int, optional
        The maximum number of worker processes to use. Defaults to the number of processors on the machine.

    Returns
    -------
    CopyResult
        A dataclass containing the results of the copy operation.
    """
    successful = []
    existing = []
    unsuccessful = []
    exceptions = set()

    # KEYS: origin_filepath. VALUES: date
    imgdates2 = {
        origin_fpath: date
        for date, origin_fpaths in imgdates.items()
        for origin_fpath in origin_fpaths
    }

    tasks = []
    # values of imgdates are lists
    with ProcessPoolExecutor(max_workers=(max_workers if parallel else 1)) as executor:
        for origin_fpath, datestamp in imgdates2.items():
            destin_path = _get_target_path(datestamp, cfg)
            task = executor.submit(_copy_file_task, origin_fpath, destin_path)
            tasks.append(task)

        with tqdm(total=len(tasks), desc="Copying files", unit="file") as pbar:
            for future in as_completed(tasks):
                result, origin_fpath, exc = future.result()
                if result == "success":
                    successful.append(origin_fpath)
                elif result == "existing":
                    existing.append(origin_fpath)
                elif result == "error":
                    unsuccessful.append(origin_fpath)
                    exceptions.add(exc)
                pbar.update()

    return CopyResult(
        total=imgdates2,
        successful=successful,
        existing=existing,
        unsuccessful=unsuccessful,
        exceptions=sorted(exceptions, key=str)
    )

def copy_photographs(cfg):
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
    _, image_dates = get_filepaths(cfg)
    _create_directories(image_dates, cfg)
    if cfg.copy.verbose >= 1:
        print(
            'Copying files:\n'
            f'\t         FROM: {str(cfg.path.origin):<30s}\n'
            f'\t           TO: {str(cfg.path.destination):<30s}\n'
            f'\tSTARTING DATE: {cfg.date.first_date.strftime(r"%d-%m-%Y"):<30s}'
        )
    copy_result = _copy_imgdates(image_dates, cfg)
    copy_result.report(verbose=cfg.copy.verbose)
    if cfg.date.auto_date:
        write_date()


if __name__ == "__main__":
    copy_photographs(get_config())
