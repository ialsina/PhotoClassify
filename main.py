#!/usr/bin/python3
from dataclasses import dataclass
from datetime import datetime, timedelta
from collections import defaultdict
import shutil
from typing import Callable, Dict, List, Optional

from tqdm import tqdm
from config import config as cfg, write_date


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

def get_filepaths(first_date: Optional[datetime] = None):
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
    if first_date is None:
        first_date = cfg.date.first_date
    imgpaths = []
    for root, _, files in cfg.path.origin.walk():
        for file in files:
            imgpaths.append(root / file)

    imgdates = defaultdict(list)
    for imgpath in imgpaths:
        dt = datetime.fromtimestamp(imgpath.stat().st_ctime)
        modified_dt = dt - cfg.date.day_starts_at
        # Pictures too old to classify (ideally already classified)
        if modified_dt < first_date:
            continue
        datestamp = modified_dt.strftime(r"%Y%m%d")
        imgdates[datestamp].append(imgpath)
    return imgpaths, imgdates


def create_directories(imgdates):
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
        target_path = cfg.path.destination / datestamp
        if target_path.exists():
            return
        try:
            target_path.mkdir()
        except FileNotFoundError as exc:
            raise FileNotFoundError(
                "Could not create file: " + str(cfg.path.destination / datestamp)
            ) from exc


def copy(imgdates):
    """
    Copies image files to a destination path organized by date.

    Parameters
    ----------
    imgdates : Dict[str, List[str]]
        A dictionary where keys are dates and values are lists of origin file paths.

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
        for date, origin_fpaths
        in imgdates.items()
        for origin_fpath
        in origin_fpaths
    }

    # values of imgdates are lists
    with tqdm(total = len(imgdates2)) as pbar:
        for origin_fpath, datestamp in imgdates2.items():
            fname = origin_fpath.name
            destin_path = cfg.path.destination / datestamp
            destin_fpath = destin_path / fname
            pbar.set_description_str(
                f"{datestamp:>8s}/{fname:<12s}"
            )
            pbar.update()

            if not destin_fpath.exists():
                try:
                    shutil.copy2(origin_fpath, destin_path)
                    successful.append(origin_fpath)
                except (FileNotFoundError, PermissionError, OSError) as exc:
                    unsuccessful.append(origin_fpath)
                    exceptions.add(exc)
            else:
                existing.append(origin_fpath)

    return CopyResult(
        total=imgdates2,
        successful=successful,
        existing=existing,
        unsuccessful=unsuccessful,
        exceptions=sorted(exceptions, key=str)
    )

if __name__ == "__main__":
    _, image_dates = get_filepaths()
    create_directories(image_dates)
    if cfg.copy.verbose >= 1:
        print(
            'Copying files:\n'
            f'\t         FROM: {cfg.path.origin:<30s}\n'
            f'\t           TO: {cfg.path.destination:<30s}\n'
            f'\tSTARTING DATE: {cfg.date.first_date:<30s}'
        )
    copy_result = copy(image_dates)
    copy_result.report(verbose=cfg.copy.verbose)
    if cfg.date.auto_date:
        write_date()
