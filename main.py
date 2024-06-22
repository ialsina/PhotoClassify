#! /usr/bin/python3
import os
from datetime import datetime, timedelta
from collections import defaultdict
import shutil

from tqdm import tqdm

from config import config as cfg, write_date

# from config import PATH_ORIGIN, PATH_DESTIN, \
#     DAY_STARTS_AT, PROCESS_AFTER, INCLUDE_FIRST, REMOVE_FROM_SD, \
#     AUTO_DATE, VERBOSE


def get_filepaths(first_date):
    imgpaths = []

    for directory in os.listdir(cfg.path.origin):
        path = os.path.join(cfg.path.origin, directory)
        files = os.listdir(path)

        for file in files:
            imgpaths.append(os.path.join(path, file))

    # KEYS: date. VALUES: list of origin_filepath
    imgdates = defaultdict(list)

    for imgpath in imgpaths:
        dt = datetime.fromtimestamp(os.path.getctime(imgpath))
        modified_dt = dt - timedelta(hours=cfg.date.day_starts_at)

        # Pictures too old to classify (ideally already classified)cd
        if modified_dt < first_date:
            continue

        datestamp = modified_dt.strftime('%Y%m%d')

        imgdates[datestamp].append(imgpath)

    return imgpaths, imgdates


def create_directories(imgdates):
    for datestamp in imgdates.keys():
        try:
            os.mkdir(os.path.join(cfg.path.destination, datestamp))
        except FileExistsError:
            pass
        except FileNotFoundError as exc:
            print("Could not create file:\n{}/{}".format(cfg.path.destination, datestamp))
            raise exc


def copy(imgdates):

    successful = []
    existing = []
    unsuccessful = []
    exceptions_set = set()

    # KEYS: origin_filepath. VALUES: date
    imgdates2 = {origin_fpath: date for date, origin_fpaths in imgdates.items() for origin_fpath in origin_fpaths}

    # values of imgdates are lists
    with tqdm(total = len(imgdates2)) as progress:
        for origin_fpath, datestamp in imgdates2.items():
            fname = os.path.basename(origin_fpath)
            destin_path = os.path.join(cfg.path.destination, datestamp)
            destin_fpath = os.path.join(destin_path, fname)
            progress.set_description_str('{:>8s}/{:<12s}'.format(datestamp, fname))
            progress.update()

            if not os.path.exists(destin_fpath):
                try:
                    shutil.copy2(origin_fpath, destin_path)
                    successful.append(origin_fpath)
                except Exception as e:
                    unsuccessful.append(origin_fpath)
                    exceptions_set.add(e)
            else:
                existing.append(origin_fpath)

    exceptions = list(exceptions_set)

    return imgdates2, successful, existing, unsuccessful, exceptions


def report(total, successful, existing, unsuccessful, exceptions):

    total_len = len(total)

    print('Copy terminated')

    if cfg.copy.verbose >= 2:
        print('Successful copies: {} out of {}'.format(len(successful), total_len))
        if len(existing) > 0:
            print('***')
            print('Existing files: {} out of {}'.format(len(existing), total_len))
    if cfg.copy.verbose >= 3:
        print('\n\t'.join(existing))
    if cfg.copy.verbose >= 2:
        if len(unsuccessful) > 0:
            print('***')
            print('Unsuccessful copies: {} out of {}'.format(len(unsuccessful), total_len))
            print('\n\t'.join(unsuccessful))
    if cfg.copy.verbose >= 3:
        print('> Errors encountered:')
        print('\n\t'.join([str(e) for e in exceptions]))

    print('***')


def main():
    first_date = cfg.date.first_date
    _, image_dates = get_filepaths(first_date)
    create_directories(image_dates)
    if cfg.copy.verbose >= 1:
        print('Copying files:\n'
              '\t         FROM: {:<30s}\n'
              '\t           TO: {:<30s}\n'
              '\tSTARTING DATE: {:<30s}'.format(cfg.path.origin,
                                                cfg.path.destination,
                                                first_date.date().strftime("%a %d/%m/%Y"))
                                                )
    result_tuple = copy(image_dates)
    report(*result_tuple)
    if cfg.date.auto_date:
        write_date()

if __name__ == "__main__":
    main()
