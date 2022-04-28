#! /usr/bin/python3
import os
from datetime import datetime, timedelta
from collections import defaultdict
import shutil
from tqdm import tqdm

from config import PATH_ORIGIN, PATH_DESTIN, \
    DAY_STARTS_AT, PROCESS_AFTER, INCLUDE_FIRST, REMOVE_FROM_SD

imgpaths = []

# Get original filepaths =================================

for directory in os.listdir(PATH_ORIGIN):
    path = os.path.join(PATH_ORIGIN, directory)
    files = os.listdir(path)

    for file in files:
        imgpaths.append(os.path.join(path, file))

# KEYS: date. VALUES: list of origin_filepath
imgdates = defaultdict(list)


if INCLUDE_FIRST:
    first_date = PROCESS_AFTER
else:
    first_date = PROCESS_AFTER + timedelta(days = 1)


for imgpath in imgpaths:
    dt = datetime.fromtimestamp(os.path.getctime(imgpath))
    modified_dt = dt - timedelta(hours = DAY_STARTS_AT)

    # Pictures too old to classify (ideally already classified)cd
    if modified_dt < datetime(**PROCESS_AFTER):
        continue

    datestamp = modified_dt.strftime('%Y%m%d')

    imgdates[datestamp].append(imgpath)


# Create directories ======================================
for datestamp in imgdates.keys():
    try:
        os.mkdir(os.path.join(PATH_DESTIN, datestamp))
    except FileExistsError:
        pass


# Copy =====================================================

print('Copying files:\nFROM: {}\nTO: {}\nSTARTING DATE: {}'.format(PATH_ORIGIN, PATH_DESTIN, PROCESS_AFTER))

successful = []
existing = []
unsuccessful = []

# KEYS: origin_filepath. VALUES: date
imgdates2 = { origin_fpath: date for date, origin_fpaths in imgdates.items() for origin_fpath in origin_fpaths }

# values of imgdates are lists
for origin_fpath, datestamp in tqdm(imgdates2.items()):
    fname = os.path.basename(origin_fpath)
    destin_path = os.path.join(PATH_DESTIN, datestamp)
    destin_fpath = os.path.join(destin_path, fname)
    excp_set = set()

    if not os.path.exists(destin_fpath):
        try:
            shutil.copy2(origin_fpath, destin_path)
            successful.append(origin_fpath)
        except Exception as e:
            unsuccessful.append(origin_fpath)
            excp_set.add(e)
    else:
        existing.append(origin_fpath)


print('Copy terminated')
print('Successful copies: {} out of {}'.format(len(successful), len(imgdates2)))
if len(existing) > 0:
    print('***')
    print('Existing files: {} out of {}'.format(len(existing), len(imgdates2)))
    print('\n\t'.join(existing))
if len(unsuccessful) > 0:
    print('***')
    print('Unsuccessful copies: {} out of {}'.format(len(unsuccessful), len(imgdates2)))
    print('\n\t'.join(unsuccessful))
    print('> Errors encountered:')
    print('\n\t'.join([str(e) for e in excp_set]))

print('***')
