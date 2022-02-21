#! /usr/bin/python3
import os
from PIL import Image, ExifTags
from datetime import datetime, timedelta
from collections import defaultdict
import shutil
from tqdm import tqdm

# Declare destination path ================================

PATH_ORIGIN = '/media/ivan/Extreme SSD/Photographs'
PATH_DESTIN = '/home/ivan/ownCloud/Pictures/edit'

print('Current destination path:\n\t{}'.format(PATH_DESTIN))
print('* Type a subdirectory to add it,\n* Type absolute path to change it,\n* OR leave blank to keep:')
_input = input('> ')

if _input == '':
    pass
elif '/' in _input:
    PATH_DESTIN = _input
else:
    PATH_DESTIN = os.path.join(PATH_DESTIN, _input)

PATH_DESTIN = os.path.abspath(PATH_DESTIN)

if not os.path.exists(PATH_DESTIN):
    os.mkdir(PATH_DESTIN)
    print('Created path:\n\t{}'.format(PATH_DESTIN))
else:
    print('Existing path:\n\t{}'.format(PATH_DESTIN))


# Find targets =============================================

imgpaths = []

with open('copyfiles.txt','r') as f:
    lines = f.readlines()
    lines = [el.replace('\n','') for el in lines]
    lines = [el.strip() for el in lines if len(el.strip()) > 0]
    lines = [el for el in lines if el[0] != '#']
    lines = [el+'.NEF' if el[-4:] != '.NEF' else el for el in lines]

for root, dirs, files in os.walk(PATH_ORIGIN):
    for line in lines:
        if line.count('/') == 1:
            target_dir, target_file = line.strip().split('/')
            if os.path.split(root)[-1] == target_dir and target_file in files:
                imgpaths.append(os.path.join(root, target_file))

        else:
            target_file = line.strip()
            if target_file in files:
                imgpaths.append(os.path.join(root, target_file))

print('Targets detected:')
print('\t'+'\n\t'.join(imgpaths))


# Copy =====================================================

print('Copying files:\n\tFROM: {}\n\tTO: {}'.format(PATH_ORIGIN, PATH_DESTIN))

successful = []
existing = []
unsuccessful = []

# values of imgdates are lists
for origin_fpath in tqdm(imgpaths):
    fname = os.path.basename(origin_fpath)
    destin_fpath = os.path.join(PATH_DESTIN, fname)

    if not os.path.exists(destin_fpath):
        try:
            shutil.copy2(origin_fpath, destin_fpath)
            successful.append(destin_fpath)
        except Exception as e:
            unsuccessful.append(origin_fpath)
            print(origin_fpath, e)
    else:
        existing.append(destin_fpath)
        print('ALREADY EXISTS: {} | {}'.format(destin_fpath, os.path.exists(destin_fpath)))


print('Copy terminated')
print('Successful copies: {} out of {}'.format(len(successful), len(imgpaths)))
if len(existing) > 0:
    print('***')
    print('Existing files: {} out of {}'.format(len(existing), len(imgpaths)))
    print('\n\t'.join(existing))
if len(unsuccessful) > 0:
    print('***')
    print('Unsuccessful copies: {} out of {}'.format(len(unsuccessful), len(imgpaths)))
    print('\n\t'.join(unsuccessful))

print('***')
