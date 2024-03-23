
import os
import piexif
from tqdm import tqdm
from PIL import Image, ExifTags
from datetime import datetime, timedelta


PATH = "/media/ivan/PortableSSD/DCIM05/104ND610"

def read(fname):
    fpath = os.path.join(PATH, fname)
    img = Image.open(fpath)
    return img

def write(img: Image, fname, exif):
    fpath = os.path.join(PATH, fname)
    img.save(fpath, exif=exif)

def get_date(img: Image):
    exif = img.getexif()
    exif_h = {ExifTags.TAGS[k]: v for k, v in exif.items()}
    return datetime.fromisoformat(exif_h["DateTime"].replace(":", "-", 2))

fname = "DSC_5125.NEF"
img = read(fname)
real = datetime(2024, 1, 11, 19, 15, 40)
dt = real - get_date(img)

fnames = [f"DSC_{el:d}.NEF" for el in range(4941, 5126)]

for fname in tqdm(fnames):
    img = read(fname)
    exif = img.getexif()
    real_date = get_date(img) + dt
    exif[{v: k for k, v in ExifTags.TAGS.items()}["DateTime"]] = real_date.strftime("%Y:%m:%d %H:%M:%S")
    write(img, fname.replace(".NEF", "_.NEF"), exif)
    break
