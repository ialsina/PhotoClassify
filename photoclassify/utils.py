from pathlib import Path
import re

def get_identifier_tuple(path: Path):
    parent = path.parent
    suffix = path.suffix
    stem = path.stem
    hexinc = re.match(r"_x([0-9a-f]{2})$", stem)
    if hexinc:
        stem = stem.rstrip(hexinc.group())
        counter = int(hexinc.groups()[0], 16)
    else:
        counter = 0
    return (parent, stem, suffix, counter)

# TODO Fix issue, this is creating paths like .../.../DSC_0162_01_01.NEF
def new_name(path: Path):
    parent, stem, suffix, counter = get_identifier_tuple(path)
    return parent / f"{stem}_{hex(counter + 1)[2:]:>02s}{suffix}"
