from pathlib import Path
import re

def get_identifier_tuple(path: Path):
    parent = path.parent
    suffix = path.suffix
    stem = path.stem
    hexinc = re.search(r"_x([0-9a-f]{2})$", stem)
    if hexinc:
        stem = stem.rsplit(hexinc.group(), 1)[0]
        counter = int(hexinc.groups()[0], 16)
    else:
        counter = 0
    return (parent, stem, suffix, counter)

def new_name(path: Path):
    parent, stem, suffix, counter = get_identifier_tuple(path)
    return parent / f"{stem}_x{hex(counter + 1)[2:]:>02s}{suffix}"
