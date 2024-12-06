from dataclasses import dataclass
from pathlib import Path
import re

# IDEA: This could be made inheriting from Path and with @property
@dataclass
class PhotoPath:
    parent: Path
    stem: str
    suffix: str
    counter: int

    @classmethod
    def from_path(cls, path: Path) -> "PhotoPath":
        stem = path.stem
        hexinc = re.search(r"_x([0-9a-f]{2})$", stem)
        if hexinc:
            stem = stem.rsplit(hexinc.group(), 1)[0]
            counter = int(hexinc.groups()[0], 16)
        else:
            counter = 0
        return cls(
            parent=path.parent,
            stem=stem,
            suffix=path.suffix,
            counter=counter,
        )

    @property
    def name(self) -> str:
        return "{stem}_x{counter:>02s}{suffix}".format(
            stem=self.stem,
            counter=hex(self.counter)[2:],
            suffix=self.suffix,
        )

    @property
    def next(self) -> "PhotoPath":
        return PhotoPath(
            parent=self.parent,
            stem=self.stem,
            suffix=self.suffix,
            counter=self.counter + 1,
        )

    @property
    def path(self) -> Path:
        return self.parent / self.name

    def same_name(self, other: "PhotoPath") -> bool:
        return (self.stem, self.suffix) == (other.stem, other.suffix)

