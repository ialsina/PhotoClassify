from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict
from yaml import safe_load

ROOT = Path(__file__).resolve().parent
CONFIG_PATH = ROOT / "config.yaml"
LAST_DATE_PATH = ROOT / ".lastdate"

def read_date():
    with open(LAST_DATE_PATH, 'r', encoding="utf-8") as rf:
        return datetime.fromisoformat(
            rf.readline()
        )

def write_date():
    with open(LAST_DATE_PATH, 'w', encoding="utf-8") as wf:
        wf.write(
            datetime.now().date().isoformat()
        )

@dataclass
class PathConfig:
    origin: Path
    destination: Path
    quarters: bool = False
    safe: bool = True

    def _is_dir(self):
        for value in self.__dict__.values():
            if isinstance(value, Path):
                if not value.is_dir():
                    return False
        return True

    @classmethod
    def parse(cls, dct: Dict[str, str]):
        path_config = cls(**{
            key: Path(value) if isinstance(value, str) else value
            for key, value
            in dct.items()
        })
        if path_config.safe and not path_config._is_dir():
            raise AssertionError(
                "Some paths are either invalid or unmounted."
            )
        return path_config
        

@dataclass
class DateConfig:
    day_starts_at: dict
    process_after: datetime
    auto_date: bool = False
    include_first: bool = True

    @property
    def first_date(self):
        if self.auto_date:
            try:
                return read_date()
            except (FileNotFoundError, ValueError):
                pass
        if self.include_first:
            return self.process_after
        return self.process_after + timedelta(days=1)

    @classmethod
    def parse(cls, dct):
        day_starts_at = dct.pop("day_starts_at", 0)
        process_after = dct.pop("process_after", "01-01-1000")
        if not isinstance(day_starts_at, (int, float)):
            raise TypeError(
                f"day_starts_at must be of type int or float, not {type(day_starts_at)}."
            )
        if not 0 <= day_starts_at < 12:
            raise TypeError(
                f"day_starts_at must be greater than 0, and lower than 12 (was {day_starts_at})."
            )
        return cls(
            day_starts_at=day_starts_at,
            process_after=datetime.strptime(process_after, r"%d-%m-%Y"),
            **dct
        )

@dataclass
class CopyConfig:
    remove_from_sd: bool = False
    auto_first: bool = False
    verbose: bool = False

    @classmethod
    def parse(cls, dct):
        return cls(**dct)

@dataclass
class Config:
    path: PathConfig
    date: DateConfig
    copy: CopyConfig

    @classmethod
    def parse(cls, dct):
        # pylint: disable=E1101
        dct_out = {}
        for subconfig_key, subconfig_dict in dct.items():
            subconfig_key = subconfig_key.lower()
            try:
                subconfig_class = cls.__annotations__[subconfig_key]
            except KeyError as exc:
                raise KeyError(
                    f"Wrong subconfig group: {subconfig_key}."
                ) from exc
            subconfig_instance = subconfig_class.parse(subconfig_dict)
            dct_out[subconfig_key] = subconfig_instance
        return cls(**dct_out)

        

with open(CONFIG_PATH, "r", encoding="utf-8") as cf:
    config = Config.parse(safe_load(cf))
