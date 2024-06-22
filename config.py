from dataclasses import dataclass
from datetime import datetime, time
from pathlib import Path
from typing import Any, Dict
from yaml import safe_load

ROOT = Path(__file__).resolve().parent
ROOT_CONFIG = ROOT / "config.yaml"

@dataclass
class PathConfig:
    origin: Path
    destination: Path
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
        if path_config.safe and not path_config._is_dir:
            raise AssertionError(
                "Some paths are either invalid or unmounted."
            )
        return path_config
        

@dataclass
class DateConfig:
    day_starts_at: datetime
    process_after: datetime
    auto_date: bool

    @classmethod
    def parse(cls, dct):
        day_starts_at = dct.get("day_starts_at", "00:00:00")
        process_after = dct.get("process_after", "01-01-1000")
        return cls(
            day_starts_at=datetime.strptime(day_starts_at, r"%H:%M:%S"),
            process_after=datetime.strptime(process_after, r"%d-%m-%Y"),
            auto_date=dct.get("auto_date", True),
        )

@dataclass
class CopyConfig:
    include_first: bool = True
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

        

with open(ROOT_CONFIG, "r", encoding="utf-8") as cf:
    config = Config.parse(safe_load(cf))
