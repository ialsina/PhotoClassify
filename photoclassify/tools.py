import sys
from argparse import ArgumentParser, RawTextHelpFormatter

from photoclassify.config import get_config
from photoclassify.copy import copy_photographs
from photoclassify.diff import find_files_with_copy

def _get_parser() -> ArgumentParser:
    parser = ArgumentParser(add_help=True, formatter_class=RawTextHelpFormatter)
    parser.add_argument("PATH.origin", action="store", metavar="ORIGIN", default=None)
    parser.add_argument("PATH.destination", action="store", metavar="DESTINATION", default=None)
    parser.add_argument("-q", "--quarters", action="store_true", dest="PATH.quarters")
    parser.add_argument("-H", "--day-starts-at", action="store", type=int, default=None, metavar="DAY_STARTS_AT", dest="DATE.day_starts_at")
    parser.add_argument("-a", "--process-after", action="store", type=str, default=None, metavar="PROCESS_AFTER", dest="DATE.process_after")
    parser.add_argument("-F", "--no-include-first", action="store_false", dest="DATE.no_include_first")
    parser.add_argument("--remove", action="store_true", dest="COPY.remove_from_sd")
    parser.add_argument("--verbose", "-v", action="count", default=0, dest="COPY.verbose")
    return parser

def _parse_cli():
    cli_config = vars(_get_parser().parse_args(sys.argv[1:]))
    cli_config["DATE.include_first"] = not cli_config.pop("DATE.no_include_first")
    return cli_config

cfg = get_config(**_parse_cli())

def copy():
    copy_photographs(cfg)
    return 0

def diff():
    _, without_copy = find_files_with_copy(cfg.path.origin, cfg.path.destination, ret_without=True)
    print("Don't have a copy:")
    for path in without_copy:
        print(path)
