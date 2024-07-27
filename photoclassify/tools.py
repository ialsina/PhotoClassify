import sys
from argparse import ArgumentParser, RawTextHelpFormatter

from photoclassify.config import get_config
from photoclassify.copies import copy_photographs
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
    parser.add_argument("--no-parallel", "-P", action="store_false")
    parser.add_argument("--max-workers", "-W", action="store", default=None)
    return parser

def _parse_cli():
    cli_config = vars(_get_parser().parse_args(sys.argv[1:]))
    cli_config["DATE.include_first"] = not cli_config.pop("DATE.no_include_first")
    parallel = not cli_config.pop("no_parallel")
    max_workers = cli_config.pop("max_workers")
    return cli_config, parallel, max_workers

_cli_config, PARALLEL, MAX_WORKERS = _parse_cli()
cfg = get_config(**_cli_config)

def copy():
    copy_photographs(cfg, parallel=PARALLEL, max_workers=MAX_WORKERS)
    return 0

def diff():
    origin = cfg.path.origin
    destination = cfg.path.destination
    _, without_copy = find_files_with_copy(
        origin=origin,
        destination=destination,
        parallel=PARALLEL,
        max_workers=MAX_WORKERS,
        ret_without=True,
    )
    if without_copy:
        print("Don't have a copy:")
        for path in without_copy:
            print(path)
    else:
        print(f"All elements in '{origin}' have a copy in '{destination}'.")
