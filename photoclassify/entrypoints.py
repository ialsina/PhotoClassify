from argparse import ArgumentParser, RawTextHelpFormatter
from pprint import pprint
import sys

from photoclassify.config import get_config
from photoclassify.copies import copy_photographs
from photoclassify.diff import (
    find_files_without_copy,
    make_histogram,
    report,
)

def _get_base_parser() -> ArgumentParser:
    parser = ArgumentParser(add_help=True, formatter_class=RawTextHelpFormatter)
    parser.add_argument("PATH.origin", action="store", metavar="ORIGIN", default=None)
    parser.add_argument("PATH.destination", action="store", metavar="DESTINATION", default=None)
    parser.add_argument("-o", "--output", action="store", metavar="OUTPUT", default=None)
    parser.add_argument("--no-parallel", "-P", action="store_true")
    parser.add_argument("--max-workers", "-W", action="store", default=None)
    return parser

def _get_diff_parser() -> ArgumentParser:
    parser = _get_base_parser()
    parser.add_argument(
        "-t", "--type",
        action="store",
        choices=["candidates", "twins", "both"],
        default="both",
        metavar="TYPE",
    )
    parser.add_argument(
        "-1", "--level-one",
        action="store_true",
    )
    return parser

def _get_copy_parser() -> ArgumentParser:
    parser = _get_base_parser()
    parser.add_argument("--remove", action="store_true", dest="COPY.remove_from_sd")
    parser.add_argument("--verbose", "-v", action="count", default=0, dest="COPY.verbose")
    parser.add_argument("-q", "--quarters", action="store_true", dest="PATH.quarters")
    parser.add_argument("-H", "--day-starts-at", action="store", type=int, default=None, metavar="DAY_STARTS_AT", dest="DATE.day_starts_at")
    parser.add_argument("-a", "--process-after", action="store", type=str, default=None, metavar="PROCESS_AFTER", dest="DATE.process_after")
    parser.add_argument("-F", "--no-include-first", action="store_true", dest="DATE.no_include_first")
    return parser

def _get_hist_parser() -> ArgumentParser:
    parser = _get_base_parser()
    parser.set_defaults(output="histogram.png")
    parser.add_argument("-b", "--nbins", action="store", metavar="BINS", type=int, default=100)
    parser.add_argument("-S", "--no-split-input", action="store_true")
    parser.add_argument("-F", "--no-filter-output", action="store_true")
    return parser

def _clean_cli_config(cli_config):
    """Separate parallel, max_workers and output from config dictionary."""

    parallel = not cli_config.pop("no_parallel")
    max_workers = cli_config.pop("max_workers")
    output = cli_config.pop("output")
    return parallel, max_workers, output

def copy():
    cli_config = vars(_get_copy_parser().parse_args(sys.argv[1:]))
    parallel, max_workers, output = _clean_cli_config(cli_config)
    cli_config["DATE.include_first"] = not cli_config.pop("DATE.no_include_first")
    cfg = get_config(**cli_config)
    if output:
        wf = open(output, "wf", encoding="utf-8")
        stdout = lambda txt: wf.write(f"{txt}\n")
    else:
        stdout = print
    copy_photographs(cfg,
                     parallel=parallel,
                     max_workers=max_workers,
                     stdout=stdout
                     )
    if output:
        wf.close()  # type: ignore
    return 0

def diff():
    cli_config = vars(_get_diff_parser().parse_args(sys.argv[1:]))
    parallel, max_workers, output = _clean_cli_config(cli_config)
    which = cli_config.pop("type")
    level_two = not cli_config.pop("level_one")
    cfg = get_config(**cli_config)
    if output is None:
        without_copy = find_files_without_copy(
            origin=cfg.path.origin,
            destination=cfg.path.destination,
            parallel=parallel,
            max_workers=max_workers,
        )
        pprint(without_copy)
    else:
        report(
            origin=cfg.path.origin,
            destination=cfg.path.destination,
            fname=output,
            which=which,
            parallel=parallel,
            max_workers=max_workers,
            level_two=level_two,
        )
    return 0

def hist():
    cli_config = vars(_get_hist_parser().parse_args(sys.argv[1:]))
    parallel, max_workers, output = _clean_cli_config(cli_config)
    nbins = cli_config.pop("nbins")
    split_input = not cli_config.pop("no_split_input")
    filter_output = not cli_config.pop("no_filter_output")
    cfg = get_config(**cli_config)
    make_histogram(
        origin=cfg.path.origin,
        destination=cfg.path.destination,
        fname=output,
        parallel=parallel,
        max_workers=max_workers,
        nbins=nbins,
        split_input=split_input,
        filter_output=filter_output,
        stacked=True
    )
    return 0

