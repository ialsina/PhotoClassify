import logging
from typing import Optional
import socket

LOGGING_LEVELS = ('DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL')

def get_logger(
        name: Optional[str] = None,
        logging_level: str = "INFO",
        filename: Optional[str] = None,
        hostname: bool = True,
        **kwargs
    ) -> logging.Logger | logging.LoggerAdapter:
    """Using in the logger function taken from the GSV to log
    errors, warnings and debugs.

    Arguments
    ---------
    logging_level : str
        Minimum severity level for log messages to be printed.
        Options are 'DEBUG', 'INFO', 'WARNING', 'ERROR' and
        'CRITICAL'.

    Returns
    -------
    logging.Logger
        Logger object for Opa logs.
    """
    if logging_level.upper() not in LOGGING_LEVELS:
        raise ValueError(
            f"Logging level must be one of {LOGGING_LEVELS}, but was '{logging_level}"
        )
    logger = logging.getLogger(name)
    if not logger.handlers:
        logging_level = logging_level.upper()
        logger.setLevel(logging_level)
        formatter = logging.Formatter('%(levelname)s:%(name)s:%(message)s')
        if filename:
            file_handler = logging.FileHandler(filename)
            file_handler.setFormatter(formatter)
            logger.addHandler(file_handler)
        else:
            stream_handler = logging.StreamHandler()
            stream_handler.setFormatter(formatter)
            logger.addHandler(stream_handler)
    if hostname:
        kwargs.update({"hostname": socket.gethostname()})
    if kwargs:
        logger = logging.LoggerAdapter(logger)
    return logger
