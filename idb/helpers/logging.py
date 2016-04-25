from __future__ import absolute_import

import os
import os.path
import sys

import logging

idblogger = logging.getLogger('idb')

STD_FORMAT = "%(asctime)s %(levelname)-5.5s %(name)s\u10fb %(message)s"
DATETIME_FORMAT = "%Y-%m-%d %H:%M:%S"
TIME_FORMAT = "%H:%M:%S"

PRECISE_FORMAT = "%(asctime)s.%(msecs)03d %(levelname)-5.5s %(name)s\u10fb %(message)s"

LOGBOOK_FORMAT_STRING = u'{record.time:%Y-%m-%d %H:%M:%S.%f} {record.level_name:<5.5} ' \
                        u'{record.channel}\u10fb {record.message}'

LIBRARY_LOGGERS = ('boto', 'requests', 'urllib3', 'elasticsearch')


def configure_app_log(verbose, fmt=PRECISE_FORMAT,
                      stderr_handler=True,
                      filename=None):
    logging_level = ({
        0: logging.WARNING,
        1: logging.INFO,
        2: logging.DEBUG,
        3: 0
    }).get(verbose, logging.DEBUG)

    idblogger.setLevel(logging_level)

    idblogger.getChild('cli').debug(
        "Running with verbose level %s - %s",
        logging_level, logging.getLevelName(logging_level))


# def configure(root=idblogger, root_level=logging.DEBUG,
#               filename=None, logdir=None,
#               file_level=None,
#               stderr_level=None,
#               clear_existing_handlers=True):
#     if root is None:
#         root = logging.root
#     if filename:
#         root.addHandler(logging.FileHandler)


def add_file_handler(logger=logging.root, level=logging.INFO,
                     filename=None, logdir='/var/log/idb/'):
    if logger.getEffectiveLevel() > level:
        logger.setLevel(level)
    if filename is None:
        filename = os.path.split(sys.argv[0])[1] + '.log'

    fh = logging.FileHandler(os.path.join(logdir, filename))
    fh.setLevel(level)
    fh.setFormatter(logging.Formatter(PRECISE_FORMAT))
    logger.addHandler(fh)
