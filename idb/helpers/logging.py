from __future__ import absolute_import

import logging
import os
import os.path
import sys


idblogger = logging.getLogger('idb')

DEFAULT_LOGDIR = u'/var/log/idb/'

STD_FORMAT = u"%(asctime)s %(levelname)-5.8s %(name)s\u10fb %(message)s"
DATETIME_FORMAT = "%Y-%m-%d %H:%M:%S"
TIME_FORMAT = "%H:%M:%S"

PRECISE_FORMAT = u"%(asctime)s.%(msecs)03d %(levelname)-5.5s %(name)s\u10fb %(message)s"

LOGBOOK_FORMAT_STRING = u'{record.time:%Y-%m-%d %H:%M:%S.%f} {record.level_name:<5.5} ' \
                        u'{record.channel}\u10fb {record.message}'

#: Libaries used whos logs should be at a higher level.
LIBRARY_LOGGERS = ('boto', 'requests', 'urllib3', 'elasticsearch', 'shapely')


def getLogger(l):
    "Wrapper around logging.getLogger that returns the original if its already a logger"
    if hasattr(l, 'addHandler') and hasattr(l, 'setLevel'):
        return l
    else:
        return logging.getLogger(l)


def configure_app_log(verbose, logfile=None):
    "Tries to do the right thing for configuring logging for command line applications"
    lvls = {
        -1: logging.ERROR,
        0: logging.WARNING,
        1: logging.INFO,
        2: logging.DEBUG,
        3: 0
    }
    idblogger.setLevel(0)  # doing filtering in handlers

    for l in LIBRARY_LOGGERS:
        # libraries should be one level less verbose
        getLogger(l).setLevel(lvls.get(verbose - 1, logging.WARNING))
    getLogger('elasticsearch.trace').addHandler(logging.NullHandler())
    getLogger('elasticsearch.trace').propagate = False

    if logfile:
        # logging to a file should be at one level more verbose
        add_file_handler(filename=logfile, level=lvls.get(verbose + 1, logging.INFO))

    logging_level = lvls.get(verbose, logging.DEBUG)
    add_stderr_handler(level=logging_level)


def configure(logger=idblogger,
              filename=None, logdir=None,
              file_level=None,
              stderr_level=None,
              clear_existing_handlers=True):
    if logger is None:
        logger = logging.root
    else:
        logger = getLogger(logger)

    if clear_existing_handlers:
        logger.handlers = []

    handlers = []
    if filename:
        handlers.append(
            add_file_handler(logger=logger, filename=filename, logdir=logdir, level=file_level))
    if stderr_level:
        handlers.append(
            add_stderr_handler(logger=logger, level=stderr_level)
        )
    return handlers


def add_file_handler(logger=logging.root, level=None,
                     filename=None, logdir=None):
    level = level or logging.INFO
    logdir = logdir or DEFAULT_LOGDIR
    if logger.getEffectiveLevel() > level:
        logger.setLevel(level)
    if filename is None:
        filename = os.path.split(sys.argv[0])[1] + '.log'
    path = filename if os.path.sep in filename else os.path.join(logdir, filename)
    fh = logging.FileHandler(path, encoding='utf-8')
    fh.setLevel(level)
    fh.setFormatter(logging.Formatter(PRECISE_FORMAT, datefmt=DATETIME_FORMAT))
    logger.addHandler(fh)
    return fh


def add_stderr_handler(logger=logging.root, level=logging.INFO):
    if logger.getEffectiveLevel() > level:
        logger.setLevel(level)
    se = logging.StreamHandler()
    se.setLevel(level)
    se.setFormatter(logging.Formatter(PRECISE_FORMAT, datefmt=TIME_FORMAT))
    logger.addHandler(se)
    return se


class LoggingContext(object):
    oldhandlers = None
    handlers = None

    configure_args = None

    def __init__(self, **kwargs):
        # eliminate None in dict
        self.logger = kwargs['logger'] = kwargs.get('logger') or logging.root
        self.configure_args = kwargs

    def __enter__(self):
        self.oldhandlers = self.logger.handlers
        self.handlers = configure(**self.configure_args)

    def __exit__(self, et, ev, tb):
        self.logger.handlers = self.oldhandlers

        for h in self.handlers:
            h.close()
