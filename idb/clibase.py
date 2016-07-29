from __future__ import division, absolute_import
from __future__ import print_function

import functools
import itertools
import logging
import functools
import os
import sys

import click
import idb

from idb.helpers.logging import configure_app_log, idblogger

clilog = idblogger.getChild('cli')


def get_std_options():
    return [
        click.Option(
            ['--verbose', '-v'],
            count=True,
            help="Output more log messages, repeat for increased verbosity"),
        click.Option(
            ['--config'],
            type=click.Path(exists=True, dir_okay=False, resolve_path=True),
            help="JSON config file to load. config value precedence: "
            "default config path < PATH < environment values < command line"),
        click.Option(
            ['--env'],
            envvar="ENV",
            default=None,
            type=click.Choice(['dev', 'test', 'beta', 'prod'])),
        click.Option(
            ['--logfile'],
            envvar="LOGFILE",
            type=click.Path(file_okay=True, dir_okay=False, writable=True),
            help="If specified path to write logfile to"),
        click.Option(
            ['--journal/--no-journal'],
            help="When set configures logging to stderr expecting to be caught by journald",
        ),
        click.Option(['--idb-uuid'],
                     envvar="IDB_UUID",
                     type=click.UUID),
        click.Option(['--idb-apikey'], envvar="IDB_APIKEY"),
        click.Option(['--idb-dbpass'], envvar="IDB_DBPASS"),
        click.Option(['--idb-storage-access-key'], envvar="IDB_STORAGE_ACCESS_KEY"),
        click.Option(['--idb-storage-secret-key'], envvar="IDB_STORAGE_SECRET_KEY"),
        click.Option(['--idb-crypt-key'], envvar="IDB_CRYPT_KEY"),
    ]

def add_std_options(fn):
    fn.params += get_std_options()

def handle_std_options(verbose=None, env=None, config=None, logfile=None, journal=False, **kwargs):
    configure_app_log(verbose=verbose, logfile=logfile, journal=journal)

    #it's important we don't import config (which will modify the
    #environment) until after click gets a chance to inspect the
    #environment to fill CLI parameters
    from . import config as _config  # noqa

    if config is not None:
        _config.load_config_file(config)

    if env is not None:
        _config.ENV = env
        os.environ['ENV'] = env

    for k,v in kwargs.items():
        if k.startswith('idb_') and v is not None:
            v = u"{0}".format(v)
            setattr(_config, k.upper(), v)
            os.environ[k.upper()] = v

    # idb uses SIGUSR1 to output extra logging/current status
    # information. This will be configured in certain contexts but the
    # idb process default should be ignore instead of the python
    # default of exit.
    from signal import signal, SIGUSR1, SIG_IGN
    signal(SIGUSR1, SIG_IGN)


@click.group()
@click.version_option(version=idb.__version__)
def cli(**kwargs):
    handle_std_options(**kwargs)
add_std_options(cli)


# Dry run helper logic
DRY_RUN = False

def maybe_dry_run(fn=None, logcallback=None, logmsg=None, logfn=idblogger.debug, retval=None):
    def wrapper(*args, **kwargs):
        if DRY_RUN:
            if logcallback:
                logcallback(*args, **kwargs)
            elif logfn:
                if logmsg:
                    logfn(logmsg)
                else:
                    fnname = getattr(fn, 'func_name', fn)
                    argsreprs = itertools.chain(
                        itertools.imap(repr, args),
                        itertools.starmap(u'{0!s}={1!r}'.format, kwargs.items()))
                    logfn('DRY_RUN: {0}({1})', fnname, u', '.join(argsreprs))
            return retval
        return fn(*args, **kwargs)

    if fn:
        return functools.wraps(fn)(wrapper)
    else:
        # save any passed params and return a partial that waits for the `fn`
        return functools.partial(
            maybe_dry_run, logcallback=logcallback, logmsg=logmsg,
            logfn=logfn, retval=retval)


def dry_run_aware(fn):
    def drcallback(ctx, param, value):
        global DRY_RUN
        if value is not None and not ctx.resilient_parsing:
            clilog.notice("Setting DRY_RUN = {0}", value)
            DRY_RUN = value
    dropt = click.Option(['--dry-run/--no-dry-run'],
                         callback=drcallback, expose_value=False,
                         default=None,
                         help="Enable DRY RUN mode, simulates but prevents external actions.")
    click.decorators._param_memo(fn, dropt)

    return fn
