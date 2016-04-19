from __future__ import division, absolute_import
from __future__ import print_function

import itertools
import logging
import functools
import os

import click

from idb.helpers.logging import configure_app_log, idblogger

clilog = idblogger.getChild('cli')


def get_std_options():
    return [
        click.Option(['--verbose', '-v'], count=True,
                     help="Output more log messages, repeat for increased verbosity"),
        click.Option(['--config'], type=click.Path(exists=True, dir_okay=False),
                     help="JSON config file to load. config value precedence: "
                     "default config path < PATH < environment values < command line values."),
        click.Option(['--env'], envvar="ENV",
                     default=None,
                     type=click.Choice(['dev', 'test', 'beta', 'prod'])),
        click.Option(['--idb-uuid'], envvar="IDB_UUID", type=click.UUID),
        click.Option(['--idb-apikey'], envvar="IDB_APIKEY"),
        click.Option(['--idb-dbpass'], envvar="IDB_DBPASS"),
        click.Option(['--idb-storage-access-key'], envvar="IDB_STORAGE_ACCESS_KEY"),
        click.Option(['--idb-storage-secret-key'], envvar="IDB_STORAGE_SECRET_KEY"),
        click.Option(['--idb-crypt-key'], envvar="IDB_CRYPT_KEY"),
    ]

def add_std_options(fn):
    fn.params += get_std_options()

def handle_std_options(verbose=None, env=None, config=None, **kwargs):
    configure_app_log(verbose)
    from . import config as _config  # noqa

    if config is not None:
        _config.load_config_file(config)

    if env is not None:
        _config.ENV = env
        os.environ['ENV'] = env

    for k,v in kwargs.items():
        if k.startswith('idb_') and v is not None:
            setattr(_config, k.upper(), v)
            os.environ[k.upper()] = v


@click.group()
def cli(**kwargs):
    handle_std_options(**kwargs)
add_std_options(cli)
