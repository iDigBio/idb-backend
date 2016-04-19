from __future__ import division, absolute_import
from __future__ import print_function

import logging
import os

import click

from .clibase import cli, clilog  # noqa

import idb.start_data_api  # noqa

@cli.group()
def datatables():
    pass

@datatables.command(name='rights-strings')
def rights_strings():
    from idb.data_tables.rights_strings import main
    main()


@datatables.command(name='locality-data')
def locality_data():
    from idb.data_tables.locality_data import main
    main()

@datatables.command(name="taxon")
def taxon():
    from idb.data_tables.taxon import main
    main()
