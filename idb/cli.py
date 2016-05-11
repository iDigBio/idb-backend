from __future__ import division, absolute_import
from __future__ import print_function

from .clibase import cli, clilog  # noqa

# the following imports register subcommands.
#
# Each of these modules has been constructed to import
# lazily. E.g. indexing only builds the command object and doesn't
# import any of the elasticsearch code until one of the commands in it
# is actually invoked. This helps keep startup speed quickly and makes
# sure the whole world isn't loaded for any and every subcommand
# invokation.

from idb import (data_api, indexing, stats)  # noqa

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
