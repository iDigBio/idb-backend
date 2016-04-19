DRY_RUN = False

import click

@click.group()
@click.option('--verbose', '-v', count=True,
              help="Output more log messages, repeat for increased verbosity")
@click.option("--env", type=str)
def cli():
    pass
