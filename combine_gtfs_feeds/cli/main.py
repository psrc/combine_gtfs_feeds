import sys

from combine_gtfs_feeds.cli import CLI
from combine_gtfs_feeds.cli import run


from combine_gtfs_feeds import __version__, __doc__


def main():
    combine = CLI(version=__version__,
               description=__doc__)
    combine.add_subcommand(name='run',
                        args_func=run.add_run_args,
                        exec_func=run.run,
                        description=run.run.__doc__)
    
    sys.exit(combine.execute())
