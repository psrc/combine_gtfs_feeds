from combine_gtfs_feeds import cli
import pandas as pd

test = cli.run.combine(
    r"C:\Stefan\out_delete\chicago_gtfs", 20190304, r"C:\Stefan\out_delete\output"
)
print("done")
