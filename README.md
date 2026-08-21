# combine_gtfs_feeds
A tool to combine gtfs feeds into one feed/dataset. The main purpose of combine_gtfs_feeds is to be able work from one GTFS feed when performing transit service analysis for a particular geographic location.   
  
Full documentation here:
https://github.com/psrc/combine_gtfs_feeds/wiki/combine_gtfs_feeds-documentation

## Quick start with uv

Syncing the environment is enough to install this package and its CLI command:

```bash
uv sync
```

Run the CLI without activating an environment:

```bash
uv run combine_gtfs_feeds --help
```

Or activate the environment and call it directly:

```bash
# Command Prompt (Windows)
.venv\Scripts\activate.bat
combine_gtfs_feeds --help
```

