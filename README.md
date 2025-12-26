# Utilities for downloading TIGER/Line Shapefiles from the US Census Bureau
 
Show download status for all states/territories and exit (will likely be empty if nothing else has been initiated):

`python -m tiger_utils.tiger_cli --show-status`

## Discover available files
Discover available files by scraping Census Bureau directories and populating the state database without downloading:
   
`python -m tiger_utils.tiger_cli --discover-only`

## Sync database state with files on disk
Synchronize state database with files on disk (mark completed if file exists):

`python -m tiger_utils.tiger_cli --sync-state`

## Start downloading files
Download all data for 2025 (default 50 states):

`python -m tiger_utils.tiger_cli`

Download data for specific states (e.g., California and Texas):

`python -m tiger_utils.tiger_cli --year 2025 --states 06 48`