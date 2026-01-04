# Utilities for downloading TIGER/Line Shapefiles from the US Census Bureau

By default, only the layers needed for DeGAUSS geocoding are downloaded (EDGES, ADDR, FEATNAMES). To download all layers needed for PostGIS, use the `--postgis-all` flag with the download command.

## Download files
 
Show download status for all states/territories and exit (will likely be empty if nothing else has been initiated):

`python -m tiger_utils download --show-status`

### Discover available files
Discover available files by scraping Census Bureau directories and populating the state database without downloading:
   
`python -m tiger_utils download --discover-only`
`python -m tiger_utils download --discover-only --year 2025 --postgis-all`

### Sync database state with files on disk
Synchronize state database with files on disk (mark completed if file exists):

`python -m tiger_utils download --sync-state`

### Start downloading files
For more parallelization (if it gets too high, server will start rejecting requests)

`python -m tiger_utils download --parallel 16`
If `--year` was set in discovery above, (this should be persisted in the config unless you change it).

Configuration precedence (highest to lowest):
1. Command-line arguments (highest priority)
2. Saved configuration from state tracker
3. Default values (lowest priority)

Download all data for 2025 (default 50 states):

(You could start with this command if you haven't discovered files yet, it will do that automatically)
`python -m tiger_utils download --year 2025`



`python -m tiger_utils download --year 2021`

Download data for specific states (e.g., California and Texas):

`python -m tiger_utils download --year 2025 --states 06 48`

## Load into Database
### Load downloaded shapefiles into PostGIS database:
### Load into DeGAUSS compatible sqlite database:
`python -m tiger_utils.load_db.degauss.importer all ./tiger_data/2025 --db geocoder.db --recursive --state 13 --type edges`