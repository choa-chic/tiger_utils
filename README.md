# Utilities for downloading TIGER/Line Shapefiles from the US Census Bureau

## Download files
 
Show download status for all states/territories and exit (will likely be empty if nothing else has been initiated):

`python -m tiger_utils.tiger_cli --show-status`

### Discover available files
Discover available files by scraping Census Bureau directories and populating the state database without downloading:
   
`python -m tiger_utils.tiger_cli --discover-only`

### Sync database state with files on disk
Synchronize state database with files on disk (mark completed if file exists):

`python -m tiger_utils.tiger_cli --sync-state`

### Start downloading files
Download all data for 2025 (default 50 states):

`python -m tiger_utils.tiger_cli`

Download data for specific states (e.g., California and Texas):

`python -m tiger_utils.tiger_cli --year 2025 --states 06 48`

## Load into Database

### Load into DeGAUSS-compatible SQLite database

The SQLite loader follows the [DeGAUSS-org/geocoder](https://github.com/DeGAUSS-org/geocoder) implementation pattern with temporary tables and transformation steps.

#### Full import workflow:
```bash
python -m tiger_utils.load_db.degauss.importer all ./tiger_data/2025 --db geocoder.db --recursive --state 13 --type edges
```

#### Step-by-step commands:
```bash
# 1. Unzip TIGER/Line files
python -m tiger_utils.load_db.degauss.importer unzip ./tiger_data/2025 ./unzipped --recursive --state 13

# 2. Create database schema
python -m tiger_utils.load_db.degauss.importer schema --db geocoder.db

# 3. Create indexes
python -m tiger_utils.load_db.degauss.importer indexes --db geocoder.db
```

#### Import workflow details:

The import process uses the DeGAUSS pattern:
1. **Unzip** - Extract TIGER/Line shapefiles
2. **Create Schema** - Create final tables (place, edge, feature, feature_edge, range)
3. **Create Temp Tables** - Create temporary staging tables (tiger_edges, tiger_featnames, tiger_addr)
4. **Load to Temp** - Load shapefiles into temporary tables
5. **Transform** - Transform temporary tables to final tables using `convert.sql`
6. **Create Indexes** - Add indexes for query performance

See `tiger_utils/load_db/degauss/sql/README.md` for detailed documentation on the SQL files and transformation logic.

### Load downloaded shapefiles into PostGIS database:
*(PostGIS implementation - see postgis.py)*