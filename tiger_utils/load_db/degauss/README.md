# TIGER/Line SQLite Loader (degauss module)

Python implementation of the [degauss-org/geocoder](https://github.com/degauss-org/geocoder) TIGER/Line import workflow. Replicates the C+Bash tooling in pure Python for cross-platform compatibility and ease of deployment.

# Quick start usage
## Python environment
```sh
uv venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
uv pip install -r requirements.txt
```

## Initialize a new geocoder database
```sh
python -m tiger_utils.load_db.degauss.importer_cli --init-db
```

## Import TIGER/Line shapefiles, optionally by year and/or state FIPS
```sh
python -m tiger_utils.load_db.degauss.importer_cli /data/tiger --state 13 --year 2025
```

## Import all available TIGER/Line shapefiles in a directory for a given year
```sh
python -m tiger_utils.load_db.degauss.importer_cli --year 2025
```

## Overview

This module provides tools to build a SQLite geocoding database from US Census TIGER/Line shapefiles, following the schema and conventions of the degauss geocoder:

- **place**: Gazetteer of place names (cities, states, ZIP codes, coordinates)
- **edge**: Line geometries with TIGER Line IDs (stored as WKB BLOB)
- **feature**: Street names with phonetic codes (metaphone)
- **feature_edge**: Links features to edges
- **range**: Address ranges (low/high house numbers, side, ZIP)

## Key Features

### Advantages Over Original C Implementation

| Feature | C (degauss) | Python (this module) |
|---------|-----------|----------------------|
| Geometry Format | PostGIS WKB/WKT | Shapely WKB (portable) |
| Bulk Loading | COPY/INSERT via SQL | Batch inserts + transaction |
| Error Handling | Limited feedback | Comprehensive logging |
| Testing | Manual | pytest-compatible |

### Geometry Handling

- **Input**: ESRI Shapefiles (.shp) with coordinate geometry
- **Processing**: Fiona reads shapefiles, Shapely handles geometry
- **Storage**: WKB (Well-Known Binary) BLOB format in SQLite
- **Output**: Valid for ST_AsWKB() queries and spatial extensions

## Installation

The required packages are in `tiger_utils/requirements.txt`:

```bash
# Install dependencies
pip install -r requirements.txt
```

## Usage

### Command-Line Interface

```bash
# Import all counties in a directory (auto-detect from filenames)
python -m tiger_utils.load_db.degauss.importer_cli /data/geocoder.db /data/tiger_files/

# Import specific counties
python -m tiger_utils.load_db.degauss.importer_cli /data/geocoder.db /data/tiger_files/ 06001 06007 06019

# Enable verbose logging
python -m tiger_utils.load_db.degauss.importer_cli /data/geocoder.db /data/tiger_files/ -v

# Specify temporary directory
python -m tiger_utils.load_db.degauss.importer_cli /data/geocoder.db /data/tiger_files/ --temp-dir /tmp/tiger
```

### Python API

#### Simple Import

```python
from tiger_utils.load_db.degauss import import_tiger_data

# Import all counties in a directory
import_tiger_data('/data/geocoder.db', '/data/tiger_files/')

# Import specific counties
import_tiger_data('/data/geocoder.db', '/data/tiger_files/', 
                 counties=['06001', '06007', '06019'])
```

#### Advanced Usage

```python
from tiger_utils.load_db.degauss import TigerImporter

# Create importer
importer = TigerImporter(
    db_path='/data/geocoder.db',
    source_dir='/data/tiger_files/',
    batch_size=1000,
    verbose=True
)

# Import specific counties
importer.import_all(counties=['06001', '06007'])

# Or auto-detect and import all
importer.import_all()

# Create final indexes
importer.create_indexes()
```

#### Direct Shapefile Loading

```python
from tiger_utils.load_db.degauss import convert_shapefile

# Load a single shapefile
convert_shapefile(
    shp_path='/path/to/tl_2025_06001_edges.shp',
    db_path='/data/geocoder.db',
    table_name='tiger_edges',
    geometry_column='the_geom',
)

# Load DBF only (attributes, no geometry)
convert_shapefile(
    shp_path='/path/to/tl_2025_06001_addr.dbf',
    db_path='/data/geocoder.db',
    table_name='tiger_addr',
    dbf_only=True,
)
```

#### Custom Schema

```python
from tiger_utils.load_db.degauss import create_schema, create_indexes

# Initialize empty database
create_schema('/data/geocoder.db')

# Create indexes after data loading
create_indexes('/data/geocoder.db')
```

## Module Structure

```
tiger_utils/load_db/degauss/
├── __init__.py           # Package exports
├── load_db.py            # High-level API
├── shp_to_sqlite.py      # Shapefile loader (replaces shp2sqlite.c)
├── tiger_importer.py     # Orchestrator (replaces tiger_import bash script)
├── db_setup.py           # Schema and indexes (replaces build/sql/*.sql)
├── importer_cli.py       # CLI interface
└── README.md             # This file
```

### shp_to_sqlite.py

Converts ESRI shapefiles to SQLite tables with WKB geometry.

```python
from tiger_utils.load_db.degauss.shp_to_sqlite import ShapefileToSQLiteConverter

converter = ShapefileToSQLiteConverter(
    db_path='/data/geocoder.db',
    table_name='tiger_edges',
    geometry_column='the_geom',
    simple_geometries=False,  # Convert MULTI* to single types
)
converter.load_shapefile('/path/to/edges.shp')
```

**Features**:
- Reads .shp (geometry) and .dbf (attributes) files
- Converts column types (int → INTEGER, str → VARCHAR, etc.)
- Handles reserved column names (gid, tableoid, cmin, etc.)
- Batch inserts for performance
- WKB geometry storage

### tiger_importer.py

Orchestrates TIGER/Line import following the degauss workflow:

1. Extract TIGER files (edges, featnames, addr)
2. Load shapefiles into temporary tables
3. Create indexes on temporary tables
4. Transform data (match ZIPs, compute metaphone, link features)
5. Populate permanent tables
6. Clean up temporary tables

```python
from tiger_utils.load_db.degauss import TigerImporter

importer = TigerImporter(
    db_path='/data/geocoder.db',
    source_dir='/data/tiger_files/',
)
importer.import_county('06001')  # Import single county
importer.create_indexes()
```

**Workflow** (matches convert.sql):
- **linezip**: Creates temporary table matching edges to ZIP codes
- **feature_bin**: Generates features with street names and metaphone codes
- **feature_edge**: Links features to edges
- **edge**: Stores geometries in WKB format
- **range**: Stores address ranges with house number prefixes

### db_setup.py

Creates database schema and indexes.

```python
from tiger_utils.load_db.degauss import create_schema, create_indexes

# Initialize new database
create_schema('/data/geocoder.db')

# Create indexes for queries
create_indexes('/data/geocoder.db')
```

**Tables Created**:
- `place(zip, city, state, city_phone, lat, lon, status, fips_class, fips_place, fips_county, priority)`
- `edge(tlid, geometry)` — Primary key: tlid
- `feature(fid, street, street_phone, paflag, zip)` — Primary key: fid
- `feature_edge(fid, tlid)` — Links features to edges
- `range(tlid, fromhn, tohn, prenum, zip, side)` — Address ranges

**Indexes Created**:
- `place_city_phone_state_idx(city_phone, state)`
- `place_zip_priority_idx(zip, priority)`
- `feature_street_phone_zip_idx(street_phone, zip)`
- `feature_edge_fid_idx(fid)`
- `range_tlid_idx(tlid)`

### importer_cli.py

Command-line interface wrapping the TigerImporter class.

```bash
python -m tiger_utils.load_db.degauss.importer_cli --help
```

**File Naming Convention**:
- `tl_YYYY_COUNTYFIPS_edges.zip` — Street network (LINESTRING geometry)
- `tl_YYYY_COUNTYFIPS_featnames.zip` — Feature names (attributes only)
- `tl_YYYY_COUNTYFIPS_addr.zip` — Address ranges (attributes only)

County FIPS codes are 5 digits: state (2) + county (3). Example:
- CA (06) + Sacramento (067) = 06067
- NY (36) + New York (061) = 36061

## Database Queries

After import, you can query the database:

```sql
-- Find a street
SELECT fid, street, zip FROM feature 
WHERE street_phone GLOB 'S*' AND zip = '95814' 
LIMIT 10;

-- Find address ranges for a street
SELECT tlid, fromhn, tohn, side FROM range 
WHERE zip = '95814' 
ORDER BY tlid;

## Performance Tuning

### Import Performance

Batch size and pragmas significantly affect speed:

```python
importer = TigerImporter(
    db_path='/data/geocoder.db',
    source_dir='/data/tiger_files/',
    batch_size=5000,  # Increase for faster bulk loads
)
```

**Recommended Settings**:
- **batch_size=1000-5000**: Balance memory vs. database round-trips
- **PRAGMA journal_mode=WAL**: Better concurrency
- **PRAGMA synchronous=NORMAL**: Balance speed and safety
- **PRAGMA cache_size=500000**: Increase for large imports

## Troubleshooting

### Module Not Found

```bash
# Ensure tiger_utils is installed
pip install -e /path/to/tiger_utils

# Or add to PYTHONPATH
export PYTHONPATH=/path/to/tiger_utils:$PYTHONPATH
```

### Missing Dependencies

```bash
# Install required packages
pip install -r requirements.txt

# Verify installation
python -c "import fiona; import shapely; print('OK')"
```

### File Not Found

Check that source directory contains TIGER files:

```bash
ls /path/to/tiger_files/tl_*_edges.zip
```

Expected patterns:
- `tl_YYYY_COUNTYFIPS_edges.zip`
- `tl_YYYY_COUNTYFIPS_featnames.zip`
- `tl_YYYY_COUNTYFIPS_addr.zip`

### Database Locked

If you get "database is locked" errors:
1. Ensure no other processes are using the database
2. Increase `timeout` in `sqlite3.connect()`
3. Use WAL mode: `PRAGMA journal_mode=WAL;`

### Out of Memory

For very large counties:
1. Reduce `batch_size` to use less memory
2. Import counties one at a time
3. Use temporary directory on fast storage

## Comparison with Original

| Aspect | Original (C+Bash) | Python Implementation |
|--------|-------------------|----------------------|
| File | src/shp2sqlite/shp2sqlite.c | shp_to_sqlite.py |
| File | build/tiger_import | tiger_importer.py + importer_cli.py |
| File | build/sql/*.sql | db_setup.py |
| Dependency | libshp, PostGIS, SQL | fiona, shapely |
| Metaphone | C code in SQL | Python (Double Metaphone via metaphone pkg) |
| Geometry | PostGIS WKT/WKB | Shapely WKB |
| Temp Tables | SQL temporary tables | SQLite TEMPORARY |
| Indexes | Separate index.sql | create_indexes() |

## Notes on Metaphone Encoding

The original degauss geocoder uses SQL metaphone() functions. This implementation now:

1. Uses a Python UDF backed by `metaphone.doublemetaphone()` to compute phonetic codes
2. Truncates codes to 5 characters to mirror `metaphone(name, 5)` in the SQL workflow
3. Registers the UDF with SQLite so downstream SQL continues to work unchanged

This preserves deployment simplicity (no compiled SQLite extensions) while matching the
intended metaphone behavior.

## License & Attribution

This module reimplements the workflow from [degauss-org/geocoder](https://github.com/degauss-org/geocoder), which is licensed under the [MIT License](https://github.com/degauss-org/geocoder/blob/master/LICENSE).

## See Also

- [TIGER/Line Shapefiles Documentation](https://www.census.gov/geographies/mapping-files/time-series/geo/tiger-line-file.html)
- [degauss-org/geocoder](https://github.com/degauss-org/geocoder)
- [Fiona Documentation](https://fiona.readthedocs.io/)
- [Shapely Documentation](https://shapely.readthedocs.io/)
- [SQLite WKB Support](https://www.sqlite.org/contrib/download/SQL)
