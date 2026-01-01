# Quick Start: Python TIGER/Line SQLite Loader

Get up and running with the degauss-equivalent Python TIGER/Line importer.

## Prerequisites

1. Python 3.8+
2. TIGER/Line shapefiles (download from [Census Bureau](https://www.census.gov/cgi-bin/geo/shapefiles/))
3. `fiona` and `shapely` packages

## Installation

### 1. Install Dependencies

```bash
cd /path/to/tiger_utils
pip install -r requirements.txt
```

This installs:
- `fiona` - Read shapefiles
- `shapely` - Geometry handling
- All other tiger_utils dependencies

### 2. Verify Installation

```python
python -c "import fiona; import shapely; print('OK')"
```

## Download TIGER/Line Data

### Option A: US Census Bureau FTP
```bash
# Browse: https://www2.census.gov/geo/tiger/TIGER2025/
# Download edges, featnames, addr files for your counties

wget https://www2.census.gov/geo/tiger/TIGER2025/EDGES/tl_2025_06001_edges.zip
wget https://www2.census.gov/geo/tiger/TIGER2025/FEATNAMES/tl_2025_06001_featnames.zip
wget https://www2.census.gov/geo/tiger/TIGER2025/ADDR/tl_2025_06001_addr.zip
```

### Option B: Using tiger_utils Downloader
```python
from tiger_utils.download.downloader import download_county_data
import asyncio

asyncio.run(download_county_data(
    state_fips="06",
    year=2025,
    output_dir="tiger_data/2025",
    dataset_types=["EDGES", "FEATNAMES", "ADDR"],
    parallel=4,
))
```

## Load Data to SQLite

### Quick Start (5 lines)

```python
from tiger_utils.load_db.degauss import import_tiger_data

import_tiger_data(
    db_path='geocoder.db',
    source_dir='tiger_data/2025/',
)
```

**That's it!** Your SQLite database is ready.

### Command Line

```bash
python -m tiger_utils.load_db.degauss.importer_cli \
    geocoder.db \
    tiger_data/2025/
```

## Verify Your Database

### Check Tables

```bash
sqlite3 geocoder.db "SELECT name FROM sqlite_master WHERE type='table';"
```

Expected output:
```
place
edge
feature
feature_edge
range
```

### Count Records

```bash
sqlite3 geocoder.db "SELECT 
    (SELECT COUNT(*) FROM edge) as edges,
    (SELECT COUNT(*) FROM feature) as features,
    (SELECT COUNT(*) FROM range) as ranges;"
```

### Query Example

```bash
sqlite3 geocoder.db "SELECT street, zip FROM feature LIMIT 5;"
```

## Advanced Usage

### Import Specific Counties

```python
from tiger_utils.load_db.degauss import TigerImporter

importer = TigerImporter(
    db_path='geocoder.db',
    source_dir='tiger_data/2025/',
)

# Import California (06) and Nevada (32)
importer.import_all(counties=['06001', '06007', '32003'])
importer.create_indexes()
```

### With Verbose Logging

```python
from tiger_utils.load_db.degauss import TigerImporter

importer = TigerImporter(
    db_path='geocoder.db',
    source_dir='tiger_data/2025/',
    verbose=True,  # See debug messages
)
importer.import_all()
```

### CLI with Options

```bash
# Import specific counties with verbose output
python -m tiger_utils.load_db.degauss.importer_cli \
    geocoder.db \
    tiger_data/2025/ \
    06001 06007 06019 \
    -v

# Use custom temp directory
python -m tiger_utils.load_db.degauss.importer_cli \
    geocoder.db \
    tiger_data/2025/ \
    --temp-dir /mnt/nvme/tmp \
    --batch-size 5000
```

## Common Tasks

### Add More Counties Later

```python
from tiger_utils.load_db.degauss import TigerImporter

importer = TigerImporter(
    db_path='geocoder.db',
    source_dir='tiger_data/2025/',
)
importer.import_all(counties=['06001', '36061'])  # Add NY counties
importer.create_indexes()
```

### Find Streets

```bash
sqlite3 geocoder.db "
SELECT DISTINCT street FROM feature 
WHERE zip = '95814' 
ORDER BY street 
LIMIT 10;"
```

### Find Address Ranges

```bash
sqlite3 geocoder.db "
SELECT tlid, fromhn, tohn, side FROM range 
WHERE zip = '95814' 
ORDER BY fromhn;"
```

### Create Geometry from WKB

If you have SpatiaLite extension:

```bash
sqlite3 geocoder.db "
SELECT tlid, ST_GeomFromWKB(geometry) FROM edge 
WHERE tlid = 123456789;"
```

Without SpatiaLite, WKB is stored as BLOB (binary data).

## Troubleshooting

### ModuleNotFoundError: No module named 'fiona'

**Solution**:
```bash
pip install fiona shapely
```

### FileNotFoundError: Source directory not found

**Check**: Directory exists and contains TIGER files
```bash
ls tiger_data/2025/tl_*_edges.zip
```

### Database is Locked

**Cause**: Another process using the database

**Solutions**:
1. Close other database connections
2. Use WAL mode (default): `PRAGMA journal_mode=WAL;`
3. Increase timeout:
   ```python
   import sqlite3
   conn = sqlite3.connect('geocoder.db', timeout=30)
   ```

### Out of Memory

**Reduce batch size**:
```python
importer = TigerImporter(
    db_path='geocoder.db',
    source_dir='tiger_data/',
    batch_size=500,  # Default 1000
)
```

## Performance

Typical performance on modern hardware:

| Task | Time |
|------|------|
| Import 1 county | 2-5 min |
| Create indexes | 30-60 sec |
| Query street | < 100 ms |
| Query address range | < 100 ms |

**Optimization Tips**:
- Use SSD for database file
- Increase `batch_size` for faster bulk loads
- Use WAL mode for concurrent access
- Create additional indexes for custom queries

## Next Steps

1. **[Read the full documentation](./tiger_utils/load_db/degauss/README.md)**
2. **[Review the implementation details](./DEGAUSS_IMPLEMENTATION.md)**
3. **[See the mapping to original degauss](./DEGAUSS_MAPPING.md)**

## Questions?

See the detailed documentation in:
- `tiger_utils/load_db/degauss/README.md` - Full API reference
- `DEGAUSS_IMPLEMENTATION.md` - Implementation overview
- `DEGAUSS_MAPPING.md` - Mapping to original C/Bash code

## Example: Complete Workflow

```python
# 1. Download TIGER data
from tiger_utils.download.downloader import download_county_data
import asyncio

asyncio.run(download_county_data(
    state_fips="06",
    year=2025,
    output_dir="tiger_data/2025",
    dataset_types=["EDGES", "FEATNAMES", "ADDR"],
))

# 2. Load to SQLite
from tiger_utils.load_db.degauss import import_tiger_data

import_tiger_data(
    db_path='geocoder.db',
    source_dir='tiger_data/2025/',
)

# 3. Query the database
import sqlite3

conn = sqlite3.connect('geocoder.db')
cur = conn.cursor()

# Find streets in a ZIP
cur.execute("""
    SELECT DISTINCT street FROM feature 
    WHERE zip = '95814' 
    ORDER BY street 
    LIMIT 10
""")

for row in cur.fetchall():
    print(row[0])

conn.close()
```

Done! You now have a geocoding database.
