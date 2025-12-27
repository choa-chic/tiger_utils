# SQL Files for DeGAUSS-Compatible TIGER/Line Import

This directory contains SQL files referenced from the [DeGAUSS-org/geocoder](https://github.com/DeGAUSS-org/geocoder) implementation for loading TIGER/Line data into SQLite.

## Workflow

The import process follows the DeGAUSS pattern with these stages:

1. **create.sql** - Creates final database schema (place, edge, feature, feature_edge, range)
2. **setup.sql** - Creates temporary tables (tiger_edges, tiger_featnames, tiger_addr) 
3. **Load Phase** - Shapefiles are loaded into temporary tables
4. **convert.sql** - Transforms temporary tables into final tables with ETL operations
5. **index.sql** - Creates indexes for query optimization

## Table Structure

### Final Tables (create.sql)
- `place` - Gazetteer of place names (cities, ZIP codes)
- `edge` - Line geometries with TLID (TIGER Line IDs)
- `feature` - Street names and associated ZIPs
- `feature_edge` - Links features to edges
- `range` - Address ranges for each edge

### Temporary Tables (setup.sql)
- `tiger_edges` - Raw edge data from TIGER shapefiles
- `tiger_featnames` - Raw feature name data 
- `tiger_addr` - Raw address range data

## Transform Logic (convert.sql)

The conversion process:
1. Index temporary tables by TLID
2. Create `linezip` - Maps edges to ZIP codes
3. Create `feature_bin` - Generates features with phonetic hashing
4. Populate `feature_edge` - Links features to edges
5. Populate final tables from temporary data
6. Clean up temporary tables

## Differences from Original DeGAUSS Implementation

The original DeGAUSS implementation uses custom C extensions for:
- `metaphone()` - Phonetic hashing for street name matching
- `compress_wkb_line()` - Geometry compression
- `digit_suffix()` / `nondigit_prefix()` - Address number parsing

This implementation provides simplified alternatives:
- `substr(lower(name), 1, 5)` instead of `metaphone(name, 5)`
- Direct WKB storage instead of compressed format
- SQL TRIM/CAST operations for address parsing

These changes maintain compatibility while avoiding C extension dependencies, though they may impact:
- Database size (without geometry compression)
- Query performance (without phonetic matching)
- Address matching accuracy (simplified parsing)

## Usage

The SQL files are automatically loaded by the `db_setup.py` module:

```python
from tiger_utils.load_db.degauss import importer

# Full import workflow
importer.import_tiger(
    zip_dir="./tiger_data/2025",
    db_path="geocoder.db",
    recursive=True,
    state="13",  # Optional: filter by state FIPS
    shape_type="edges"  # Optional: filter by shape type
)
```

## References

- DeGAUSS geocoder: https://github.com/DeGAUSS-org/geocoder
- TIGER/Line shapefiles: https://www.census.gov/geographies/mapping-files/time-series/geo/tiger-line-file.html
