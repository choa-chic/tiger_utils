# Usage Instructions for TIGER/Line SQLite Import Scripts

This module provides a workflow for importing US Census TIGER/Line shapefiles into a SpatiaLite-enabled SQLite database, following the DeGAUSS-org/geocoder ETL pattern.

## Main Entry Point

### importer.py

This script orchestrates the full import process. Run it as a CLI:

```bash
python -m tiger_utils.load_db.degauss.importer all <zip_dir> [--db DB_PATH] [--tmp TMP_DIR] [--recursive] [--state STATE_FIPS] [--type SHAPE_TYPE]
```

**Commands:**
- `all`: Full workflow (unzip, schema, temp tables, shapefile load, transform, index)
- `unzip`: Only unzip TIGER/Line zip files
- `schema`: Create main database schema
- `indexes`: Create indexes
- `shp`: Import shapefiles into database

**Common options:**
- `--db`: Output SQLite DB path (default: geocoder.db)
- `--tmp`: Temp directory for unzipped files (default: _tiger_tmp)
- `--recursive`: Recursively search for zip files
- `--state`: State FIPS code to filter files (e.g., 13)
- `--type`: Shape type to filter files (e.g., edges, faces)

## Supporting Modules

- **db_setup.py**: Functions to create schema, indexes, and temp tables from SQL templates.
- **shp_to_sqlite.py**: Loads a single shapefile into a SpatiaLite-enabled SQLite table, dynamically creating the schema.
- **shp_to_temp_tables.py**: Loads TIGER/Line shapefiles into pre-defined temporary tables for ETL.

## Example: Full Import

```bash
python -m tiger_utils.load_db.degauss.importer all ./tiger_zips --db geocoder.db --tmp _tiger_tmp --recursive --state 06 --type edges
```

## Example: Import Shapefiles Only

```bash
python -m tiger_utils.load_db.degauss.importer shp ./unzipped_shapefiles --db geocoder.db
```

## Notes

- For custom shapefile schemas, use `shp_to_sqlite.py` directly for dynamic table creation.
- For strict TIGER/Line ETL, use the full workflow with temp tables and SQL transforms.

---
