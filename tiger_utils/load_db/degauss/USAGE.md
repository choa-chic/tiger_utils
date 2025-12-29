# Usage Instructions for TIGER/Line SQLite Import Scripts

This module provides a three-phase ETL workflow for importing US Census TIGER/Line shapefiles into a SQLite database, following the [DeGAUSS-org/geocoder](https://github.com/degauss-org/geocoder) import pattern.

## Workflow Overview

The import process follows a proven ETL pattern:

1. **Stage** - Load raw shapefiles into temporary `tiger_*` tables with minimal transformation
2. **Transform** - Use SQL scripts to normalize and consolidate staged data into final schema
3. **Index** - Create indexes on final tables for query performance

This approach provides:
- **Debuggability** - Inspect staging tables to diagnose data issues
- **Maintainability** - SQL transformations are in version-controlled scripts, not embedded in code
- **Flexibility** - Modify transformations without changing Python code
- **Reliability** - Failed transforms don't corrupt raw data; re-run transforms independently

## Main Entry Point: importer.py

Orchestrates the full import workflow. Run as a CLI:

```bash
python -m tiger_utils.load_db.degauss.importer all <zip_dir> [--db DB_PATH] [--tmp TMP_DIR] [--recursive] [--state STATE_FIPS] [--type SHAPE_TYPE]
```

### Commands

- **`all`** - Full workflow: unzip → schema → load shapefiles → transform → index
- **`unzip`** - Extract TIGER/Line zip files to temp directory
- **`schema`** - Create empty database schema (place, edge, feature, etc.)
- **`shp`** - Load shapefiles into staging tables (tiger_edges, tiger_addr, tiger_featnames, etc.)
- **`indexes`** - Create indexes on final tables

### Options

- `--db` - Output SQLite database path (default: `geocoder.db`)
- `--tmp` - Temporary directory for unzipped files (default: `_tiger_tmp`)
- `--recursive` - Recursively search for zip files in subdirectories
- `--state` - State FIPS code to filter files (e.g., `13` for Georgia)
- `--type` - Shape type to filter files (e.g., `edges`, `addr`, `featnames`)
- `--year` - Year to filter files (e.g., `2025`)

## Supporting Modules

### shp_to_sqlite.py
Loads raw shapefiles into staging tables with geometry stored as WKB BLOB.

- Creates `tiger_*` tables matching the shapefile structure
- Stores all attributes as-is (no transformation)
- Geometry column named `the_geom`
- Tables are indexed for transform phase

### db_setup.py
Creates schema and executes SQL transformation scripts.

- Initializes empty final tables (place, edge, feature, feature_edge, range)
- Applies transformation logic via `convert.sql`
- Creates final indexes via `index.sql`

## SQL Scripts (sql/ directory)

- **create.sql** - Creates empty final schema tables
- **setup.sql** - Optional setup/initialization
- **convert.sql** - Transforms staged tiger_* tables into normalized final tables
- **index.sql** - Creates indexes on final tables

## Examples

### Full Import: State of Georgia (2025)

```bash
python -m tiger_utils.load_db.degauss.importer all ./tiger_zips \
  --db geocoder.db \
  --tmp _tiger_tmp \
  --state 13 \
  --year 2025 \
  --recursive
```

This will:
1. Unzip all TIGER/Line files for Georgia from `./tiger_zips`
2. Create empty schema in `geocoder.db`
3. Load shapefiles into `tiger_edges`, `tiger_addr`, `tiger_featnames` staging tables
4. Transform staged data into `edge`, `feature`, `feature_edge`, `range` tables
5. Create indexes for geocoding queries

### Load Shapefiles Only (skip transformation)

```bash
python -m tiger_utils.load_db.degauss.importer shp ./unzipped_shapefiles --db geocoder.db
```

This loads the unzipped shapefiles into staging tables but skips the transform/index steps.

### Debug Mode: Inspect Staging Tables

```bash
python -m tiger_utils.load_db.degauss.importer all ./tiger_zips --db geocoder.db --state 13
```

After this completes, you can inspect staging tables:

```bash
sqlite3 geocoder.db
sqlite> SELECT COUNT(*) FROM tiger_edges;
sqlite> SELECT COUNT(*) FROM tiger_addr;
sqlite> SELECT * FROM edge LIMIT 5;  -- After transform
```

## Architecture

```
Raw Shapefiles
    ↓
shp_to_sqlite.py (Stage)
    ↓
tiger_edges, tiger_addr, tiger_featnames (staging tables)
    ↓
convert.sql (Transform)
    ↓
edge, feature, feature_edge, range (final tables)
    ↓
index.sql (Index)
    ↓
Ready for geocoding
```

## Notes

- The staging tables (`tiger_*`) can be dropped after transformation if space is a concern
- Transformation is idempotent - re-run `convert.sql` if needed without data corruption
- For single-county imports, use `--state` and `--type` filters to reduce initial zip download size
