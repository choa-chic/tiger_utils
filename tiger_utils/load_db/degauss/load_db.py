"""
load_db.py - High-level API for loading TIGER/Line data to SQLite.

Replicates degauss-org/geocoder functionality in pure Python:
- shp_to_sqlite: Convert ESRI shapefiles to SQLite with WKB geometry
- tiger_importer: Orchestrate county-by-county import with ETL transforms
- db_setup: Create schema and indexes matching degauss conventions

Key improvements over original C/bash implementation:
- Pure Python (no C dependencies or bash scripting required)
- Cross-platform (Windows, macOS, Linux)
- Uses fiona for robust shapefile reading
- WKB geometry format (more portable than PostGIS)
- Batch inserts for performance
- Comprehensive logging and error handling

Usage:
    # Import all counties in a directory
    from tiger_utils.load_db.degauss import import_tiger_data
    import_tiger_data('/data/geocoder.db', '/data/tiger_files/')

    # Import specific counties
    import_tiger_data('/data/geocoder.db', '/data/tiger_files/', 
                     counties=['06001', '06007'])

    # CLI usage
    python -m tiger_utils.load_db.degauss.importer_cli /data/geocoder.db /data/tiger_files/
"""

from pathlib import Path

from .shp_to_sqlite import ShapefileToSQLiteConverter, convert_shapefile
from .tiger_importer import TigerImporter, import_tiger_data
from .db_setup import create_schema, create_indexes

__all__ = [
    "ShapefileToSQLiteConverter",
    "convert_shapefile",
    "TigerImporter",
    "import_tiger_data",
    "create_schema",
    "create_indexes",
]
