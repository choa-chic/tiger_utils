"""
degauss - Python implementation of degauss-org/geocoder for TIGER/Line to SQLite import.

A modular, cross-platform replacement for the C+Bash degauss geocoder that loads
TIGER/Line shapefiles into SQLite with proper schema and indexes.

Modules:
- shp_to_sqlite: Convert ESRI shapefiles to SQLite with WKB geometry
- tiger_importer: Orchestrate TIGER/Line import (replicates bash tiger_import)
- db_setup: Create schema and indexes matching degauss conventions
- importer_cli: Command-line interface

CLI Usage:
    python -m tiger_utils.load_db.degauss.importer_cli /path/to/geocoder.db /path/to/tiger/

Library Usage:
    from tiger_utils.load_db.degauss import import_tiger_data
    import_tiger_data('/data/geocoder.db', '/data/tiger/', counties=['06001'])

See load_db.py for high-level API.
"""

from .load_db import (
    ShapefileToSQLiteConverter,
    convert_shapefile,
    TigerImporter,
    import_tiger_data,
    create_schema,
    create_indexes,
)

__all__ = [
    "ShapefileToSQLiteConverter",
    "convert_shapefile",
    "TigerImporter",
    "import_tiger_data",
    "create_schema",
    "create_indexes",
]

from . import db_setup
from . import shp_to_sqlite
# Do NOT import unzipper here to avoid circular import (imported at higher level)