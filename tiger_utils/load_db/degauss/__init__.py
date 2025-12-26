"""
degauss: TIGER/Line to SQLite/SpatiaLite loader

This package provides a modular, scriptable, and CLI-driven workflow for building a spatially-enabled SQLite database from TIGER/Line shapefiles, inspired by the DeGAUSS project.

Main entry point: importer.py (see CLI usage)

Modules:
	- importer: Orchestrates unzip, schema, index, and shapefile import
	- db_setup: Schema and index creation
	- unzipper: Unzips TIGER/Line zip files
	- shp_to_sqlite: Loads shapefiles into SpatiaLite-enabled SQLite tables

For CLI usage, run:
	python -m tiger_utils.load_db.degauss.importer --help
"""

# Avoid star imports to prevent circular import issues.
# Only import modules, not symbols, to avoid triggering circular imports.
from . import importer
from . import db_setup
from . import shp_to_sqlite
# Do NOT import unzipper here to avoid circular import (imported at higher level)