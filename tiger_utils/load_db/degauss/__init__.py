
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
