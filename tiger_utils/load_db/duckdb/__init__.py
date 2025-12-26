"""
Modular import for Census ZIP/SHP to DuckDB.
"""
# Expose main API for this module
from .schema_mapper import get_duckdb_schema
from .loader import load_shp_to_duckdb
