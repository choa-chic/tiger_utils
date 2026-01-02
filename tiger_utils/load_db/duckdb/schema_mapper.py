"""
schema_mapper.py - DEPRECATED

This module is deprecated. Do NOT use hardcoded schemas.

Schemas MUST be inferred directly from actual SHP and DBF files using:
  - infer_schema_from_shp() in schema_cache.py
  - infer_schema_from_dbf() in schema_cache.py

These functions use DuckDB's spatial extension to read the actual file structure,
not predefined schemas that may not match the data.
"""
from typing import Dict, Any, List

def get_duckdb_schema(shp_path: str) -> List[Dict[str, Any]]:
    """
    DEPRECATED: Do not use this function.
    
    This function returns a hardcoded schema that does NOT match TIGER/Line data.
    Use schema_cache.get_or_infer_schema() instead to infer from actual files.
    """
    raise NotImplementedError(
        "get_duckdb_schema() is deprecated. "
        "Use schema_cache.get_or_infer_schema() or schema_cache.infer_schema_from_shp() "
        "to infer schemas directly from actual SHP/DBF files."
    )

def map_field_type(dbf_type: str) -> str:
    """
    DEPRECATED: Do not use this function.
    
    Field type mapping should be done by DuckDB's spatial extension, not manually.
    """
    raise NotImplementedError(
        "map_field_type() is deprecated. "
        "Use DuckDB's spatial extension to handle field type mapping automatically."
    )