"""
schema_mapper.py
Maps Census SHP/DBF fields to DuckDB schema.
"""
from typing import Dict, Any, List

def get_duckdb_schema(shp_path: str) -> List[Dict[str, Any]]:
    """
    Given a .shp file path, return a list of dicts describing DuckDB schema.
    Each dict: {"name": str, "type": str}
    (Stub: In real use, parse .dbf or use fiona/pyshp to inspect fields)
    """
    # Example: return a fixed schema for demonstration
    # In production, use pyshp or fiona to inspect the .dbf file
    return [
        {"name": "gid", "type": "INTEGER"},
        {"name": "fullname", "type": "VARCHAR"},
        {"name": "statefp", "type": "VARCHAR"},
        {"name": "geometry", "type": "GEOMETRY"},
    ]

def map_field_type(dbf_type: str) -> str:
    """
    Map DBF field type to DuckDB SQL type (stub).
    """
    mapping = {
        "C": "VARCHAR",
        "N": "DOUBLE",
        "F": "DOUBLE",
        "I": "INTEGER",
        "D": "DATE",
        "L": "BOOLEAN",
    }
    return mapping.get(dbf_type, "VARCHAR")