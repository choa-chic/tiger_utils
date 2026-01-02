"""
schema_inference.py - Infer SQLite table schemas from TIGER/Line DBF and SHP files.

This module reads DBF/SHP file schemas using fiona and generates CREATE TABLE
statements dynamically. This eliminates hardcoded schemas and ensures tables
match the actual TIGER/Line data structure.
"""

import sqlite3
from pathlib import Path
from typing import Dict, List, Optional, Tuple

try:
    import fiona
except ImportError:
    raise ImportError("schema_inference requires: pip install fiona")

from tiger_utils.utils.logger import get_logger

logger = get_logger()


# Type mapping from fiona to SQLite
FIONA_TO_SQLITE_TYPE = {
    "int": "INTEGER",
    "int32": "INTEGER",
    "int64": "INTEGER",
    "float": "REAL",
    "double": "REAL",
    "str": "TEXT",
    "bool": "INTEGER",  # SQLite uses INTEGER for booleans
    "date": "TEXT",
    "datetime": "TEXT",
    "time": "TEXT",
}


def infer_schema_from_file(
    file_path: Path,
    include_geometry: bool = True,
    geometry_column: str = "geometry",
) -> Tuple[List[Tuple[str, str]], bool]:
    """
    Infer table schema from a DBF or SHP file.

    Args:
        file_path: Path to .dbf or .shp file
        include_geometry: If True and file is .shp, include geometry column
        geometry_column: Name for geometry column (default: "geometry")

    Returns:
        Tuple of (columns, has_geometry) where:
        - columns: List of (column_name, sql_type) tuples
        - has_geometry: Boolean indicating if geometry column is included
    """
    columns = []
    has_geometry = False

    with fiona.open(str(file_path)) as src:
        # Check if file has geometry
        if src.schema.get("geometry") and include_geometry:
            has_geometry = True
            columns.append((geometry_column, "BLOB"))

        # Add attribute columns
        for prop_name, prop_type in src.schema["properties"].items():
            # Map fiona type to SQLite type
            sql_type = FIONA_TO_SQLITE_TYPE.get(
                prop_type.split(":")[0].lower(), "TEXT"
            )
            # Clean column name (lowercase, remove special chars)
            clean_name = prop_name.lower().strip()
            columns.append((clean_name, sql_type))

    return columns, has_geometry


def create_table_from_schema(
    db_path: str,
    table_name: str,
    columns: List[Tuple[str, str]],
    primary_key: Optional[str] = None,
    if_not_exists: bool = True,
) -> None:
    """
    Create a SQLite table from inferred schema.

    Args:
        db_path: Path to SQLite database
        table_name: Name of table to create
        columns: List of (column_name, sql_type) tuples
        primary_key: Optional column name to use as primary key
        if_not_exists: If True, use CREATE TABLE IF NOT EXISTS
    """
    col_defs = []
    for col_name, sql_type in columns:
        # Escape column names
        safe_name = f'"{col_name}"' if " " in col_name else col_name
        
        # Add PRIMARY KEY constraint if specified
        if primary_key and col_name == primary_key:
            col_defs.append(f"{safe_name} {sql_type} PRIMARY KEY")
        else:
            col_defs.append(f"{safe_name} {sql_type}")

    col_def_str = ", ".join(col_defs)
    
    if_not_exists_clause = "IF NOT EXISTS " if if_not_exists else ""
    create_stmt = f"CREATE TABLE {if_not_exists_clause}{table_name} ({col_def_str});"

    conn = sqlite3.connect(db_path)
    try:
        conn.execute(create_stmt)
        conn.commit()
        logger.info(f"Created table {table_name} with {len(columns)} columns")
    finally:
        conn.close()


def create_tiger_tables_from_files(
    db_path: str,
    edges_shp: Optional[Path] = None,
    featnames_dbf: Optional[Path] = None,
    addr_dbf: Optional[Path] = None,
) -> Dict[str, List[Tuple[str, str]]]:
    """
    Create TIGER/Line tables dynamically from sample files.

    This function reads actual TIGER/Line files to infer schemas and creates
    tables accordingly. This ensures tables match the data structure.

    Args:
        db_path: Path to SQLite database
        edges_shp: Optional sample EDGES.shp file for schema inference
        featnames_dbf: Optional sample FEATNAMES.dbf for schema inference
        addr_dbf: Optional sample ADDR.dbf for schema inference

    Returns:
        Dictionary mapping table names to their column schemas
    """
    schemas = {}

    # Create tl_edges table from EDGES.shp
    if edges_shp and edges_shp.exists():
        logger.info(f"Inferring schema from {edges_shp.name}")
        columns, has_geom = infer_schema_from_file(edges_shp, include_geometry=True)
        
        # TLID should be primary key
        create_table_from_schema(
            db_path, "tl_edges", columns, primary_key="tlid", if_not_exists=True
        )
        schemas["tl_edges"] = columns

    # Create tl_featnames table from FEATNAMES.dbf
    if featnames_dbf and featnames_dbf.exists():
        logger.info(f"Inferring schema from {featnames_dbf.name}")
        columns, _ = infer_schema_from_file(featnames_dbf, include_geometry=False)
        
        # No primary key for featnames (multiple records per TLID)
        create_table_from_schema(
            db_path, "tl_featnames", columns, primary_key=None, if_not_exists=True
        )
        schemas["tl_featnames"] = columns

    # Create tl_addr table from ADDR.dbf
    if addr_dbf and addr_dbf.exists():
        logger.info(f"Inferring schema from {addr_dbf.name}")
        columns, _ = infer_schema_from_file(addr_dbf, include_geometry=False)
        
        # No primary key for addr (multiple records per TLID)
        create_table_from_schema(
            db_path, "tl_addr", columns, primary_key=None, if_not_exists=True
        )
        schemas["tl_addr"] = columns

    return schemas


def get_table_columns(db_path: str, table_name: str) -> List[str]:
    """
    Get list of column names for a table.

    Args:
        db_path: Path to SQLite database
        table_name: Name of table

    Returns:
        List of column names
    """
    conn = sqlite3.connect(db_path)
    try:
        cursor = conn.execute(f"PRAGMA table_info({table_name})")
        columns = [row[1] for row in cursor.fetchall()]
        return columns
    finally:
        conn.close()
