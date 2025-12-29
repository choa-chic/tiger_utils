"""
shp_to_sqlite.py
Loads shapefiles into SQLite staging tables using the DeGAUSS ETL pattern.

This module implements the first phase of the DeGAUSS import workflow:
1. Load raw shapefile data into temporary staging tables (tiger_*)
2. Subsequent SQL scripts (convert.sql, etc.) transform staging tables to final schema

See convert.sql for the transformation logic that normalizes raw TIGER/Line data
and creates the final edge, feature, feature_edge, and range tables.
"""

import sqlite3
from pathlib import Path
import fiona
from shapely.geometry import shape


def shp_to_sqlite(shp_path: str, db_path: str, table_name: str) -> None:
    """
    Loads a shapefile or DBF file into a staging table with raw data.
    
    This follows the DeGAUSS pattern: stage raw data, then transform via SQL.
    
    Args:
        shp_path: Path to the shapefile or DBF file to load
        db_path: Path to the SQLite database
        table_name: Name for the staging table (raw data, unchanged)
    
    Notes:
        - Geometry is stored as WKB (binary) in a 'the_geom' column
        - All other columns are loaded as-is from the shapefile
        - Table is created if it does not exist
        - The table name should include the 'tiger_' prefix (e.g., tiger_edges, tiger_addr)
    """
    with fiona.open(shp_path) as src:
        fields = src.schema["properties"]
        has_geometry = src.schema.get("geometry") is not None and src.schema.get("geometry") != "None"
        
        # Map OGR types to SQLite types
        def ogr_to_sqlite_type(ogr_type):
            """Convert Fiona/OGR type names to SQLite type names."""
            if ogr_type.startswith("int"):
                return "INTEGER"
            elif ogr_type.startswith(("float", "double", "real")):
                return "REAL"
            else:
                return "TEXT"
        
        # Build column definitions from shapefile schema
        columns = list(fields.keys())
        col_defs = ", ".join(
            [f'"{col}" {ogr_to_sqlite_type(fields[col])}' for col in columns]
        )
        
        # Add geometry column for spatial data
        if has_geometry:
            col_defs += ', the_geom BLOB'
        
        # Connect to database and create staging table
        conn = sqlite3.connect(db_path)
        cur = conn.cursor()
        
        # Drop table if it exists to ensure clean load (staging tables are recreated each run)
        cur.execute(f'DROP TABLE IF EXISTS "{table_name}"')
        
        create_sql = f'CREATE TABLE "{table_name}" ({col_defs});'
        cur.execute(create_sql)
        
        # Build and execute INSERT statements for all features
        if has_geometry:
            placeholders = ", ".join(["?"] * (len(columns) + 1))
            insert_sql = f'INSERT INTO "{table_name}" ({", ".join(columns)}, the_geom) VALUES ({placeholders})'
        else:
            placeholders = ", ".join(["?"] * len(columns))
            insert_sql = f'INSERT INTO "{table_name}" ({", ".join(columns)}) VALUES ({placeholders})'
        
        row_count = 0
        for feat in src:
            values = [feat["properties"].get(col, None) for col in columns]
            
            if has_geometry:
                if feat["geometry"]:
                    geom = shape(feat["geometry"])
                    wkb = geom.wkb
                else:
                    wkb = None
                values.append(wkb)
            
            cur.execute(insert_sql, values)
            row_count += 1
        
        conn.commit()
        conn.close()
        
        geom_type = "spatial" if has_geometry else "non-spatial"
        print(
            f"Loaded {row_count} records from {Path(shp_path).name} "
            f"({geom_type}) into {table_name}"
        )
