"""
loader.py
Handles loading SHP/DBF data into DuckDB.
"""
import duckdb
import os
from typing import List, Dict, Any
from tiger_utils.utils.logger import get_logger

def load_shp_to_duckdb(shp_path: str, schema: List[Dict[str, Any]], db_path: str, table_name: str = None) -> None:
    """
    Loads a .shp file into DuckDB. Lets DuckDB infer schema from st_read().
    - shp_path: path to .shp file
    - schema: (ignored, kept for API compatibility)
    - db_path: DuckDB database file
    - table_name: optional, defaults to stem of shp_path
    """
    logger = get_logger()
    if table_name is None:
        table_name = os.path.splitext(os.path.basename(shp_path))[0]
    logger.info(f"Loading {shp_path} into DuckDB table {table_name}")
    con = duckdb.connect(db_path)
    con.execute("INSTALL spatial;")
    con.execute("LOAD spatial;")
    # Check if table exists
    table_exists = False
    try:
        res = con.execute(f"SELECT 1 FROM information_schema.tables WHERE table_name = '{table_name}'").fetchone()
        if res:
            table_exists = True
    except Exception:
        table_exists = False
    try:
        if not table_exists:
            # Create table from SHP
            create_sql = f"CREATE TABLE {table_name} AS SELECT * FROM st_read('{shp_path}');"
            logger.info(f"Creating table with: {create_sql}")
            con.execute(create_sql)
            logger.info(f"Created and imported {shp_path} into {table_name}")
        else:
            # Insert into existing table
            import_sql = f"INSERT INTO {table_name} SELECT * FROM st_read('{shp_path}');"
            logger.info(f"Importing with: {import_sql}")
            con.execute(import_sql)
            logger.info(f"Imported {shp_path} into {table_name}")
    except Exception as e:
        logger.error(f"Failed to import {shp_path}: {e}")
    finally:
        con.close()