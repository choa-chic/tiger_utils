
"""
loader.py
Handles loading SHP/DBF data into DuckDB.
"""
import duckdb
import pandas as pd
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
    try:
        con.execute("INSTALL spatial;")
        con.execute("LOAD spatial;")
        
        # Check if table exists (using parameterized query)
        table_exists = False
        try:
            res = con.execute(
                "SELECT 1 FROM information_schema.tables WHERE table_name = ?",
                [table_name]
            ).fetchone()
            if res:
                table_exists = True
        except Exception:
            table_exists = False
        
        # Begin transaction for better performance
        con.execute("BEGIN TRANSACTION;")
        
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
            
            # Commit transaction
            con.execute("COMMIT;")
        except Exception as e:
            con.execute("ROLLBACK;")
            logger.error(f"Failed to import {shp_path}: {e}")
            raise
    finally:
        con.close()

def load_dbf_to_duckdb(dbf_path: str, db_path: str, table_name: str = None) -> None:
    """
    Loads a .dbf file (non-spatial, e.g. addr, featnames) into DuckDB.
    - dbf_path: path to .dbf file
    - db_path: DuckDB database file
    - table_name: optional, defaults to stem of dbf_path
    """
    logger = get_logger()
    if table_name is None:
        table_name = os.path.splitext(os.path.basename(dbf_path))[0]
    logger.info(f"Loading DBF {dbf_path} into DuckDB table {table_name}")
    
    con = None
    try:
        # Read DBF using pandas (requires 'dbfread')
        try:
            import dbfread
            records = list(dbfread.DBF(dbf_path, load=True))
            if not records:
                logger.warning(f"No records found in {dbf_path}")
                return
            df = pd.DataFrame(records)
        except ImportError:
            logger.error("dbfread is not installed. Cannot import DBF.")
            return
        
        # Connect to DuckDB and write table with transaction
        con = duckdb.connect(db_path)
        con.execute("BEGIN TRANSACTION;")
        
        try:
            con.execute(f"CREATE TABLE IF NOT EXISTS {table_name} AS SELECT * FROM df LIMIT 0;")
            con.register('df', df)
            con.execute(f"INSERT INTO {table_name} SELECT * FROM df;")
            con.execute("COMMIT;")
            logger.info(f"Imported DBF {dbf_path} into {table_name} ({len(df)} rows)")
        except Exception as e:
            con.execute("ROLLBACK;")
            logger.error(f"Failed to import DBF {dbf_path}: {e}")
            raise
    except Exception as e:
        logger.error(f"Failed to process DBF {dbf_path}: {e}")
    finally:
        if con:
            con.close()