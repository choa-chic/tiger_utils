"""
loader.py
Handles loading SHP/DBF data into DuckDB with performance optimizations.
"""
import duckdb
import pandas as pd
import os
import threading
import multiprocessing
import zipfile
from pathlib import Path
from typing import List, Dict, Any, Optional
from tiger_utils.utils.logger import get_logger
from .schema_cache import get_or_infer_schema

# Thread-local storage for connection pooling (optimization #3)
_thread_local = threading.local()

def get_optimized_connection(db_path: str) -> duckdb.DuckDBPyConnection:
    """Get or create an optimized DuckDB connection for the current thread (optimization #2 & #3)."""
    if not hasattr(_thread_local, 'connection') or _thread_local.db_path != db_path:
        if hasattr(_thread_local, 'connection'):
            _thread_local.connection.close()
        
        conn = duckdb.connect(db_path)
        # Optimization #2: DuckDB configuration tuning
        conn.execute(f"SET memory_limit='8GB';")  # Adjust based on available RAM
        conn.execute(f"SET threads={multiprocessing.cpu_count()};")  # Use all CPU cores
        conn.execute("SET checkpoint_threshold='1GB';")  # Reduce checkpoint frequency during bulk loads
        
        _thread_local.connection = conn
        _thread_local.db_path = db_path
    
    return _thread_local.connection

def load_shp_to_duckdb(
    shp_path: str,
    schema: dict,  # Now optional - can be None
    db_path: str,
    use_connection_pool: bool = True,
) -> None:
    """
    Load a single SHP file into DuckDB with automatic schema inference.
    """
    conn = get_connection(db_path) if use_connection_pool else get_optimized_connection(db_path)
    
    try:
        # Infer schema if not provided (uses cache)
        if schema is None:
            columns, dtypes = get_or_infer_schema(shp_path, use_cache=True)
            schema = {'table_name': Path(shp_path).stem, 'columns': columns, 'dtypes': dtypes}
        
        table_name = schema["table_name"]
        
        # Create table if not exists
        conn.execute(f"CREATE TABLE IF NOT EXISTS {table_name} AS SELECT * FROM st_read('{shp_path}') LIMIT 0;")
        
        # Insert data
        conn.execute(f"INSERT INTO {table_name} SELECT * FROM st_read('{shp_path}');")
        
    finally:
        if not use_connection_pool:
            conn.close()

def load_dbf_to_duckdb(dbf_path: str, db_path: str, table_name: str = None, 
                       use_connection_pool: bool = True) -> None:
    """
    Loads a .dbf file (non-spatial, e.g. addr, featnames) into DuckDB with optimizations.
    Handles schema differences by checking compatibility or recreating table.
    - dbf_path: path to .dbf file
    - db_path: DuckDB database file
    - table_name: optional, defaults to stem of dbf_path (keeps county-specific names)
    - use_connection_pool: if True, uses thread-local connection (optimization #3)
    """
    logger = get_logger()
    if table_name is None:
        # Keep the full filename as table name (includes county code) to avoid conflicts
        table_name = os.path.splitext(os.path.basename(dbf_path))[0]
    
    try:
        # Read DBF using pandas (requires 'dbfread')
        try:
            import dbfread
            # Try multiple encodings to handle non-ASCII characters
            encodings = ['utf-8', 'latin-1', 'cp1252', 'iso-8859-1']
            records = None
            last_error = None
            
            for encoding in encodings:
                try:
                    records = list(dbfread.DBF(dbf_path, encoding=encoding, load=True))
                    break  # Success, use this encoding
                except (UnicodeDecodeError, UnicodeError) as e:
                    last_error = e
                    continue
            
            if records is None:
                # All encodings failed, try with ignore errors
                logger.warning(f"Encoding issues with {dbf_path}, using latin-1 with error handling")
                records = list(dbfread.DBF(dbf_path, encoding='latin-1', char_decode_errors='ignore', load=True))
            
            if not records:
                logger.warning(f"No records found in {dbf_path}")
                return
            df = pd.DataFrame(records)
        except ImportError:
            logger.error("dbfread is not installed. Cannot import DBF.")
            return
        
        # Use connection pool or create new connection
        if use_connection_pool:
            con = get_optimized_connection(db_path)
            should_close = False
        else:
            con = duckdb.connect(db_path)
            should_close = True
        
        try:
            # Check if table exists and validate schema BEFORE starting transaction
            table_exists = con.execute(
                "SELECT 1 FROM information_schema.tables WHERE table_name = ?",
                [table_name]
            ).fetchone()
            
            if table_exists:
                # Get existing table columns
                existing_cols = set([
                    row[0].lower() for row in con.execute(
                        "SELECT column_name FROM information_schema.columns WHERE table_name = ?",
                        [table_name]
                    ).fetchall()
                ])
                new_cols = set([col.lower() for col in df.columns])
                
                # If schemas don't match, this table already exists with different structure
                if existing_cols != new_cols:
                    logger.warning(
                        f"Skipping {dbf_path}: table {table_name} exists with different schema "
                        f"(existing={len(existing_cols)} cols, new={len(new_cols)} cols)"
                    )
                    return
            
            # Schema is compatible or table doesn't exist - proceed with import
            con.execute("BEGIN TRANSACTION;")
            
            try:
                # Register the dataframe with DuckDB
                con.register('df_temp', df)
                
                if not table_exists:
                    # Create new table from dataframe
                    con.execute(f"CREATE TABLE {table_name} AS SELECT * FROM df_temp;")
                else:
                    # Insert into existing compatible table
                    con.execute(f"INSERT INTO {table_name} SELECT * FROM df_temp;")
                
                con.execute("COMMIT;")
                logger.debug(f"Imported DBF {os.path.basename(dbf_path)} into {table_name} ({len(df)} rows)")
            except Exception as e:
                con.execute("ROLLBACK;")
                logger.error(f"Failed to import DBF {dbf_path}: {e}")
                raise
        finally:
            if should_close:
                con.close()
    except Exception as e:
        logger.error(f"Failed to process DBF {dbf_path}: {e}")

def load_shp_from_zip_directly(zip_path: str, shp_name: str, db_path: str, table_name: str = None) -> None:
    """
    Load SHP directly from ZIP without extraction (optimization #4).
    Uses GDAL's /vsizip/ virtual file system.
    
    Args:
        zip_path: Path to ZIP file
        shp_name: Name of .shp file within ZIP (e.g., 'tl_2021_13001_edges.shp')
        db_path: DuckDB database file
        table_name: Optional table name, defaults to stem of shp_name
    """
    logger = get_logger()
    if table_name is None:
        table_name = os.path.splitext(shp_name)[0]
    
    # Construct /vsizip/ path for GDAL
    vsi_path = f"/vsizip/{zip_path}/{shp_name}"
    logger.debug(f"Loading {shp_name} from ZIP {zip_path} using /vsizip/")
    
    # Use regular load function with vsizip path
    load_shp_to_duckdb(vsi_path, [], db_path, table_name)

def batch_load_dbfs(dbf_paths: List[str], db_path: str) -> None:
    """
    Load multiple DBF files in optimized batches (optimization #5).
    
    Args:
        dbf_paths: List of DBF file paths
        db_path: DuckDB database file
    """
    logger = get_logger()
    con = get_optimized_connection(db_path)
    
    logger.info(f"Batch loading {len(dbf_paths)} DBF files...")
    con.execute("BEGIN TRANSACTION;")
    
    try:
        for dbf_path in dbf_paths:
            table_name = os.path.splitext(os.path.basename(dbf_path))[0]
            try:
                import dbfread
                records = list(dbfread.DBF(dbf_path, load=True))
                if not records:
                    logger.warning(f"No records in {dbf_path}")
                    continue
                df = pd.DataFrame(records)
                
                con.execute(f"CREATE TABLE IF NOT EXISTS {table_name} AS SELECT * FROM df LIMIT 0;")
                con.register('df', df)
                con.execute(f"INSERT INTO {table_name} SELECT * FROM df;")
                logger.debug(f"Batch loaded {dbf_path} ({len(df)} rows)")
            except Exception as e:
                logger.error(f"Failed to batch load {dbf_path}: {e}")
                continue
        
        con.execute("COMMIT;")
        logger.info(f"Batch load complete for {len(dbf_paths)} DBF files")
    except Exception as e:
        con.execute("ROLLBACK;")
        logger.error(f"Batch load failed: {e}")
        raise