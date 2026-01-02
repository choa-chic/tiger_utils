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
        # Load spatial extension
        conn.execute("INSTALL spatial;")
        conn.execute("LOAD spatial;")
        
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
    Supports both regular files and /vsizip/ paths for direct ZIP reading.
    """
    conn = get_optimized_connection(db_path)
    
    try:
        # Infer schema if not provided (uses cache)
        if schema is None:
            columns, dtypes = get_or_infer_schema(shp_path, use_cache=True)
            # Extract table name from the file path (handles /vsizip/ paths)
            if '/vsizip/' in shp_path:
                # Extract from the inner file: /vsizip/path/to/file.zip/inner_file.shp
                inner_file = shp_path.split('/')[-1]
                table_name = Path(inner_file).stem
            else:
                table_name = Path(shp_path).stem
            schema = {'table_name': table_name, 'columns': columns, 'dtypes': dtypes}
        
        table_name = schema["table_name"]
        
        # Create table if not exists
        conn.execute(f"CREATE TABLE IF NOT EXISTS {table_name} AS SELECT * FROM st_read('{shp_path}') LIMIT 0;")
        
        # Insert data (st_read handles /vsizip/ paths automatically)
        conn.execute(f"INSERT INTO {table_name} SELECT * FROM st_read('{shp_path}');")
        
    finally:
        pass  # Don't close connection pool connections

def load_from_zip(
    zip_path: str,
    db_path: str,
    file_type: str = 'shp',
    use_connection_pool: bool = True,
) -> None:
    """
    Load SHP or DBF file directly from ZIP without extraction.
    Uses DuckDB's /vsizip/ virtual file system.
    
    Args:
        zip_path: Path to ZIP file
        db_path: DuckDB database path
        file_type: 'shp' or 'dbf' to determine which file to load
        use_connection_pool: Use thread-local connection pool
    """
    with zipfile.ZipFile(zip_path, 'r') as zf:
        # Find the target file
        target_ext = f".{file_type}"
        target_file = None
        for name in zf.namelist():
            if name.lower().endswith(target_ext):
                target_file = name
                break
        
        if not target_file:
            raise FileNotFoundError(f"No {file_type} file found in {zip_path}")
        
        # Construct /vsizip/ path
        vsi_path = f"/vsizip/{zip_path}/{target_file}"
        
        if file_type == 'shp':
            load_shp_to_duckdb(vsi_path, schema=None, db_path=db_path, use_connection_pool=use_connection_pool)
        elif file_type == 'dbf':
            load_dbf_to_duckdb(vsi_path, db_path, use_connection_pool=use_connection_pool)
        else:
            raise ValueError(f"Unsupported file_type: {file_type}")

def load_dbf_to_duckdb(dbf_path: str, db_path: str, table_name: str = None, 
                       use_connection_pool: bool = True) -> None:
    """
    Loads a .dbf file (non-spatial, e.g. addr, featnames) into DuckDB.
    Supports both regular files and /vsizip/ paths for direct ZIP reading.
    Uses DuckDB's st_read which handles DBF files natively.
    
    Args:
        dbf_path: Path to .dbf file (or /vsizip/ path)
        db_path: DuckDB database file
        table_name: Optional, defaults to stem of dbf_path (keeps county-specific names)
        use_connection_pool: If True, uses thread-local connection (optimization #3)
    """
    logger = get_logger()
    
    if table_name is None:
        # Extract table name from path (handles /vsizip/ paths)
        if '/vsizip/' in dbf_path:
            # Extract from the inner file: /vsizip/path/to/file.zip/inner_file.dbf
            inner_file = dbf_path.split('/')[-1]
            table_name = os.path.splitext(inner_file)[0]
        else:
            table_name = os.path.splitext(os.path.basename(dbf_path))[0]
    
    conn = get_optimized_connection(db_path)
    
    try:
        # Check if table exists
        exists = conn.execute(
            "SELECT 1 FROM information_schema.tables WHERE table_name = ?",
            [table_name]
        ).fetchone()
        
        if exists:
            # Table exists - check schema compatibility
            existing_cols = set([
                row[0].lower() for row in conn.execute(
                    "SELECT column_name FROM information_schema.columns WHERE table_name = ?",
                    [table_name]
                ).fetchall()
            ])
            
            # Get columns from the DBF file
            result = conn.execute(f"SELECT * FROM st_read('{dbf_path}') LIMIT 0")
            new_cols = set([desc[0].lower() for desc in result.description])
            
            if existing_cols != new_cols:
                # Schema mismatch
                logger.warning(
                    f"Skipping {dbf_path}: table {table_name} exists with different schema "
                    f"(expected {sorted(new_cols)}, found {sorted(existing_cols)})"
                )
                return
        else:
            # Create table
            logger.debug(f"Creating table {table_name}")
            conn.execute(f"CREATE TABLE {table_name} AS SELECT * FROM st_read('{dbf_path}') LIMIT 0;")
        
        # Insert data
        logger.debug(f"Inserting data into {table_name}")
        conn.execute(f"INSERT INTO {table_name} SELECT * FROM st_read('{dbf_path}');")
        
    except Exception as e:
        logger.error(f"Error loading {dbf_path}: {e}")
        raise

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