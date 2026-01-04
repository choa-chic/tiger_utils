import duckdb
import sys

from pathlib import Path

from tiger_utils.config import db_duck_dg_source, db_sqlite_dg_target
from tiger_utils.utils.logger import get_logger

logger = get_logger()

logger.info(f"DeGAUSS source DuckDB path: {db_duck_dg_source}")
logger.info(f"DeGAUSS target SQLite path: {db_sqlite_dg_target}")

def target_db_exists(target_db: Path):
    # Check if target database already exists
    if target_db.exists():
        logger.warning(f"Target database already exists: {target_db}")
        response = input("Do you want to overwrite it? ([y]es/No): ").strip().lower()
        
        if response not in ['yes', 'y']:
            logger.info("Export cancelled by user.")
            sys.exit(0)
        else:
            logger.info("Overwriting existing database...")
            target_db.unlink()

def sqlite_clean(db_path: Path, analyze: bool = True, reindex: bool = False, vacuum: bool = True, optimize: bool = True):
    """Perform comprehensive optimization on the SQLite database after export."""
    import sqlite3

    logger.info("Starting SQLite database optimization...")
    conn = sqlite3.connect(db_path.as_posix())
    cursor = conn.cursor()

    if analyze:
        # ANALYZE - Update query planner statistics for better query optimization
        logger.info("  Running ANALYZE to update query statistics...")
        cursor.execute("ANALYZE;")
    
    if reindex:
        # REINDEX - Rebuild all indexes to fix fragmentation
        logger.info("  Running REINDEX to rebuild indexes...")
        cursor.execute("REINDEX;")
    
    if vacuum:
        # VACUUM - Rebuild database file to reclaim space and defragment
        logger.info("  Running VACUUM to reclaim space and defragment...")
        cursor.execute("VACUUM;")
    
    if optimize:
        # PRAGMA optimize - Comprehensive auto-optimization (SQLite 3.18.0+)
        logger.info("  Running PRAGMA optimize...")
        cursor.execute("PRAGMA optimize;")
    
    # Optional: Set recommended pragmas for performance
    logger.info("  Setting performance pragmas...")
    cursor.execute("PRAGMA journal_mode=WAL;")  # Write-Ahead Logging for better concurrency
    cursor.execute("PRAGMA synchronous=NORMAL;")  # Good balance of safety and speed
    cursor.execute("PRAGMA cache_size=-64000;")  # 64MB cache (negative = KB)
    cursor.execute("PRAGMA temp_store=MEMORY;")  # Store temp tables in memory
    
    conn.commit()
    
    # Get final database size
    db_size_mb = db_path.stat().st_size / (1024 * 1024)
    logger.info(f"  Final database size: {db_size_mb:.2f} MB")
    
    conn.close()
    logger.info("SQLite database optimization complete.")

def convert_duckdb_to_sqlite(db_source: Path = db_duck_dg_source, db_target: Path = db_sqlite_dg_target):
    """
    Convert DeGAUSS DuckDB database to SQLite format.
    """

    target_db_exists(db_target)
    
    with duckdb.connect(db_source.as_posix()) as conn:
        logger.info("Connected to DeGAUSS DuckDB database.")
        conn.execute("INSTALL sqlite;")
        conn.execute("LOAD sqlite;")

        # Get all tables from information_schema for better metadata
        tables = conn.execute("""
            SELECT table_schema, table_name 
            FROM information_schema.tables 
            WHERE table_type='BASE TABLE' AND table_schema NOT IN ('information_schema', 'pg_catalog')
            ORDER BY table_schema, table_name
        """).fetchall()
        
        logger.info(f"Found {len(tables)} tables in DeGAUSS DuckDB.")
        for schema_name, table_name in tables:
            logger.info(f" - {schema_name}.{table_name}")

        # Get views if any
        views = conn.execute("""
            SELECT table_schema, table_name 
            FROM information_schema.tables 
            WHERE table_type='VIEW' AND table_schema NOT IN ('information_schema', 'pg_catalog')
            ORDER BY table_schema, table_name
        """).fetchall()
        
        if views:
            logger.info(f"Found {len(views)} views (will be skipped for SQLite export):")
            for schema_name, view_name in views:
                logger.info(f" - {schema_name}.{view_name}")

        # Export to SQLite using ATTACH from within DuckDB
        logger.info("Starting export to SQLite...")
        conn.execute(f"ATTACH '{db_target.as_posix()}' AS sqlite_db (TYPE sqlite);")
        
        # Export tables with schema preservation
        for schema_name, table_name in tables:
            logger.info(f"Exporting table: {schema_name}.{table_name}")
            
            try:
                # Get column information for logging
                columns = conn.execute(f"""
                    SELECT column_name, data_type 
                    FROM information_schema.columns 
                    WHERE table_name = '{table_name}' AND table_schema = '{schema_name}'
                    ORDER BY ordinal_position
                """).fetchall()
                logger.debug(f"  Table {table_name} has {len(columns)} columns")
                
                # Create table with data (use fully qualified source table name)
                conn.execute(f"CREATE TABLE sqlite_db.{table_name} AS SELECT * FROM {schema_name}.{table_name};")
                
                # Get row count for verification
                row_count = conn.execute(f"SELECT COUNT(*) FROM sqlite_db.{table_name}").fetchone()[0]
                logger.info(f"  Exported {row_count:,} rows")
                
                # Get and attempt to recreate indexes
                indexes = conn.execute(f"""
                    SELECT index_name, is_unique, sql
                    FROM duckdb_indexes() 
                    WHERE schema_name = '{schema_name}' AND table_name = '{table_name}'
                """).fetchall()
                
                if indexes:
                    logger.info(f"  Found {len(indexes)} indexes for {table_name}")
                    for idx_name, is_unique, idx_sql in indexes:
                        try:
                            # Adapt DuckDB index SQL to SQLite syntax
                            # SQLite syntax: CREATE [UNIQUE] INDEX index_name ON table_name (columns)
                            if idx_sql:
                                # Remove schema prefix from index name and table reference
                                sqlite_idx_sql = idx_sql.replace(f'{schema_name}.{table_name}', table_name)
                                sqlite_idx_sql = sqlite_idx_sql.replace(f'"{schema_name}"."{table_name}"', table_name)
                                
                                # Create index directly on sqlite_db schema without schema prefix in index name
                                # Extract the column list from the original SQL
                                if 'ON' in sqlite_idx_sql and '(' in sqlite_idx_sql:
                                    # Rebuild as: CREATE [UNIQUE] INDEX idx_name ON table_name (columns)
                                    # SQLite doesn't support schema prefix in the ON clause
                                    unique_clause = "UNIQUE " if is_unique else ""
                                    col_start = sqlite_idx_sql.index('(')
                                    columns_part = sqlite_idx_sql[col_start:]
                                    sqlite_idx_sql = f"CREATE {unique_clause}INDEX {idx_name} ON {table_name} {columns_part}"
                                    
                                    conn.execute(sqlite_idx_sql)
                                    logger.info(f"    Created index: {idx_name}")
                                else:
                                    logger.warning(f"    Could not parse index SQL for {idx_name}")
                        except Exception as idx_err:
                            # Skip "already exists" errors on re-runs
                            if "already exists" not in str(idx_err):
                                logger.warning(f"    Could not create index {idx_name}: {idx_err}")
            except Exception as e:
                logger.error(f"  Error exporting table {table_name}: {e}")
                raise

        # Summary
        logger.info("=" * 60)
        logger.info("Export Summary:")
        for schema_name, table_name in tables:
            row_count = conn.execute(f"SELECT COUNT(*) FROM sqlite_db.{table_name}").fetchone()[0]
            logger.info(f"  {table_name}: {row_count:,} rows")

        conn.execute("DETACH sqlite_db;")

        sqlite_clean(db_target)
        
        logger.info("=" * 60)
        logger.info(f"Export completed. SQLite database created at: {db_sqlite_dg_target}")
        logger.info("Note: Complex DuckDB types (e.g., LIST, STRUCT) may not fully preserve in SQLite")

if __name__ == "__main__":
    convert_duckdb_to_sqlite()