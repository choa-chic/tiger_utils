"""
db_setup.py
Creates the required SQLite schema (tables, indexes) for TIGER/Line data import.
Referenced from DeGAUSS-org/geocoder implementation.
"""
import sqlite3
from pathlib import Path

# Get the directory where this module is located
MODULE_DIR = Path(__file__).parent
SQL_DIR = MODULE_DIR / "sql"

def create_schema(db_path: str = "geocoder.db") -> None:
    """
    Creates tables for TIGER/Line import using SQL files.
    Referenced from DeGAUSS-org/geocoder implementation.
    """
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    
    # Read and execute create.sql
    create_sql_path = SQL_DIR / "create.sql"
    with open(create_sql_path, 'r') as f:
        create_sql = f.read()
    
    cur.executescript(create_sql)
    conn.commit()
    conn.close()
    print(f"Created schema in {db_path}")

def create_indexes(db_path: str = "geocoder.db") -> None:
    """
    Creates indexes for TIGER/Line import tables using SQL files.
    Referenced from DeGAUSS-org/geocoder implementation.
    """
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    
    # Read and execute index.sql
    index_sql_path = SQL_DIR / "index.sql"
    with open(index_sql_path, 'r') as f:
        index_sql = f.read()
    
    cur.executescript(index_sql)
    conn.commit()
    conn.close()
    print(f"Created indexes in {db_path}")

def create_temp_tables(db_path: str = "geocoder.db") -> None:
    """
    Creates temporary tables for TIGER/Line data loading.
    Referenced from DeGAUSS-org/geocoder implementation.
    """
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    
    # Read and execute setup.sql
    setup_sql_path = SQL_DIR / "setup.sql"
    with open(setup_sql_path, 'r') as f:
        setup_sql = f.read()
    
    cur.executescript(setup_sql)
    conn.commit()
    conn.close()
    print(f"Created temporary tables in {db_path}")

def transform_temp_to_final(db_path: str = "geocoder.db") -> None:
    """
    Transforms temporary tables to final tables.
    Referenced from DeGAUSS-org/geocoder implementation convert.sql.
    """
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    
    # Read and execute convert.sql
    convert_sql_path = SQL_DIR / "convert.sql"
    with open(convert_sql_path, 'r') as f:
        convert_sql = f.read()
    
    cur.executescript(convert_sql)
    conn.commit()
    conn.close()
    print(f"Transformed temporary tables to final tables in {db_path}")

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Create SQLite schema and/or indexes for TIGER/Line import.")
    parser.add_argument("db_path", nargs='?', default="geocoder.db", help="Path to SQLite database (default: geocoder.db)")
    parser.add_argument("--indexes", action="store_true", help="Only create indexes (tables must exist)")
    args = parser.parse_args()
    if args.indexes:
        create_indexes(args.db_path)
    else:
        create_schema(args.db_path)
        create_indexes(args.db_path)
