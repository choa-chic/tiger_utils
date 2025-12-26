"""
schema.py
Creates the required SQLite schema (tables, indexes) for TIGER/Line data import.
"""
import sqlite3

def create_schema(db_path: str = "geocoder.db") -> None:
    """
    Creates tables for TIGER/Line import. Adjust as needed for your schema.
    """
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    # Set PRAGMA settings for performance
    cur.execute('PRAGMA temp_store=MEMORY;')
    cur.execute('PRAGMA journal_mode=OFF;')
    cur.execute('PRAGMA synchronous=OFF;')
    cur.execute('PRAGMA cache_size=500000;')
    cur.execute('PRAGMA count_changes=0;')
    # Create 'place' table
    cur.execute('''
    CREATE TABLE IF NOT EXISTS place(
        zip CHAR(5),
        city VARCHAR(100),
        state CHAR(2),
        city_phone VARCHAR(5),
        lat NUMERIC(9,6),
        lon NUMERIC(9,6),
        status CHAR(1),
        fips_class CHAR(2),
        fips_place CHAR(7),
        fips_county CHAR(5),
        priority CHAR(1)
    );
    ''')
    # Create 'edge' table
    cur.execute('''
    CREATE TABLE IF NOT EXISTS edge (
        tlid INTEGER PRIMARY KEY,
        geometry BLOB
    );
    ''')
    # Create 'feature' table
    cur.execute('''
    CREATE TABLE IF NOT EXISTS feature (
        fid INTEGER PRIMARY KEY,
        street VARCHAR(100),
        street_phone VARCHAR(5),
        paflag BOOLEAN,
        zip CHAR(5)
    );
    ''')
    # Create 'feature_edge' table
    cur.execute('''
    CREATE TABLE IF NOT EXISTS feature_edge (
        fid INTEGER,
        tlid INTEGER
    );
    ''')
    # Create 'range' table
    cur.execute('''
    CREATE TABLE IF NOT EXISTS range (
        tlid INTEGER,
        fromhn INTEGER,
        tohn INTEGER,
        prenum VARCHAR(12),
        zip CHAR(5),
        side CHAR(1)
    );
    ''')
    conn.commit()
    conn.close()
    print(f"Created schema in {db_path}")

def create_indexes(db_path: str = "geocoder.db") -> None:
    """
    Creates indexes for TIGER/Line import tables.
    """
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    # Set PRAGMA settings for performance
    cur.execute('PRAGMA temp_store=MEMORY;')
    cur.execute('PRAGMA journal_mode=OFF;')
    cur.execute('PRAGMA synchronous=OFF;')
    cur.execute('PRAGMA cache_size=500000;')
    cur.execute('PRAGMA count_changes=0;')
    cur.execute('''CREATE INDEX IF NOT EXISTS place_city_phone_state_idx ON place (city_phone, state);''')
    cur.execute('''CREATE INDEX IF NOT EXISTS place_zip_priority_idx ON place (zip, priority);''')
    cur.execute('''CREATE INDEX IF NOT EXISTS feature_street_phone_zip_idx ON feature (street_phone, zip);''')
    cur.execute('''CREATE INDEX IF NOT EXISTS feature_edge_fid_idx ON feature_edge (fid);''')
    cur.execute('''CREATE INDEX IF NOT EXISTS range_tlid_idx ON range (tlid);''')
    conn.commit()
    conn.close()
    print(f"Created indexes in {db_path}")

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
