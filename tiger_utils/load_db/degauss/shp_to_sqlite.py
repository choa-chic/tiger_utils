"""
shp_to_sqlite.py
Loads shapefiles into a SQLite database table using fiona and sqlite3.
"""


import os
import sqlite3
from pathlib import Path
import fiona
from fiona import BytesCollection
from shapely.geometry import shape
import binascii
from . import unzipper

def shp_to_sqlite(shp_path: str, db_path: str, table_name: str) -> None:
    """
    Loads a shapefile into a SpatiaLite-enabled SQLite table. Table is created if it does not exist.
    Geometry is stored as WKB in a 'geometry' column (BLOB).
    Adds spatial metadata if needed.
    """
    import shapely.wkb
    import shapely.geometry
    import shapely
    with fiona.open(shp_path) as src:
        fields = src.schema['properties']
        # Map Fiona/OGR types to SQLite types
        def ogr_to_sqlite_type(ogr_type):
            if ogr_type.startswith('int'):
                return 'INTEGER'
            elif ogr_type.startswith('float') or ogr_type.startswith('double') or ogr_type.startswith('real'):
                return 'REAL'
            elif ogr_type.startswith('date'):
                return 'TEXT'
            else:
                return 'TEXT'
        columns = list(fields.keys())
        col_defs = ', '.join([f'"{col}" {ogr_to_sqlite_type(fields[col])}' for col in columns])
        # Geometry column as BLOB
        col_defs += ', geometry BLOB'
        # Connect to SQLite and load SpatiaLite
        conn = sqlite3.connect(db_path)
        cur = conn.cursor()
        # Enable extension loading explicitly
        try:
            conn.enable_load_extension(True)
        except Exception:
            print("Warning: Could not enable extension loading on this SQLite connection.")
        loaded_spatialite = False
        try:
            cur.execute("SELECT load_extension('mod_spatialite')")
            loaded_spatialite = True
        except Exception:
            try:
                cur.execute("SELECT load_extension('libspatialite')")
                loaded_spatialite = True
            except Exception:
                print("Warning: Could not load SpatiaLite extension. Proceeding without spatial index support.")
        # Initialize spatial metadata if needed
        if loaded_spatialite:
            cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='geometry_columns'")
            if not cur.fetchone():
                cur.execute("SELECT InitSpatialMetadata(1)")
        # Create table if not exists
        create_sql = f'CREATE TABLE IF NOT EXISTS "{table_name}" ({col_defs});'
        cur.execute(create_sql)
        # Register geometry column
        if loaded_spatialite:
            cur.execute(f"SELECT RecoverGeometryColumn('{table_name}', 'geometry', {src.crs['init'].split(':')[1] if src.crs and 'init' in src.crs else 4326}, '{src.schema['geometry']}', 2)")
        # Insert features
        insert_sql = f'INSERT INTO "{table_name}" ({', '.join(columns)}, geometry) VALUES ({', '.join(['?']*(len(columns)+1))});'
        for feat in src:
            values = [feat['properties'].get(f, None) for f in columns]
            if feat['geometry']:
                geom = shape(feat['geometry'])
                wkb = geom.wkb
            else:
                wkb = None
            values.append(wkb)
            cur.execute(insert_sql, values)
        conn.commit()
        # Optionally, create spatial index
        if loaded_spatialite:
            try:
                cur.execute(f"SELECT CreateSpatialIndex('{table_name}', 'geometry')")
            except Exception:
                print(f"Warning: Could not create spatial index for {table_name}.")
        conn.close()
        print(f"Loaded {shp_path} into {table_name} in {db_path} (spatially enabled: {loaded_spatialite})")

if __name__ == "__main__":
    print("This module is not intended to be run directly. Use importer.py as the CLI entry point for all workflows.")
