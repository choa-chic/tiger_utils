"""
importer.py
Orchestrates the import process: unzip, load shapefiles, create schema, and index.
Referenced from DeGAUSS-org/geocoder implementation.
"""
import os
from pathlib import Path
import sys
import glob

from tiger_utils.load_db import unzipper
from . import db_setup, shp_to_temp_tables


def run_unzip(zip_dir: str, out_dir: str, recursive: bool = False, state: str = None, shape_type: str = None):
    unzipper.unzip_all(zip_dir, out_dir, recursive=recursive, state=state, shape_type=shape_type)

def run_schema(db_path: str):
    db_setup.create_schema(db_path)

def run_indexes(db_path: str):
    db_setup.create_indexes(db_path)

def run_shp_import(shp_dir: str, db_path: str):
    import re
    import sys
    # Allow passing state and shape_type as optional args (for internal use)
    state = getattr(sys, '_importer_state', None)
    shape_type = getattr(sys, '_importer_shape_type', None)
    for shp_file in Path(shp_dir).rglob("*.shp"):
        name = shp_file.name
        # If state is set, filter by state FIPS in correct position
        if state:
            if not re.search(r"tl_\d{4}_(0?%s)[0-9]{3}_" % re.escape(state), name):
                continue
        if shape_type and shape_type not in name:
            continue
        table_name = shp_file.stem.lower()
        shp_to_sqlite.shp_to_sqlite(str(shp_file), db_path, table_name)

def import_tiger(zip_dir: str, db_path: str = "geocoder.db", temp_dir: str = "_tiger_tmp", recursive: bool = False, state: str = None, shape_type: str = None, year: str = None):
    """
    Import TIGER/Line data following DeGAUSS-org/geocoder workflow:
    1. Unzip files
    2. Create final table schema
    3. Create temporary tables
    4. Load shapefiles into temporary tables
    5. Transform temporary to final tables (convert.sql)
    6. Create indexes
    """
    temp_dir = Path(temp_dir)
    temp_dir.mkdir(exist_ok=True)
    
    # Step 1: Unzip files
    print("Step 1: Unzipping files...")
    run_unzip(zip_dir, temp_dir, recursive=recursive, state=state, shape_type=shape_type)
    
    # Step 2: Create final table schema
    print("Step 2: Creating final table schema...")
    run_schema(db_path)
    
    # Step 3: Create temporary tables
    print("Step 3: Creating temporary tables...")
    db_setup.create_temp_tables(db_path)
    
    # Step 4: Load shapefiles into temporary tables
    print("Step 4: Loading shapefiles into temporary tables...")
    shp_to_temp_tables.load_tiger_files_to_temp(str(temp_dir), db_path, state=state, year=year)
    
    # Step 5: Transform temporary to final tables
    print("Step 5: Transforming temporary to final tables...")
    db_setup.transform_temp_to_final(db_path)
    
    # Step 6: Create indexes
    print("Step 6: Creating indexes...")
    run_indexes(db_path)
    
    print(f"Import complete. Database at {db_path}")

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="TIGER/Line to SQLite/SpatiaLite importer.")
    subparsers = parser.add_subparsers(dest="command", required=False)


    # All-in-one import
    parser_all = subparsers.add_parser("all", help="Run full import: unzip, schema, indexes, shapefiles")
    parser_all.add_argument(
        "zip_dir",
        nargs="?",
        default="./tiger_utils",
        help="Directory containing TIGER/Line zip files (default: ./tiger_utils)"
    )
    parser_all.add_argument("--db", dest="db_path", default="geocoder.db", help="Output SQLite DB path (default: geocoder.db)")
    parser_all.add_argument("--tmp", dest="temp_dir", default="_tiger_tmp", help="Temp directory for unzipped files")
    parser_all.add_argument("--recursive", action="store_true", help="Recursively search for zip files")
    parser_all.add_argument("--state", dest="state", default=None, help="State FIPS code to filter zip files (e.g., 13)")
    parser_all.add_argument("--type", dest="shape_type", default=None, help="Shape type to filter zip files (e.g., edges, faces)")
    parser_all.add_argument("--year", dest="year", default=None, help="Year to filter or annotate (parsed from filename if not provided)")

    # Unzip only
    parser_unzip = subparsers.add_parser("unzip", help="Unzip TIGER/Line zip files")
    parser_unzip.add_argument(
        "zip_dir",
        nargs="?",
        default="./tiger_utils",
        help="Directory containing TIGER/Line zip files (default: ./tiger_utils)"
    )
    parser_unzip.add_argument("out_dir", help="Output directory for unzipped files")
    parser_unzip.add_argument("--recursive", action="store_true", help="Recursively search for zip files")
    parser_unzip.add_argument("--state", dest="state", default=None, help="State FIPS code to filter zip files (e.g., 13)")
    parser_unzip.add_argument("--type", dest="shape_type", default=None, help="Shape type to filter zip files (e.g., edges, faces)")

    # Schema only
    parser_schema = subparsers.add_parser("schema", help="Create database schema")
    parser_schema.add_argument("--db", dest="db_path", default="geocoder.db", help="Output SQLite DB path (default: geocoder.db)")

    # Indexes only
    parser_indexes = subparsers.add_parser("indexes", help="Create database indexes")
    parser_indexes.add_argument("--db", dest="db_path", default="geocoder.db", help="Output SQLite DB path (default: geocoder.db)")

    # Import shapefiles only
    parser_shp = subparsers.add_parser("shp", help="Import shapefiles into database")
    parser_shp.add_argument("shp_dir", help="Directory containing .shp files (unzipped)")
    parser_shp.add_argument("--db", dest="db_path", default="geocoder.db", help="Output SQLite DB path (default: geocoder.db)")

    args = parser.parse_args()

    if args.command == "all":
        import_tiger(args.zip_dir, args.db_path, args.temp_dir, recursive=args.recursive, state=args.state, shape_type=args.shape_type, year=args.year)
    elif args.command == "unzip":
        run_unzip(args.zip_dir, args.out_dir, recursive=args.recursive, state=args.state, shape_type=args.shape_type)
    elif args.command == "schema":
        run_schema(args.db_path)
    elif args.command == "indexes":
        run_indexes(args.db_path)
    elif args.command == "shp":
        run_shp_import(args.shp_dir, args.db_path)
    else:
        parser.print_help()
