"""
importer.py
Orchestrates the import process: unzip and load shapefiles dynamically.
Uses Fiona to dynamically infer table structure from TIGER/Line files.
"""
import os
from pathlib import Path
import sys
import glob
import re

from tiger_utils.load_db import unzipper
from . import db_setup, shp_to_sqlite


def run_unzip(zip_dir: str, out_dir: str, recursive: bool = False, state: str = None, shape_type: str = None):
    unzipper.unzip_all(zip_dir, out_dir, recursive=recursive, state=state, shape_type=shape_type)

def run_schema(db_path: str):
    db_setup.create_schema(db_path)

def run_indexes(db_path: str):
    db_setup.create_indexes(db_path)

def load_tiger_files_dynamic(shp_dir: str, db_path: str, state: str = None, year: str = None, shape_type: str = None) -> None:
    """
    Dynamically load all TIGER/Line shapefiles and DBF files using Fiona.
    Each file is loaded into its own table based on the filename.
    """
    shp_dir = Path(shp_dir)
    file_count = 0
    
    if not shp_dir.exists():
        print(f"Warning: Directory {shp_dir} does not exist.")
        return
    
    print(f"Searching for files in {shp_dir}...")
    if state:
        print(f"  Filtering by state: {state}")
    if year:
        print(f"  Filtering by year: {year}")
    if shape_type:
        print(f"  Filtering by shape type: {shape_type}")
    
    # Process all .shp and .dbf files
    for file_path in shp_dir.rglob("*"):
        if file_path.suffix.lower() not in ['.shp', '.dbf']:
            continue
        
        # Skip .dbf files if a corresponding .shp file exists (avoid duplicates)
        if file_path.suffix.lower() == '.dbf':
            corresponding_shp = file_path.with_suffix('.shp')
            if corresponding_shp.exists():
                continue  # Skip this DBF, the SHP will load all data
            
        name = file_path.name
        
        # Filter by state if specified
        if state:
            if not re.search(r"tl_\d{4}_(0?%s)[0-9]{3}_" % re.escape(state), name):
                continue
        
        # Filter by year if specified
        if year:
            if not re.search(r"tl_%s_" % re.escape(year), name):
                continue
        
        # Filter by shape_type if specified
        if shape_type and shape_type.lower() not in name.lower():
            continue
        
        # Use the file stem as the table name (lowercase)
        table_name = file_path.stem.lower()
        
        print(f"Loading {file_path.name} into table {table_name}...")
        shp_to_sqlite.shp_to_sqlite(str(file_path), db_path, table_name)
        file_count += 1
    
    print(f"Loaded {file_count} files into {db_path}")

def run_shp_import(shp_dir: str, db_path: str, state: str = None, year: str = None, shape_type: str = None):
    """Import shapefiles from directory using dynamic loading."""
    load_tiger_files_dynamic(shp_dir, db_path, state=state, year=year, shape_type=shape_type)

def detect_available_years(base_dir: str) -> set:
    """
    Detect available TIGER/Line years from directory structure or filenames.
    Looks for year subdirectories (e.g., 2025/) or tl_YYYY_* patterns in files.
    """
    base_path = Path(base_dir)
    years = set()
    
    # Check for year subdirectories
    if base_path.exists():
        for item in base_path.iterdir():
            if item.is_dir() and item.name.isdigit() and len(item.name) == 4:
                years.add(item.name)
        
        # Also check filenames for tl_YYYY_ pattern
        for file_path in base_path.rglob("*"):
            if file_path.is_file():
                match = re.match(r"tl_(\d{4})_", file_path.name)
                if match:
                    years.add(match.group(1))
    
    return years

def import_tiger(zip_dir: str, db_path: str = "geocoder.db", temp_dir: str = "_tiger_tmp", recursive: bool = True, state: str = None, shape_type: str = None, year: str = None):
    """
    Import TIGER/Line data using dynamic Fiona-based loading:
    1. Unzip files
    2. Dynamically load all shapefiles/DBF files into tables
    3. Optionally create indexes
    """
    temp_dir = Path(temp_dir)
    temp_dir.mkdir(exist_ok=True)
    
    # Step 1: Unzip files
    print("Step 1: Unzipping files...")
    run_unzip(zip_dir, str(temp_dir), recursive=recursive, state=state, shape_type=shape_type)
    
    # Step 2: Dynamically load all TIGER/Line files
    print("Step 2: Loading TIGER/Line files dynamically...")
    load_tiger_files_dynamic(str(temp_dir), db_path, state=state, year=year, shape_type=shape_type)
    
    # Step 3: Create indexes (optional, can be slow)
    # Uncomment if you want to create indexes after loading
    # print("Step 3: Creating indexes...")
    # run_indexes(db_path)
    
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
        default="./tiger_data",
        help="Directory containing TIGER/Line zip files (default: ./tiger_data)"
    )
    parser_all.add_argument("--db", dest="db_path", default="geocoder.db", help="Output SQLite DB path (default: geocoder.db)")
    parser_all.add_argument("--tmp", dest="temp_dir", default="_tiger_tmp", help="Temp directory for unzipped files")
    parser_all.add_argument("--recursive", action="store_true", default=True, help="Recursively search for zip files (default: True)")
    parser_all.add_argument("--no-recursive", dest="recursive", action="store_false", help="Do not recursively search for zip files")
    parser_all.add_argument("--state", dest="state", default=None, help="State FIPS code to filter zip files (e.g., 13)")
    parser_all.add_argument("--type", dest="shape_type", default=None, help="Shape type to filter zip files (e.g., edges, faces)")
    parser_all.add_argument("--year", dest="year", default=None, help="Year to filter or annotate (parsed from filename if not provided)")

    # Unzip only
    parser_unzip = subparsers.add_parser("unzip", help="Unzip TIGER/Line zip files")
    parser_unzip.add_argument(
        "zip_dir",
        nargs="?",
        default="./tiger_data",
        help="Directory containing TIGER/Line zip files (default: ./tiger_data)"
    )
    parser_unzip.add_argument("out_dir", help="Output directory for unzipped files")
    parser_unzip.add_argument("--recursive", action="store_true", default=True, help="Recursively search for zip files (default: True)")
    parser_unzip.add_argument("--no-recursive", dest="recursive", action="store_false", help="Do not recursively search for zip files")
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
        year = args.year
        # If year not specified, detect available years
        if not year:
            available_years = detect_available_years(args.zip_dir)
            if len(available_years) > 1:
                print(f"Multiple years found: {', '.join(sorted(available_years))}")
                year = input("Please enter the year to use (or press Enter to use all): ").strip()
                if not year:
                    year = None
            elif len(available_years) == 1:
                year = list(available_years)[0]
                print(f"Using detected year: {year}")
        import_tiger(args.zip_dir, args.db_path, args.temp_dir, recursive=args.recursive, state=args.state, shape_type=args.shape_type, year=year)
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
