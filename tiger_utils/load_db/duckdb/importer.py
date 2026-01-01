"""
importer.py
CLI for importing Census ZIP/SHP files into DuckDB using the modular loader.
"""
import argparse
import os
from pathlib import Path
from .schema_mapper import get_duckdb_schema
from .loader import load_shp_to_duckdb

def import_census_to_duckdb(
    input_dir: str,
    output_dir: str,
    db_path: str,
    recursive: bool = False,
    state: str = None,
    shape_type: str = None,
    logger=None,
):
    """
    High-level function: unzip, map schema, and load all SHP files to DuckDB.
    Args:
        input_dir: Directory containing ZIP files
        output_dir: Directory to extract files to
        db_path: DuckDB database file
        recursive: Recursively search for ZIP files
        state: State FIPS code to filter (optional)
        shape_type: Shape type to filter (optional)
        logger: Optional logger instance (if None, sets up default logger)
    """
    from tiger_utils.load_db.unzipper import unzip_all
    from tiger_utils.utils.logger import setup_logger
    if logger is None:
        logger = setup_logger()
    logger.info("Starting Census import to DuckDB")
    # Ensure input/output directories exist
    input_path = Path(input_dir)
    output_path = Path(output_dir)
    if not input_path.exists():
        logger.error(f"Input directory does not exist: {input_path}")
        raise FileNotFoundError(f"Input directory does not exist: {input_path}")
    output_path.mkdir(parents=True, exist_ok=True)
    # Unzip all relevant files
    unzip_all(str(input_path), str(output_path), recursive=recursive, state=state, shape_type=shape_type)
    import re
    # Import .shp files (spatial)
    shp_files = list(output_path.rglob("*.shp"))
    if state:
        state_pattern = re.compile(r"tl_\d{4}_(0?%s)[0-9]{3}_" % re.escape(state))
        shp_files = [shp for shp in shp_files if state_pattern.search(shp.name)]
        logger.info(f"Filtered {len(shp_files)} SHP files for state FIPS {state} (from {len(list(output_path.rglob('*.shp')))} total).")
    else:
        logger.info(f"Found {len(shp_files)} SHP files to import.")
    for shp_path in shp_files:
        schema = get_duckdb_schema(str(shp_path))
        load_shp_to_duckdb(str(shp_path), schema, db_path)

    # Import .dbf files (non-spatial, e.g. addr, featnames) that do NOT have a corresponding .shp
    from .loader import load_dbf_to_duckdb
    dbf_files = list(output_path.rglob("*.dbf"))
    # Exclude .dbf files that have a .shp with the same stem
    shp_stems = {shp_path.stem for shp_path in shp_files}
    dbf_files_to_import = [dbf for dbf in dbf_files if dbf.stem not in shp_stems]
    if state:
        dbf_files_to_import = [dbf for dbf in dbf_files_to_import if state_pattern.search(dbf.name)]
        logger.info(f"Filtered {len(dbf_files_to_import)} DBF files for state FIPS {state} (from {len(dbf_files)} total).")
    else:
        logger.info(f"Found {len(dbf_files_to_import)} DBF files to import (non-spatial tables).")
    for dbf_path in dbf_files_to_import:
        load_dbf_to_duckdb(str(dbf_path), db_path)
    logger.info("Census import to DuckDB complete.")

def main():
    parser = argparse.ArgumentParser(
        description="Import Census ZIP/SHP files into DuckDB using the modular loader."
    )
    parser.add_argument(
        "--db",
        default=None,
        help="DuckDB database file (default: <project_root>/database/geocoder.duckdb)",
    )
    parser.add_argument(
        "--input-dir",
        default=None,
        help="Directory containing ZIP files (default: <project_root>/tiger_data)",
    )
    parser.add_argument(
        "--output-dir",
        default=None,
        help="Directory to extract files to (default: <project_root>/_tiger_tmp)",
    )
    parser.add_argument(
        "--year",
        default="2025",
        help="Census year (default: 2025)",
    )
    parser.add_argument(
        "--state",
        default=None,
        help="State FIPS code to filter (optional)",
    )
    parser.add_argument(
        "--no-recursive",
        action="store_false",
        dest="recursive",
        default=True,
        help="Disable recursive search for ZIP files (default: recursive search enabled)",
    )
    parser.add_argument(
        "--shape-type",
        default=None,
        help="Shape type to filter (e.g., edges, faces, addr, featnames)",
    )
    args = parser.parse_args()

    # Determine project root (three levels up from this file)
    project_root = Path(__file__).resolve().parent.parent.parent.parent
    input_dir = args.input_dir or str(project_root / "tiger_data")
    output_dir = args.output_dir or str(project_root / "_tiger_tmp")
    db_path = args.db or str(project_root / "database" / "geocoder.duckdb")
    # Ensure database directory exists
    db_dir = Path(db_path).parent
    db_dir.mkdir(parents=True, exist_ok=True)
    
    # Check if database already exists
    db_exists = Path(db_path).exists()
    if db_exists:
        print("\n" + "="*70)
        print("⚠️  DATABASE ALREADY EXISTS")
        print("="*70)
        print(f"Database file: {db_path}")
        print(f"File size:     {Path(db_path).stat().st_size / (1024*1024):.2f} MB")
        print("\nWhat would you like to do?")
        print("  [A] Append to existing database (add new data)")
        print("  [R] Remove and start over (delete existing data)")
        print("  [C] Cancel import")
        print("="*70)
        
        choice = input("\nEnter choice (A/R/C): ").strip().upper()
        
        if choice == "R":
            confirm = input(f"\n⚠️  Are you sure you want to DELETE {db_path}? (yes/no): ").strip().lower()
            if confirm == "yes":
                Path(db_path).unlink()
                print(f"✓ Deleted {db_path}")
            else:
                print("Delete cancelled. Exiting.")
                return
        elif choice == "C":
            print("Import cancelled.")
            return
        elif choice != "A":
            print(f"Invalid choice '{choice}'. Exiting.")
            return
        # If choice == "A", continue with append mode
    
    year = args.year
    state = args.state
    recursive = args.recursive
    shape_type = args.shape_type

    print("\n" + "="*70)
    print("IMPORT CONFIGURATION")
    print("="*70)
    print(f"Year:              {year}")
    print(f"Database:          {db_path}")
    print(f"Mode:              {'Append' if db_exists else 'Create new'}")
    print(f"Tiger data source: {input_dir}")
    print(f"Extract to:        {output_dir}")
    if state:
        print(f"State FIPS filter: {state}")
    if shape_type:
        print(f"Shape type filter: {shape_type}")
    print(f"Recursive search:  {recursive}")
    print("="*70)
    
    # Ask for confirmation
    response = input("\nProceed with import? (Y/n): ").strip().lower()
    if response in ("n", "no"):
        print("Import cancelled.")
        return
    
    import_census_to_duckdb(
        input_dir=input_dir,
        output_dir=output_dir,
        db_path=db_path,
        recursive=recursive,
        state=state,
        shape_type=shape_type,
    )

if __name__ == "__main__":
    main()
