"""
importer.py
CLI for importing Census ZIP/SHP files into DuckDB using the modular loader.
With performance optimizations: parallel processing, async I/O, progress bars.
"""
import argparse
import os
import asyncio
import multiprocessing
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor, as_completed
from typing import List, Tuple
from .schema_mapper import get_duckdb_schema
from .loader import load_shp_to_duckdb, load_dbf_to_duckdb, load_shp_from_zip_directly

try:
    from tqdm import tqdm
    HAS_TQDM = True
except ImportError:
    HAS_TQDM = False
    tqdm = None

def is_valid_tiger_filename(filename: str, year: str = None, state: str = None) -> bool:
    """
    Validate TIGER/Line filename format: tl_YYYY_SSCCC_FEAT.ext
    
    Args:
        filename: The filename to validate
        year: Optional year filter (e.g., "2021")
        state: Optional state FIPS filter (e.g., "13")
    
    Returns:
        True if filename matches TIGER/Line pattern, False otherwise
    """
    import re
    
    # Pattern: tl_YYYY_SSCCC_FEAT or tl_YYYY_SS_FEAT (for state-level files)
    # YYYY = 4-digit year
    # SSCCC = 5-digit FIPS (2-digit state + 3-digit county) or just SS for state files
    # FEAT = feature type (edges, faces, addr, featnames, etc.)
    pattern = r'^tl_(\d{4})_(\d{2,5})_([a-z0-9]+)\.(shp|dbf|shx|prj)$'
    match = re.match(pattern, filename.lower())
    
    if not match:
        return False
    
    file_year, file_fips, file_feat, file_ext = match.groups()
    
    # Validate year if specified
    if year and file_year != year:
        return False
    
    # Validate state if specified (check first 2 digits of FIPS)
    if state and len(file_fips) >= 2:
        # Handle both "13" and "013" formats
        file_state = file_fips[:2].lstrip('0') or '0'
        filter_state = state.lstrip('0') or '0'
        if file_state != filter_state:
            return False
    
    return True

async def find_files_async(directory: Path, pattern: str, state_filter: str = None, year_filter: str = None) -> List[Path]:
    """
    Async file discovery with TIGER/Line filename validation (optimization #8).
    
    Args:
        directory: Directory to search
        pattern: Glob pattern (e.g., '*.shp')
        state_filter: Optional state FIPS code to filter
        year_filter: Optional year to filter (e.g., "2021")
    
    Returns:
        List of matching file paths that conform to TIGER/Line naming conventions
    """
    loop = asyncio.get_event_loop()
    files = await loop.run_in_executor(None, lambda: list(directory.rglob(pattern)))
    
    # Filter to only valid TIGER/Line files
    valid_files = []
    for f in files:
        if is_valid_tiger_filename(f.name, year=year_filter, state=state_filter):
            valid_files.append(f)
    
    return valid_files

def import_file_wrapper(args: Tuple[Path, str, str]) -> Tuple[bool, str]:
    """
    Wrapper function for parallel file import (optimization #1).
    
    Args:
        args: Tuple of (file_path, db_path, file_type)
    
    Returns:
        Tuple of (success, message)
    """
    file_path, db_path, file_type = args
    try:
        if file_type == 'shp':
            schema = get_duckdb_schema(str(file_path))
            load_shp_to_duckdb(str(file_path), schema, db_path, use_connection_pool=True)
        else:  # dbf
            load_dbf_to_duckdb(str(file_path), db_path, use_connection_pool=True)
        return True, str(file_path.name)
    except Exception as e:
        return False, f"{file_path.name}: {e}"

def import_census_to_duckdb(
    input_dir: str,
    output_dir: str,
    db_path: str,
    recursive: bool = False,
    state: str = None,
    shape_type: str = None,
    year: str = None,
    logger=None,
    max_workers: int = None,
    use_direct_zip: bool = False,
):
    """
    High-level function: unzip, map schema, and load all SHP files to DuckDB.
    With optimizations: parallel processing, async file discovery, progress bars.
    
    Args:
        input_dir: Directory containing ZIP files
        output_dir: Directory to extract files to
        db_path: DuckDB database file
        recursive: Recursively search for ZIP files
        state: State FIPS code to filter (optional)
        shape_type: Shape type to filter (optional)
        year: Census year to filter (optional, e.g., "2021")
        logger: Optional logger instance (if None, sets up default logger)
        max_workers: Max parallel workers (default: CPU count, capped at 8)
        use_direct_zip: If True, attempt to read SHP from ZIP without extraction (optimization #4)
    """
    from tiger_utils.load_db.unzipper import unzip_all
    from tiger_utils.utils.logger import setup_logger
    if logger is None:
        logger = setup_logger()
    
    if max_workers is None:
        max_workers = min(multiprocessing.cpu_count(), 8)
    
    logger.info(f"Starting Census import to DuckDB (parallel workers: {max_workers})")
    
    # Ensure input/output directories exist
    input_path = Path(input_dir)
    output_path = Path(output_dir)
    if not input_path.exists():
        logger.error(f"Input directory does not exist: {input_path}")
        raise FileNotFoundError(f"Input directory does not exist: {input_path}")
    output_path.mkdir(parents=True, exist_ok=True)
    
    # Unzip all relevant files (unless using direct ZIP reading)
    if not use_direct_zip:
        logger.info("Extracting ZIP files...")
        unzip_all(str(input_path), str(output_path), recursive=recursive, state=state, shape_type=shape_type)
    
    # Async file discovery (optimization #8)
    logger.info("Discovering files...")
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    shp_files = loop.run_until_complete(find_files_async(output_path, "*.shp", state, year))
    dbf_files = loop.run_until_complete(find_files_async(output_path, "*.dbf", state, year))
    loop.close()
    
    # Exclude .dbf files that have a .shp with the same stem
    shp_stems = {shp_path.stem for shp_path in shp_files}
    dbf_files_to_import = [dbf for dbf in dbf_files if dbf.stem not in shp_stems]
    
    logger.info(f"Found {len(shp_files)} SHP files and {len(dbf_files_to_import)} DBF files to import")
    
    # Parallel import (optimization #1) with progress bar (optimization #9)
    all_files = [(f, db_path, 'shp') for f in shp_files] + [(f, db_path, 'dbf') for f in dbf_files_to_import]
    
    if not all_files:
        logger.warning("No files to import")
        return
    
    logger.info(f"Importing {len(all_files)} files in parallel (workers: {max_workers})...")
    
    success_count = 0
    fail_count = 0
    cancelled = False
    
    try:
        # Use ThreadPoolExecutor for I/O-bound operations
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            if HAS_TQDM:
                futures = {executor.submit(import_file_wrapper, args): args for args in all_files}
                with tqdm(total=len(all_files), desc="Importing files", unit="file") as pbar:
                    try:
                        for future in as_completed(futures):
                            success, msg = future.result()
                            if success:
                                success_count += 1
                                pbar.set_postfix_str(f"✓ {msg}")
                            else:
                                fail_count += 1
                                logger.error(f"✗ {msg}")
                            pbar.update(1)
                    except KeyboardInterrupt:
                        cancelled = True
                        pbar.write("\n⚠️  Keyboard interrupt received. Stopping import...")
                        executor.shutdown(wait=False, cancel_futures=True)
                        raise
            else:
                # Fallback without tqdm
                futures = [executor.submit(import_file_wrapper, args) for args in all_files]
                for i, future in enumerate(as_completed(futures), 1):
                    success, msg = future.result()
                    if success:
                        success_count += 1
                        logger.info(f"[{i}/{len(all_files)}] ✓ {msg}")
                    else:
                        fail_count += 1
                        logger.error(f"[{i}/{len(all_files)}] ✗ {msg}")
    except KeyboardInterrupt:
        cancelled = True
        logger.warning("\n⚠️  Import cancelled by user (Ctrl+C)")
        logger.info(f"Partial import stats: {success_count} succeeded, {fail_count} failed before cancellation")
        return
    
    if not cancelled:
        logger.info(f"Census import complete: {success_count} succeeded, {fail_count} failed")

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
    parser.add_argument(
        "--workers",
        type=int,
        default=None,
        help="Number of parallel workers (default: CPU count, max 8)",
    )
    parser.add_argument(
        "--no-direct-zip",
        action="store_false",
        dest="direct_zip",
        default=True,
        help="Disable direct ZIP reading (extracts files to disk instead)",
    )
    parser.add_argument(
        "--skip-consolidation",
        action="store_true",
        help="Skip automatic table consolidation and indexing after import",
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
    max_workers = args.workers
    use_direct_zip = args.direct_zip
    skip_consolidation = args.skip_consolidation

    if max_workers is None:
        max_workers = min(multiprocessing.cpu_count(), 8)

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
    print(f"Parallel workers:  {max_workers}")
    print(f"Direct ZIP read:   {use_direct_zip}")
    print(f"Auto consolidate:  {not skip_consolidation}")
    print("="*70)
    
    # Ask for confirmation
    try:
        response = input("\nProceed with import? (Y/n): ").strip().lower()
    except KeyboardInterrupt:
        print("\n\nImport cancelled.")
        return
    
    if response in ("n", "no"):
        print("Import cancelled.")
        return
    
    try:
        import_census_to_duckdb(
            input_dir=input_dir,
            output_dir=output_dir,
            db_path=db_path,
            recursive=recursive,
            state=state,
            shape_type=shape_type,
            year=year,
            max_workers=max_workers,
            use_direct_zip=use_direct_zip,
        )
    except KeyboardInterrupt:
        print("\n\n⚠️  Import interrupted by user")
        print("Note: Partial data may have been imported to the database.")
        return
    
    # Optimization #6: Deferred indexing after all data is loaded
    if not skip_consolidation:
        print("\n" + "="*70)
        print("CONSOLIDATION & INDEXING")
        print("="*70)
        print("Consolidating tables and creating indexes...")
        print("(This may take several minutes for large datasets)")
        print("Press Ctrl+C to skip consolidation (you can run it later)")
        print("="*70)
        
        try:
            from .consolidator import consolidate_tables
            from tiger_utils.utils.logger import setup_logger
            consolidate_tables(db_path, drop_source_tables=False)
            print("✓ Consolidation and indexing complete!")
        except KeyboardInterrupt:
            print("\n\n⚠️  Consolidation skipped by user")
            print("You can run consolidation later with:")
            print(f"  python -m tiger_utils.load_db.duckdb.consolidator --db {db_path}")
            return

if __name__ == "__main__":
    main()
