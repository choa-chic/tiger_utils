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
    # Find all .shp files in output_dir
    import re
    shp_files = list(output_path.rglob("*.shp"))
    # Filter by state FIPS if specified
    if state:
        # Accept both 2-digit and 3-digit FIPS (with/without leading zero)
        # e.g., tl_2025_13_ or tl_2025_013_ or tl_2025_13177_
        state_pattern = re.compile(r"tl_\d{4}_(0?%s)[0-9]{3}_" % re.escape(state))
        filtered_shp_files = [shp for shp in shp_files if state_pattern.search(shp.name)]
        logger.info(f"Filtered {len(filtered_shp_files)} SHP files for state FIPS {state} (from {len(shp_files)} total).")
        shp_files = filtered_shp_files
    else:
        logger.info(f"Found {len(shp_files)} SHP files to import.")
    for shp_path in shp_files:
        schema = get_duckdb_schema(str(shp_path))
        load_shp_to_duckdb(str(shp_path), schema, db_path)
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
        "--recursive",
        action="store_true",
        help="Recursively search for ZIP files",
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
    year = args.year
    state = args.state
    recursive = args.recursive
    shape_type = args.shape_type

    print(f"Importing Census data for year {year} into {db_path}")
    print(f"Input dir: {input_dir}\nOutput dir: {output_dir}")
    if state:
        print(f"Filtering for state FIPS: {state}")
    if shape_type:
        print(f"Filtering for shape type: {shape_type}")
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
