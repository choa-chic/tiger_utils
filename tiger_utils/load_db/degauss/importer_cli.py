"""
importer_cli.py - Command-line interface for TIGER/Line SQLite import.

Replicates the degauss-org/geocoder tiger_import bash script in Python.

Usage:
    python -m tiger_utils.load_db.degauss.importer_cli /path/to/geocoder.db /path/to/tiger_files
    python -m tiger_utils.load_db.degauss.importer_cli /path/to/geocoder.db /path/to/tiger_files 06001 06007
"""

import argparse
import logging
import sys
from pathlib import Path
from typing import Optional, List

from .db_setup import create_schema, create_indexes
from .tiger_importer import TigerImporter


def get_default_db_path() -> Path:
    """Return project-root database path (database/geocoder.db)."""
    return Path(__file__).resolve().parents[3] / "database" / "geocoder.db"


def setup_logging(verbose: bool = False) -> None:
    """Configure logging."""
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )


def main(argv: Optional[List[str]] = None) -> int:
    """
    Main CLI entry point.

    Args:
        argv: Command-line arguments (default: sys.argv[1:])

    Returns:
        Exit code (0 for success, 1 for failure)
    """
    parser = argparse.ArgumentParser(
        prog="tiger-import-sqlite",
        description="Import TIGER/Line data to SQLite (Python equivalent of degauss tiger_import)",
        epilog="Example: tiger-import-sqlite /data/geocoder.db /data/tiger/ 06001 06007",
    )

    parser.add_argument(
        "database",
        nargs="?",
        help="Path to SQLite database (default: project_root/database/geocoder.db)",
    )

    parser.add_argument(
        "source",
        nargs="?",
        help="Directory containing TIGER/Line zip files or extracted files (default: project_root/tiger_data)",
    )

    parser.add_argument(
        "counties",
        nargs="*",
        help="County FIPS codes to import (e.g., 06001 06007). If not specified, auto-detect from files.",
    )

    parser.add_argument(
        "--year",
        dest="year",
        help="TIGER/Line vintage (e.g., 2025) to filter zip files",
    )

    parser.add_argument(
        "--state",
        dest="state",
        help="Filter to a 2-digit state FIPS (e.g., 06)",
    )

    parser.add_argument(
        "--temp-dir",
        help="Temporary directory for file extraction (default: system temp)",
    )

    parser.add_argument(
        "--batch-size",
        type=int,
        default=1000,
        help="Records per batch insert (default: 1000)",
    )

    parser.add_argument(
        "--init-db",
        action="store_true",
        help="Initialize database schema/indexes and exit (skip import)",
    )

    parser.add_argument(
        "--no-recursive",
        dest="recursive",
        action="store_false",
        default=True,
        help="Disable recursive search in source directory (default: recursive on)",
    )

    parser.add_argument(
        "-v",
        "--verbose",
        action="store_true",
        help="Enable verbose debug output",
    )

    project_root = Path(__file__).resolve().parents[3]
    default_db_path = get_default_db_path()
    default_source_path = project_root / "tiger_data"
    args = parser.parse_args(argv)

    # Fill defaults when omitted
    if args.source is None:
        args.source = str(default_source_path)
    if args.database is None:
        args.database = str(default_db_path)

    # Normalize year to 4-digit string if provided
    if args.year:
        args.year = str(args.year)
        if not args.year.isdigit() or len(args.year) != 4:
            parser.error("--year must be a 4-digit year (e.g., 2025)")

    # Normalize state to 2-digit FIPS if provided
    if args.state:
        args.state = str(args.state).zfill(2)
        if not args.state.isdigit() or len(args.state) != 2:
            parser.error("--state must be a 2-digit FIPS code (e.g., 06)")

    setup_logging(args.verbose)
    logger = logging.getLogger(__name__)

    try:
        # Validate paths
        db_path = Path(args.database)
        source_dir = Path(args.source)

        if not source_dir.exists():
            logger.error(f"Source directory not found: {source_dir}")
            return 1

        logger.info(f"Import target: {db_path}")
        logger.info(f"Source directory: {source_dir}")
        if args.year:
            logger.info(f"Filtering year: {args.year}")

        if args.init_db:
            create_schema(str(db_path))
            create_indexes(str(db_path))
            logger.info("Database initialized; exiting (--init-db)")
            return 0

        # Create importer
        importer = TigerImporter(
            db_path=str(db_path),
            source_dir=str(source_dir),
            temp_dir=args.temp_dir,
            state=args.state,
            year=args.year,
            recursive=args.recursive,
            batch_size=args.batch_size,
            verbose=args.verbose,
        )

        # Import data
        counties = args.counties if args.counties else None
        importer.import_all(counties)

        # Create indexes
        importer.create_indexes()

        logger.info("Import complete!")
        return 0

    except KeyboardInterrupt:
        logger.error("Import interrupted by user")
        return 130
    except Exception as e:
        logger.error(f"Import failed: {e}")
        if args.verbose:
            import traceback

            traceback.print_exc()
        return 1


if __name__ == "__main__":
    sys.exit(main())
