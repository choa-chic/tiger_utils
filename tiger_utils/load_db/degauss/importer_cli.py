"""
importer_cli.py - Command-line interface for TIGER/Line SQLite import.

Replicates the degauss-org/geocoder tiger_import bash script in Python.

Usage:
    python -m tiger_utils.load_db.degauss.importer_cli /path/to/geocoder.db /path/to/tiger_files
    python -m tiger_utils.load_db.degauss.importer_cli /path/to/geocoder.db /path/to/tiger_files 06001 06007
"""

import argparse
import sys
from pathlib import Path
from typing import Optional, List

from tiger_utils.utils.logger import get_logger, setup_logger
from .db_setup import create_schema, create_indexes
from .tiger_importer import TigerImporter


def get_default_db_path() -> Path:
    """Return project-root database path (database/geocoder.db)."""
    return Path(__file__).resolve().parents[3] / "database" / "geocoder.db"


def confirm_defaults(
    db_path: Path,
    source_dir: Path,
    using_default_db: bool,
    using_default_source: bool,
) -> bool:
    """
    Ask user to confirm when defaults are being used.

    Args:
        db_path: Database path
        source_dir: Source directory path
        using_default_db: Whether database path is default
        using_default_source: Whether source directory is default

    Returns:
        True to proceed, False to cancel
    """
    defaults_used = []
    if using_default_db:
        defaults_used.append(f"Database: {db_path}")
    if using_default_source:
        defaults_used.append(f"Source: {source_dir}")

    if not defaults_used:
        return True  # All explicit, no confirmation needed

    print("\n⚠️  Using default paths:")
    for item in defaults_used:
        print(f"  • {item}")

    response = input("\nProceed? [Y/n]: ").strip().lower()
    return response != "n"


def confirm_database_action(db_path: Path) -> str:
    """
    Ask user what to do if database already exists.

    Args:
        db_path: Path to database file

    Returns:
        'append' to add to existing data, 'recreate' to start fresh, 'cancel' to abort
    """
    if not db_path.exists():
        return "append"  # Database doesn't exist, proceed normally

    print(f"\n⚠️  Database already exists: {db_path}")
    print("Choose an action:")
    print("  [A] Append - Add to existing data (default)")
    print("  [R] Recreate - Delete and start fresh")
    print("  [C] Cancel - Abort import")

    while True:
        response = input("\nAction [A/r/c]: ").strip().lower()
        if response == "" or response == "a":
            return "append"
        elif response == "r":
            confirm = input("⚠️  This will DELETE all existing data. Confirm? [y/N]: ").strip().lower()
            if confirm == "y":
                return "recreate"
            print("Recreate cancelled, choose another action.")
        elif response == "c":
            return "cancel"
        else:
            print("Invalid choice. Please enter A, R, or C.")


def setup_logging(verbose: bool = False) -> None:
    """Configure logging."""
    setup_logger()


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
        "--counties",
        dest="counties_flag",
        help="Comma-separated county FIPS list (e.g., 06001,06007). Overrides positional counties.",
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

    # Track which defaults are being used
    using_default_db = args.database is None
    using_default_source = args.source is None

    # Fill defaults when omitted
    if args.source is None:
        args.source = str(default_source_path)
    if args.database is None:
        args.database = str(default_db_path)

    # Normalize comma-delimited counties flag
    if args.counties_flag:
        args.counties = [c.strip() for c in args.counties_flag.split(",") if c.strip()]

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
    logger = get_logger()

    try:
        # Validate paths
        db_path = Path(args.database)
        source_dir = Path(args.source)

        if not source_dir.exists():
            logger.error(f"Source directory not found: {source_dir}")
            return 1

        # Confirm defaults if being used
        if not confirm_defaults(db_path, source_dir, using_default_db, using_default_source):
            logger.info("Import cancelled by user")
            return 0

        # Check if database exists and ask what to do
        db_action = confirm_database_action(db_path)
        if db_action == "cancel":
            logger.info("Import cancelled by user")
            return 0
        elif db_action == "recreate":
            logger.info(f"Deleting existing database: {db_path}")
            if db_path.exists():
                db_path.unlink()
            # Also delete any WAL files
            wal_file = db_path.with_suffix(db_path.suffix + "-wal")
            shm_file = db_path.with_suffix(db_path.suffix + "-shm")
            if wal_file.exists():
                wal_file.unlink()
            if shm_file.exists():
                shm_file.unlink()
            logger.info("Database deleted, will create fresh")

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
