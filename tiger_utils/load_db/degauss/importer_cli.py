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

from .tiger_importer import TigerImporter


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
        help="Path to SQLite database (will be created if not exists)",
    )

    parser.add_argument(
        "source",
        help="Directory containing TIGER/Line zip files or extracted files",
    )

    parser.add_argument(
        "counties",
        nargs="*",
        help="County FIPS codes to import (e.g., 06001 06007). If not specified, auto-detect from files.",
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
        "-v",
        "--verbose",
        action="store_true",
        help="Enable verbose debug output",
    )

    args = parser.parse_args(argv)

    setup_logging(args.verbose)
    logger = logging.getLogger(__name__)

    try:
        # Validate paths
        db_path = Path(args.database)
        source_dir = Path(args.source)

        if not source_dir.exists():
            logger.error(f"Source directory not found: {source_dir}")
            return 1

        logger.info(f"Importing TIGER/Line data to {db_path}")
        logger.info(f"Source directory: {source_dir}")

        # Create importer
        importer = TigerImporter(
            db_path=str(db_path),
            source_dir=str(source_dir),
            temp_dir=args.temp_dir,
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
    # Clean up
    if hasattr(sys, '_importer_state'):
        del sys._importer_state
    if hasattr(sys, '_importer_shape_type'):
        del sys._importer_shape_type
    print(f"Import complete. Database at {db_path}")

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="TIGER/Line to SQLite/SpatiaLite importer.")
    subparsers = parser.add_subparsers(dest="command", required=False)

    # All-in-one import
    parser_all = subparsers.add_parser("all", help="Run full import: unzip, schema, indexes, shapefiles")
    parser_all.add_argument("zip_dir", help="Directory containing TIGER/Line zip files")
    parser_all.add_argument("--db", dest="db_path", default="geocoder.db", help="Output SQLite DB path (default: geocoder.db)")
    parser_all.add_argument("--tmp", dest="temp_dir", default="_tiger_tmp", help="Temp directory for unzipped files")
    parser_all.add_argument("--recursive", action="store_true", help="Recursively search for zip files")
    parser_all.add_argument("--state", dest="state", default=None, help="State FIPS code to filter zip files (e.g., 13)")
    parser_all.add_argument("--type", dest="shape_type", default=None, help="Shape type to filter zip files (e.g., edges, faces)")

    # Unzip only
    parser_unzip = subparsers.add_parser("unzip", help="Unzip TIGER/Line zip files")
    parser_unzip.add_argument("zip_dir", help="Directory containing TIGER/Line zip files")
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
        import_tiger(args.zip_dir, args.db_path, args.temp_dir, recursive=args.recursive, state=args.state, shape_type=args.shape_type)
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
