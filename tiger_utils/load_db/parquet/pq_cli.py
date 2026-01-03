"""
CLI for TIGER/Line Parquet utilities: convert zips to Parquet or register Parquet as DuckDB views.

Usage:
    # to convert zipped shapefiles (including shape and dbf) to parquet
    python -m tiger_utils.load_db.parquet.pq_cli convert --year 2021

    # to register parquet files as DuckDB views
    python -m tiger_utils.load_db.parquet.pq_cli register --year 2021
"""
import argparse
from pathlib import Path
from tiger_utils.load_db.parquet.zip_to_parquet import convert_all
from tiger_utils.load_db.parquet.pq_to_duckdb import register_parquet_views
from tiger_utils.utils.logger import get_logger
import shutil
import sys
import os

logger = get_logger()

def ensure_ogr2ogr():
    """
    Ensure ogr2ogr is available in PATH, adding OSGeo4W paths on Windows if necessary.
    Searches:
      1. PATH
      2. User OSGeo4W install (AppData\Local\Programs\OSGeo4W\bin)
      3. AppData\OSGeo4W*\bin
      4. Program Files/Program Files (x86)\OSGeo4W*\bin
    """
    import glob

    def add_to_path(bin_dir):
        path_dirs = os.environ["PATH"].split(";")
        if not any(os.path.normcase(bin_dir) == os.path.normcase(p) for p in path_dirs):
            os.environ["PATH"] = bin_dir + ";" + os.environ["PATH"]

    # 1. Check PATH
    ogr2ogr_path = shutil.which("ogr2ogr")
    if ogr2ogr_path:
        add_to_path(os.path.dirname(ogr2ogr_path))
        return

    # 2. User OSGeo4W install (AppData\Local\Programs\OSGeo4W\bin)
    if os.name == "nt":
        user_osgeo_bin = os.path.join(
            os.environ.get("USERPROFILE", ""),
            "AppData",
            "Local",
            "Programs",
            "OSGeo4W",
            "bin"
        )
        ogr2ogr_exe = os.path.join(user_osgeo_bin, "ogr2ogr.exe")
        if os.path.isfile(ogr2ogr_exe):
            add_to_path(user_osgeo_bin)
            if shutil.which("ogr2ogr"):
                return

        # 3. AppData\OSGeo4W*\bin
        appdata = os.environ.get("APPDATA")
        if appdata:
            candidates = glob.glob(os.path.join(appdata, "OSGeo4W*", "bin", "ogr2ogr.exe"))
            if candidates:
                add_to_path(os.path.dirname(candidates[0]))
                if shutil.which("ogr2ogr"):
                    return

        # 4. Program Files and Program Files (x86)\OSGeo4W*\bin
        for pf_var in ["ProgramFiles", "ProgramFiles(x86)"]:
            pf = os.environ.get(pf_var)
            if pf:
                candidates = glob.glob(os.path.join(pf, "OSGeo4W*", "bin", "ogr2ogr.exe"))
                if candidates:
                    add_to_path(os.path.dirname(candidates[0]))
                    if shutil.which("ogr2ogr"):
                        return

    logger.error(
        "ogr2ogr (from GDAL) is not installed or not found in PATH. Please install GDAL.\n"
        "Visit https://gdal.org/download.html for installation instructions.\n"
        "On Windows, consider using OSGeo4W (https://trac.osgeo.org/osgeo4w/) or conda.\n"
        "Searched PATH, user AppData, and Program Files for OSGeo4W."
    )
    sys.exit(1)

def main():
    parser = argparse.ArgumentParser(
        description="TIGER/Line Parquet utilities: convert zips to Parquet or register Parquet as DuckDB views."
    )

    subparsers = parser.add_subparsers(dest="command", required=True)

    # Subcommand: convert
    convert_parser = subparsers.add_parser("convert", help="Convert TIGER/Line zipped shapefiles to GeoParquet using ogr2ogr and GDAL VFS.")
    convert_parser.add_argument(
        "--input-dir",
        type=Path,
        default=Path(__file__).resolve().parent.parent.parent.parent / "tiger_data",
        help="Input directory containing TIGER/Line zip files (default: project_root/tiger_data)",
    )
    convert_parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path(__file__).resolve().parent.parent.parent.parent / "database" / "parquet",
        help="Output directory for GeoParquet files (default: project_root/database/parquet)",
    )
    convert_parser.add_argument("--year", default="2025", type=str, help="Filter by year (YYYY)")
    convert_parser.add_argument("--state", type=str, help="Filter by state FIPS (SS)")
    convert_parser.add_argument("--county", type=str, help="Filter by county FIPS (CCC)")
    convert_parser.add_argument("--layer", type=str, help="Filter by layer (e.g., EDGES, ADDR)")

    # Subcommand: register
    reg_parser = subparsers.add_parser("register", help="Register Parquet files as DuckDB views (virtual tables)")
    reg_parser.add_argument(
        "--parquet-root",
        type=Path,
        default=Path(__file__).resolve().parent.parent.parent.parent / "database" / "parquet",
        help="Root directory containing Parquet files (default: project_root/database/parquet)",
    )
    reg_parser.add_argument(
        "--duckdb-path",
        type=Path,
        help="Path to DuckDB file (default: project_root/database/parquet/YYYY/tiger_line.duckdb)",
    )
    reg_parser.add_argument("--year", required=True, type=str, help="Year (YYYY)")
    reg_parser.add_argument("--state", type=str, help="Filter by state FIPS (SS)")
    reg_parser.add_argument("--county", type=str, help="Filter by county FIPS (CCC)")
    reg_parser.add_argument("--layer", type=str, help="Filter by layer (e.g., EDGES, ADDR)")

    args = parser.parse_args()

    if args.command == "convert":
        ensure_ogr2ogr()
        logger.info(f"Starting pq_cli convert with args: input_dir={args.input_dir}, output_dir={args.output_dir}, year={args.year}, state={args.state}, county={args.county}, layer={args.layer}")
        convert_all(
            input_dir=args.input_dir,
            output_dir=args.output_dir,
            year=args.year,
            state=args.state,
            county=args.county,
            layer=args.layer,
        )
        logger.info("pq_cli convert completed.")
    elif args.command == "register":
        year = args.year
        duckdb_path = args.duckdb_path or (args.parquet_root / year / "tiger_line.duckdb")
        logger.info(f"Registering Parquet files for year={year} as DuckDB views in {duckdb_path}")
        register_parquet_views(
            parquet_root=args.parquet_root,
            duckdb_path=duckdb_path,
            year=year,
            state=args.state,
            county=args.county,
            layer=args.layer,
        )
        logger.info("pq_cli register completed.")

if __name__ == "__main__":
    main()
