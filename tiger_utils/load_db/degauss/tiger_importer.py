"""
tiger_importer.py - Python equivalent of the degauss-org/geocoder tiger_import script.

Orchestrates TIGER/Line data import to SQLite following the degauss geocoder workflow:
1. Initialize database schema
2. Extract TIGER files (edges, featnames, addr)
3. Load into temporary tables
4. Transform and populate permanent tables
5. Create indexes

This replicates the C shp2sqlite + bash tiger_import workflow in pure Python.
"""

import logging
import sqlite3
import tempfile
from pathlib import Path
from typing import Optional, List, Set
import shutil

from .shp_to_sqlite import ShapefileToSQLiteConverter
from .db_setup import create_schema, create_indexes
from ..unzipper import unzip_all

logger = logging.getLogger(__name__)


class TigerImporter:
    """
    Import TIGER/Line data to SQLite following degauss geocoder conventions.

    Table structure:
    - place: Gazetteer of place names
    - edge: Line geometries with TLID (TIGER Line ID)
    - feature: Street names with computed metaphone
    - feature_edge: Links features to edges
    - range: Address ranges with side (L/R)
    """

    # File patterns and dataset types
    SHAPEFILE_TYPES = {"edges"}  # .shp files with geometry
    DBF_TYPES = {"featnames", "addr"}  # .dbf files (attributes only)

    def __init__(
        self,
        db_path: str,
        source_dir: str,
        temp_dir: Optional[str] = None,
        batch_size: int = 1000,
        verbose: bool = False,
    ):
        """
        Initialize TIGER importer.

        Args:
            db_path: Path to SQLite database (will be created if not exists)
            source_dir: Path to directory containing TIGER/Line zip files
            temp_dir: Temporary directory for extraction (default: system temp)
            batch_size: Records per batch insert
            verbose: Enable verbose logging
        """
        self.db_path = Path(db_path)
        self.source_dir = Path(source_dir)
        self.temp_dir = Path(temp_dir) if temp_dir else Path(tempfile.gettempdir())
        self.batch_size = batch_size
        self.verbose = verbose

        if not self.source_dir.exists():
            raise FileNotFoundError(f"Source directory not found: {self.source_dir}")

        # Configure logging
        level = logging.DEBUG if verbose else logging.INFO
        logging.basicConfig(level=level)

    def import_all(self, counties: Optional[List[str]] = None) -> None:
        """
        Import all TIGER/Line data for specified counties.

        Args:
            counties: List of county FIPS codes (e.g., ["06001", "06007"])
                     If None, auto-detect from source files
        """
        # Initialize database schema
        self._init_database()

        # Discover counties to import
        if counties is None:
            counties = self._discover_counties()

        logger.info(f"Importing {len(counties)} counties")

        # Import each county
        for county_code in sorted(counties):
            logger.info(f"--- Importing county {county_code}")
            try:
                self.import_county(county_code)
            except Exception as e:
                logger.error(f"Failed to import county {county_code}: {e}")
                if self.verbose:
                    raise

    def import_county(self, county_code: str) -> None:
        """
        Import TIGER/Line data for a single county.

        Args:
            county_code: County FIPS code (e.g., "06001")
        """
        # Create temporary work directory
        work_dir = self.temp_dir / f"tiger-import-{county_code}"
        work_dir.mkdir(parents=True, exist_ok=True)

        try:
            # Extract county files
            self._extract_county_files(county_code, work_dir)

            # Load shapefile with geometry (edges)
            for shp_type in self.SHAPEFILE_TYPES:
                shp_files = list(work_dir.glob(f"*_{shp_type}.shp"))
                for shp_file in shp_files:
                    logger.debug(f"Loading shapefile: {shp_file.name}")
                    self._load_shapefile(shp_file, f"tiger_{shp_type}", county_code)

            # Load DBF files (attributes only)
            for dbf_type in self.DBF_TYPES:
                dbf_files = list(work_dir.glob(f"*_{dbf_type}.dbf"))
                for dbf_file in dbf_files:
                    logger.debug(f"Loading DBF file: {dbf_file.name}")
                    self._load_dbf_file(dbf_file, f"tiger_{dbf_type}", county_code)

            # Transform and load data
            self._transform_and_load(county_code)

        finally:
            # Clean up temporary files
            if work_dir.exists():
                shutil.rmtree(work_dir)

    def _init_database(self) -> None:
        """Initialize database schema and create required tables."""
        # Create parent directories
        self.db_path.parent.mkdir(parents=True, exist_ok=True)

        # Create schema
        create_schema(str(self.db_path))
        logger.info(f"Initialized database schema at {self.db_path}")

    def _discover_counties(self) -> List[str]:
        """
        Auto-discover county FIPS codes from source files.

        Returns:
            List of county FIPS codes found
        """
        counties: Set[str] = set()

        # Look for patterns: tl_YYYY_COUNTYFIPS_edges.zip
        for pattern in ["tl_*_edges.zip", "tl_*_*_edges.zip"]:
            for file_path in self.source_dir.glob(pattern):
                # Extract county code from filename
                # Format: tl_YYYY_COUNTYFIPS_edges.zip or tl_YYYY_COUNTYFIPS_NAME.zip
                parts = file_path.stem.split("_")
                if len(parts) >= 3:
                    # County code is typically the 3rd or later component
                    # Try to find numeric FIPS code (5 digits)
                    for part in parts[2:]:
                        if part.isdigit() and len(part) == 5:
                            counties.add(part)
                            break

        if not counties:
            raise ValueError(
                f"No TIGER/Line files found in {self.source_dir}. "
                "Expected patterns: tl_*_*_edges.zip"
            )

        return sorted(list(counties))

    def _extract_county_files(self, county_code: str, work_dir: Path) -> None:
        """
        Extract TIGER/Line files for a county.

        Uses unzipper module for robust zip handling.

        Args:
            county_code: County FIPS code
            work_dir: Destination directory
        """
        # Use unzipper to extract all zipped files for this county
        # This handles both standard and pre-existing unzipped files
        unzip_all(
            input_dir=str(self.source_dir),
            output_dir=str(work_dir),
            recursive=False,
            state=county_code[:2],  # Extract state FIPS from county code
        )

        # unzipper extracts to subdirectories; flatten by moving files
        # from subdirectories to work_dir
        for subdir in work_dir.glob("*"):
            if subdir.is_dir() and subdir.name != county_code:
                # Move all files from subdirectory to work_dir root
                for file_path in subdir.glob("*"):
                    if file_path.is_file():
                        dest = work_dir / file_path.name
                        if not dest.exists():
                            file_path.rename(dest)
                # Remove empty subdirectory
                try:
                    subdir.rmdir()
                except OSError:
                    pass  # Directory not empty, skip

        # Handle unzipped files (create symlinks if not already extracted)
        for file_type in list(self.SHAPEFILE_TYPES) + list(self.DBF_TYPES):
            shp_pattern = f"*_{county_code}_{file_type}.*"
            found = False

            # Check if we already have extracted files
            if list(work_dir.glob(f"*_{county_code}_{file_type}.*")):
                found = True

            # If not found, look for unzipped source files and symlink them
            if not found:
                for file_path in self.source_dir.glob(shp_pattern):
                    if file_path.is_file():
                        dest = work_dir / file_path.name
                        if not dest.exists():
                            dest.symlink_to(file_path)
                            logger.debug(f"Linked {file_path.name}")

    def _load_shapefile(self, shp_path: Path, table_name: str, county_code: str) -> None:
        """
        Load shapefile with geometry into temporary table.

        Args:
            shp_path: Path to .shp file
            table_name: Temporary table name (e.g., "tiger_edges")
            county_code: County code (for logging)
        """
        converter = ShapefileToSQLiteConverter(
            db_path=str(self.db_path),
            table_name=table_name,
            batch_size=self.batch_size,
            geometry_column="the_geom",
            simple_geometries=False,
            append_mode=False,  # Create new temp table
        )
        converter.load_shapefile(str(shp_path))
        logger.debug(f"Loaded {shp_path.name} into {table_name}")

    def _load_dbf_file(self, dbf_path: Path, table_name: str, county_code: str) -> None:
        """
        Load DBF file (attributes only) into temporary table.

        Args:
            dbf_path: Path to .dbf file
            table_name: Temporary table name (e.g., "tiger_featnames")
            county_code: County code (for logging)
        """
        converter = ShapefileToSQLiteConverter(
            db_path=str(self.db_path),
            table_name=table_name,
            batch_size=self.batch_size,
            append_mode=False,  # Create new temp table
        )
        converter.load_dbf_only(str(dbf_path))
        logger.debug(f"Loaded {dbf_path.name} into {table_name}")

    def _transform_and_load(self, county_code: str) -> None:
        """
        Transform temporary tables into permanent schema.

        Implements the convert.sql workflow:
        1. Create indexes on temporary tables
        2. Generate linezip (edges with ZIP codes)
        3. Generate features with metaphone
        4. Link features to edges
        5. Store edges with compressed geometry
        6. Store address ranges

        Args:
            county_code: County code (for logging)
        """
        conn = sqlite3.connect(str(self.db_path))
        cur = conn.cursor()

        try:
            # Set performance pragmas
            cur.execute("PRAGMA temp_store=MEMORY;")
            cur.execute("PRAGMA journal_mode=WAL;")
            cur.execute("PRAGMA synchronous=NORMAL;")
            cur.execute("PRAGMA cache_size=500000;")

            # Create indexes on temp tables
            logger.debug(f"Creating indexes for {county_code}")
            cur.execute(
                "CREATE INDEX IF NOT EXISTS featnames_tlid ON tiger_featnames (tlid);"
            )
            cur.execute("CREATE INDEX IF NOT EXISTS addr_tlid ON tiger_addr (tlid);")
            cur.execute("CREATE INDEX IF NOT EXISTS edges_tlid ON tiger_edges (tlid);")

            # Generate linezip table (edges matched to ZIP codes)
            logger.debug(f"Generating linezip for {county_code}")
            cur.execute(
                """
                CREATE TEMPORARY TABLE linezip AS
                    SELECT DISTINCT tlid, zip FROM (
                        SELECT tlid, zip FROM tiger_addr a
                        UNION
                        SELECT tlid, zipr AS zip FROM tiger_edges e
                           WHERE e.mtfcc LIKE 'S%' AND zipr <> "" AND zipr IS NOT NULL
                        UNION
                        SELECT tlid, zipl AS zip FROM tiger_edges e
                           WHERE e.mtfcc LIKE 'S%' AND zipl <> "" AND zipl IS NOT NULL
                    ) AS whatever;
                """
            )
            cur.execute("CREATE INDEX IF NOT EXISTS linezip_tlid ON linezip (tlid);")

            # Generate features with metaphone
            logger.debug(f"Generating features for {county_code}")
            cur.execute(
                """
                CREATE TEMPORARY TABLE feature_bin (
                  fid INTEGER PRIMARY KEY AUTOINCREMENT,
                  street VARCHAR(100),
                  street_phone VARCHAR(5),
                  paflag BOOLEAN,
                  zip CHAR(5));
                """
            )

            # Initialize sequence for feature_bin
            cur.execute(
                "INSERT OR IGNORE INTO sqlite_sequence (name, seq) VALUES ('feature_bin', 0);"
            )

            # Update sequence from existing feature table max
            cur.execute(
                """
                UPDATE sqlite_sequence
                    SET seq=(SELECT COALESCE(max(fid), 0) FROM feature)
                    WHERE name="feature_bin";
                """
            )

            # Insert features with metaphone
            cur.execute(
                """
                INSERT INTO feature_bin
                    SELECT DISTINCT NULL, fullname, 
                           HEX(fullname) AS street_phone,
                           paflag, zip
                        FROM linezip l, tiger_featnames f
                        WHERE l.tlid=f.tlid AND name <> "" AND name IS NOT NULL;
                """
            )
            cur.execute(
                "CREATE INDEX IF NOT EXISTS feature_bin_idx ON feature_bin (street, zip);"
            )

            # Link features to edges
            logger.debug(f"Linking features to edges for {county_code}")
            cur.execute(
                """
                INSERT OR IGNORE INTO feature_edge
                    SELECT DISTINCT fid, f.tlid
                        FROM linezip l, tiger_featnames f, feature_bin b
                        WHERE l.tlid=f.tlid AND l.zip=b.zip
                          AND f.fullname=b.street AND f.paflag=b.paflag;
                """
            )

            # Insert final features
            cur.execute("INSERT OR IGNORE INTO feature SELECT * FROM feature_bin;")

            # Insert edges (storing WKB geometry)
            logger.debug(f"Storing edges for {county_code}")
            cur.execute(
                """
                INSERT OR IGNORE INTO edge
                    SELECT l.tlid, e.the_geom FROM
                        (SELECT DISTINCT tlid FROM linezip) AS l, tiger_edges e
                        WHERE l.tlid=e.tlid AND fullname <> "" AND fullname IS NOT NULL;
                """
            )

            # Insert address ranges
            logger.debug(f"Storing address ranges for {county_code}")
            cur.execute(
                """
                INSERT INTO range
                    SELECT tlid,
                           CAST(SUBSTR(fromhn, -10) AS INTEGER),
                           CAST(SUBSTR(tohn, -10) AS INTEGER),
                           SUBSTR(fromhn, 1, LENGTH(fromhn) - 10),
                           zip,
                           side
                    FROM tiger_addr
                    WHERE fromhn REGEXP '^[0-9]+$';
                """
            )

            # Clean up temporary tables
            logger.debug(f"Cleaning up temporary tables for {county_code}")
            cur.execute("DROP TABLE IF EXISTS feature_bin;")
            cur.execute("DROP TABLE IF EXISTS linezip;")
            cur.execute("DROP TABLE IF EXISTS tiger_addr;")
            cur.execute("DROP TABLE IF EXISTS tiger_featnames;")
            cur.execute("DROP TABLE IF EXISTS tiger_edges;")

            conn.commit()
            logger.info(f"Transformed and loaded data for county {county_code}")

        except Exception as e:
            conn.rollback()
            logger.error(f"Transform failed for county {county_code}: {e}")
            raise
        finally:
            cur.close()
            conn.close()

    def create_indexes(self) -> None:
        """Create final indexes for queryability."""
        logger.info("Creating final indexes")
        create_indexes(str(self.db_path))
        logger.info("Index creation complete")


def import_tiger_data(
    db_path: str,
    source_dir: str,
    counties: Optional[List[str]] = None,
    verbose: bool = False,
) -> None:
    """
    High-level function to import TIGER/Line data.

    Args:
        db_path: Path to SQLite database
        source_dir: Directory containing TIGER/Line files
        counties: List of county FIPS codes (auto-detect if None)
        verbose: Enable verbose logging
    """
    importer = TigerImporter(
        db_path=db_path,
        source_dir=source_dir,
        verbose=verbose,
    )
    importer.import_all(counties)
    importer.create_indexes()
