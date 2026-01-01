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
import re

from .db_setup import create_schema, create_indexes, load_place_data
from .tiger_etl import TigerETL
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

    def __init__(
        self,
        db_path: str,
        source_dir: str,
        temp_dir: Optional[str] = None,
        state: Optional[str] = None,
        year: Optional[str] = None,
        recursive: bool = True,
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
        self.state = state
        self.year = year
        self.recursive = recursive
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

        # Load place gazetteer if not already present
        load_place_data(str(self.db_path))

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

            # Find EDGES, FEATNAMES, ADDR files
            edges_shp = self._find_file(work_dir, "*_EDGES.shp")
            featnames_dbf = self._find_file(work_dir, "*_FEATNAMES.dbf")
            addr_dbf = self._find_file(work_dir, "*_ADDR.dbf")

            if not all([edges_shp, featnames_dbf, addr_dbf]):
                missing = []
                if not edges_shp:
                    missing.append("EDGES.shp")
                if not featnames_dbf:
                    missing.append("FEATNAMES.dbf")
                if not addr_dbf:
                    missing.append("ADDR.dbf")
                raise FileNotFoundError(
                    f"Missing required files for {county_code}: {', '.join(missing)}"
                )

            # Process county with in-memory ETL
            etl = TigerETL(
                db_path=str(self.db_path),
                batch_size=self.batch_size,
                verbose=self.verbose,
            )
            etl.process_county(edges_shp, featnames_dbf, addr_dbf, county_code)

        finally:
            # Clean up temporary files
            if work_dir.exists():
                shutil.rmtree(work_dir)

    def _drop_temp_tables(self) -> None:
        """Drop temp tables from prior runs to avoid table-exists conflicts."""
        conn = sqlite3.connect(str(self.db_path))
        cur = conn.cursor()
        try:
            for tbl in [
                "feature_bin",
                "linezip",
                "tiger_addr",
                "tiger_featnames",
                "tiger_edges",
            ]:
                cur.execute(f"DROP TABLE IF EXISTS {tbl};")
            conn.commit()
        finally:
            cur.close()
            conn.close()

    def _find_file(self, directory: Path, pattern: str) -> Optional[Path]:
        """
        Find a file matching pattern in directory.

        Args:
            directory: Directory to search
            pattern: Glob pattern (e.g., "*_EDGES.shp")

        Returns:
            Path to first matching file, or None
        """
        matches = list(directory.glob(pattern))
        if matches:
            return matches[0]
        return None

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

        iterator = self.source_dir.rglob("*.zip") if self.recursive else self.source_dir.glob("*.zip")
        year_filter = self.year
        state_filter = self.state

        for file_path in iterator:
            name = file_path.stem.lower()

            # Enforce year if provided
            if year_filter and f"tl_{year_filter}_" not in name:
                continue

            # Extract county from standard TIGER naming: tl_YYYY_SSSCC_edges
            m = re.search(r"tl_(\d{4})_(\d{5})", name)
            county_code = None
            if m:
                county_code = m.group(2)
                file_state = county_code[:2]
                if state_filter and file_state != state_filter:
                    continue
            else:
                # Fallback: look for any 5-digit numeric chunk
                for chunk in name.split("_"):
                    if chunk.isdigit() and len(chunk) == 5:
                        county_code = chunk
                        if state_filter and county_code[:2] != state_filter:
                            county_code = None
                            continue
                        break

            if not county_code:
                continue

            counties.add(county_code)

        if not counties:
            raise ValueError(
                f"No TIGER/Line files found in {self.source_dir}. "
                "Expected patterns: tl_*_*_edges.zip"
            )
        filtered = counties
        if self.state:
            filtered = {c for c in counties if c.startswith(self.state)}
        return sorted(list(filtered))

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
            recursive=self.recursive,
            state=self.state or county_code[:2],
            county=county_code,
            year=self.year,
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

    def create_indexes(self) -> None:
        """Create final indexes for queryability."""
        logger.info("Creating final indexes")
        create_indexes(str(self.db_path))
        logger.info("Index creation complete")


def import_tiger_data(
    db_path: str,
    source_dir: str,
    counties: Optional[List[str]] = None,
    state: Optional[str] = None,
    year: Optional[str] = None,
    recursive: bool = True,
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
        state=state,
        year=year,
        recursive=recursive,
        verbose=verbose,
    )
    importer.import_all(counties)
    importer.create_indexes()
