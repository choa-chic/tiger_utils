"""
db_setup.py - Database schema and initialization for TIGER/Line geocoding.

Creates SQLite tables and indexes following the degauss-org/geocoder schema:
- place: Gazetteer of place names
- edge: Line geometries with TLID (stored as WKB BLOB)
- feature: Street names with phonetic codes
- feature_edge: Junction table linking features to edges
- range: Address ranges by side (left/right)

Mirrors build/sql/{create,setup,index}.sql from degauss-org/geocoder.
"""
import logging
import sqlite3
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)


def create_schema(db_path: Optional[str] = None) -> None:
    """
    Create SQLite schema for TIGER/Line geocoding.

    Tables:
    - place: Gazetteer (city, state, ZIP, coordinates)
    - edge: Line geometries (WKB BLOB format)
    - feature: Street names with metaphone codes
    - feature_edge: Links features to edges
    - range: Address ranges (low/high numbers, side, ZIP)

    Args:
        db_path: Path to SQLite database. If None, uses default location.
    """
    if db_path is None:
        # Default: project_root / database / geocoder.db
        project_root = Path(__file__).resolve().parents[3]
        db_path = project_root / "database" / "geocoder.db"
        db_path.parent.mkdir(parents=True, exist_ok=True)

    db_path = Path(db_path)
    db_path.parent.mkdir(parents=True, exist_ok=True)

    conn = sqlite3.connect(str(db_path))
    cur = conn.cursor()

    # Set PRAGMA settings for performance (matching degauss setup.sql)
    cur.execute("PRAGMA temp_store=MEMORY;")
    cur.execute("PRAGMA journal_mode=WAL;")  # Write-Ahead Logging for concurrency
    cur.execute("PRAGMA synchronous=NORMAL;")  # Balance speed and safety
    cur.execute("PRAGMA cache_size=500000;")
    cur.execute("PRAGMA count_changes=0;")

    # Create 'place' table - Gazetteer of place names
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS place(
            zip CHAR(5),
            city VARCHAR(100),
            state CHAR(2),
            city_phone VARCHAR(5),
            lat NUMERIC(9,6),
            lon NUMERIC(9,6),
            status CHAR(1),
            fips_class CHAR(2),
            fips_place CHAR(7),
            fips_county CHAR(5),
            priority CHAR(1)
        );
    """
    )

    # Create 'edge' table - Line geometries with TLID
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS edge (
            tlid INTEGER PRIMARY KEY,
            geometry BLOB
        );
    """
    )

    # Create 'feature' table - Street names with metaphone codes
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS feature (
            fid INTEGER PRIMARY KEY,
            street VARCHAR(100),
            street_phone VARCHAR(5),
            paflag BOOLEAN,
            zip CHAR(5)
        );
    """
    )

    # Create 'feature_edge' table - Links features to edges
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS feature_edge (
            fid INTEGER,
            tlid INTEGER
        );
    """
    )

    # Create 'range' table - Address ranges with side (L/R)
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS range (
            tlid INTEGER,
            fromhn INTEGER,
            tohn INTEGER,
            prenum VARCHAR(12),
            zip CHAR(5),
            side CHAR(1)
        );
    """
    )

    conn.commit()
    conn.close()
    logger.info(f"Created schema in {db_path}")


def load_place_data(db_path: Optional[str] = None) -> None:
    """
    Load place (gazetteer) data from place.sql if not already present.

    Checks if place table has data; if empty, loads from sql/place.sql.
    This is idempotent—safe to call multiple times.

    Args:
        db_path: Path to SQLite database. If None, uses default location.
    """
    if db_path is None:
        # Default: project_root / database / geocoder.db
        project_root = Path(__file__).resolve().parents[3]
        db_path = project_root / "database" / "geocoder.db"

    db_path = Path(db_path)
    sql_dir = Path(__file__).resolve().parent / "sql"
    place_sql = sql_dir / "place.sql"

    if not place_sql.exists():
        logger.warning(f"place.sql not found at {place_sql}")
        return

    conn = sqlite3.connect(str(db_path))
    cur = conn.cursor()

    try:
        # Check if place table already has data
        cur.execute("SELECT COUNT(*) FROM place;")
        count = cur.fetchone()[0]

        if count > 0:
            logger.info(f"place table already populated with {count} rows; skipping load")
            return

        logger.info(f"Loading place data from {place_sql}")

        # Read and execute place.sql
        with open(place_sql, "r", encoding="utf-8") as f:
            sql_text = f.read()

        # Execute the SQL script
        conn.executescript(sql_text)
        conn.commit()

        # Verify load
        cur.execute("SELECT COUNT(*) FROM place;")
        count = cur.fetchone()[0]
        logger.info(f"Successfully loaded {count} place records")

    except sqlite3.OperationalError as e:
        logger.error(f"Failed to load place data: {e}")
        conn.rollback()
        raise
    finally:
        cur.close()
        conn.close()


def create_indexes(db_path: Optional[str] = None) -> None:
    """
    Create indexes for TIGER/Line tables.

    Matches degauss-org/geocoder index.sql:
    - place_city_phone_state_idx: For city lookups
    - place_zip_priority_idx: For ZIP code queries
    - feature_street_phone_zip_idx: For street name searches
    - feature_edge_fid_idx: For feature-to-edge joining
    - range_tlid_idx: For address range lookups

    Args:
        db_path: Path to SQLite database. If None, uses default location.
    """
    if db_path is None:
        # Default: project_root / database / geocoder.db
        project_root = Path(__file__).resolve().parents[3]
        db_path = project_root / "database" / "geocoder.db"

    db_path = Path(db_path)

    conn = sqlite3.connect(str(db_path))
    cur = conn.cursor()

    # Set PRAGMA settings for performance
    cur.execute("PRAGMA temp_store=MEMORY;")
    cur.execute("PRAGMA journal_mode=WAL;")
    cur.execute("PRAGMA synchronous=NORMAL;")
    cur.execute("PRAGMA cache_size=500000;")
    cur.execute("PRAGMA count_changes=0;")

    # Create indexes (matching degauss build/sql/index.sql)
    indexes = [
        (
            "place_city_phone_state_idx",
            "CREATE INDEX IF NOT EXISTS place_city_phone_state_idx ON place (city_phone, state);",
        ),
        (
            "place_zip_priority_idx",
            "CREATE INDEX IF NOT EXISTS place_zip_priority_idx ON place (zip, priority);",
        ),
        (
            "feature_street_phone_zip_idx",
            "CREATE INDEX IF NOT EXISTS feature_street_phone_zip_idx ON feature (street_phone, zip);",
        ),
        (
            "feature_edge_fid_idx",
            "CREATE INDEX IF NOT EXISTS feature_edge_fid_idx ON feature_edge (fid);",
        ),
        (
            "range_tlid_idx",
            "CREATE INDEX IF NOT EXISTS range_tlid_idx ON range (tlid);",
        ),
    ]

    for idx_name, idx_sql in indexes:
        try:
            cur.execute(idx_sql)
            logger.debug(f"Created index {idx_name}")
        except sqlite3.OperationalError as e:
            logger.warning(f"Could not create index {idx_name}: {e}")

    conn.commit()
    conn.close()
    logger.info(f"Created indexes in {db_path}")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="Create SQLite schema and/or indexes for TIGER/Line import."
    )
    default_db = Path(__file__).resolve().parents[3] / "database" / "degauss" / "geocoder.db"
    parser.add_argument(
        "db_path",
        nargs="?",
        default=str(default_db),
        help=f"Path to SQLite database (default: {default_db})",
    )
    parser.add_argument(
        "--indexes",
        action="store_true",
        help="Only create indexes (tables must exist)",
    )
    args = parser.parse_args()

    if args.indexes:
        create_indexes(args.db_path)
    else:
        create_schema(args.db_path)
        create_indexes(args.db_path)
