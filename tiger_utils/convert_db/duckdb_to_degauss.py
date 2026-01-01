"""
Convert TIGER/Line DuckDB database to DeGAUSS format.

This script converts the raw TIGER/Line tables (addr, edges, featnames) 
from geocoder.duckdb into the optimized format expected by DeGAUSS geocoder
matching the degauss-org/geocoder schema.

The DeGAUSS format includes:
- edge: Line geometries with TLID (TIGER Line ID)
- feature: Street names with pre-computed metaphone phonetic codes
- feature_edge: Junction table linking features to edges
- range: Address ranges (low/high house numbers, side, ZIP)
- place: Gazetteer of place names (cities, states, ZIPs, coordinates)
"""

import duckdb
import jellyfish
from pathlib import Path
from typing import Optional
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger(__name__)


def create_degauss_schema(conn: duckdb.DuckDBPyConnection):
    """
    Create the DeGAUSS database schema matching degauss-org/geocoder.
    
    Args:
        conn: DuckDB connection to degauss database
    """
    logger.info("Creating DeGAUSS schema...")
    
    # Place table - Gazetteer of place names (cities, states, ZIPs, coordinates)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS place (
            zip VARCHAR(5) PRIMARY KEY,
            city VARCHAR(100),
            state VARCHAR(2),
            city_phone VARCHAR(5),
            lat DOUBLE,
            lon DOUBLE,
            fips_county VARCHAR(5),
            fips_class VARCHAR(2),
            fips_place VARCHAR(7),
            priority INTEGER
        );
    """)
    
    # Edge table - Line geometries with TLID (TIGER Line ID)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS edge (
            tlid BIGINT PRIMARY KEY,
            geometry GEOMETRY
        );
    """)
    
    # Feature table - Street names with phonetic codes
    conn.execute("""
        CREATE TABLE IF NOT EXISTS feature (
            fid INTEGER PRIMARY KEY,
            street VARCHAR(100),
            street_phone VARCHAR(5),
            paflag BOOLEAN,
            zip VARCHAR(5)
        );
    """)
    
    # Feature_edge table - Links features to edges
    conn.execute("""
        CREATE TABLE IF NOT EXISTS feature_edge (
            fid INTEGER,
            tlid BIGINT,
            PRIMARY KEY (fid, tlid)
        );
    """)
    
    # Range table - Address ranges with side (L/R)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS range (
            tlid BIGINT,
            fromhn INTEGER,
            tohn INTEGER,
            prenum VARCHAR(10),
            zip VARCHAR(5),
            side VARCHAR(1)
        );
    """)
    
    logger.info("Schema created successfully")


def compute_metaphone(text: Optional[str], max_length: int = 5) -> Optional[str]:
    """
    Compute metaphone phonetic code for a string.
    
    Args:
        text: Input string
        max_length: Maximum length of metaphone code
        
    Returns:
        Metaphone code or None if input is None
    """
    if not text:
        return None
    
    # Clean the text
    cleaned = ''.join(c for c in text if c.isalnum())
    if not cleaned:
        return None
    
    try:
        result = jellyfish.metaphone(cleaned)
        return result[:max_length] if result else None
    except Exception:
        return cleaned[:max_length]


def convert_to_degauss(
    source_db: str,
    target_db: str,
    state_fips: Optional[str] = None,
    batch_size: int = 10000
):
    """
    Convert TIGER/Line data from source database to DeGAUSS format.
    
    Args:
        source_db: Path to source geocoder.duckdb
        target_db: Path to target degauss.duckdb
        state_fips: Optional state FIPS code to filter (e.g., "13" for Georgia)
        batch_size: Number of records to process at once
    """
    import os
    
    # Check if target database already exists
    target_path = Path(target_db)
    if target_path.exists():
        logger.warning(f"Target database already exists: {target_db}")
        response = input("Delete existing database and proceed? (y/n): ").strip().lower()
        if response != 'y':
            logger.info("Conversion cancelled by user")
            return
        target_path.unlink()
        logger.info("Deleted existing database")
    
    logger.info(f"Converting {source_db} to {target_db}")
    
    # Connect to target database only
    target_conn = duckdb.connect(target_db)
    
    # Load spatial extension
    target_conn.execute("INSTALL spatial;")
    target_conn.execute("LOAD spatial;")
    
    # Attach source database
    logger.info(f"Attaching source database: {source_db}")
    target_conn.execute(f"ATTACH '{source_db}' AS source_db (READ_ONLY);")
    
    # Create schema in target database
    create_degauss_schema(target_conn)
    
    # Register metaphone function with NULL handling
    target_conn.create_function(
        "compute_metaphone", 
        compute_metaphone,
        return_type="VARCHAR",
        null_handling="special"
    )
    
    # Build WHERE clause for state filter
    where_clause = f"WHERE statefp = '{state_fips}'" if state_fips else ""
    
    try:
        # Step 1: Populate edge table from edges
        logger.info("Populating edge table from edges...")
        
        target_conn.execute(f"""
            INSERT INTO edge (tlid, geometry)
            SELECT DISTINCT
                tlid,
                geom as geometry
            FROM source_db.main.edges
            {where_clause}
        """)
        
        edge_count = target_conn.execute("SELECT COUNT(*) FROM edge").fetchone()[0]
        logger.info(f"Inserted {edge_count:,} records into edge table")
        
        # Step 2: Populate feature table from featnames
        logger.info("Populating feature table from featnames...")
        
        # Get TLIDs from edges if filtering by state
        tlid_filter = "WHERE tlid IN (SELECT tlid FROM edge)" if state_fips else ""
        
        target_conn.execute(f"""
            INSERT INTO feature (fid, street, street_phone, paflag, zip)
            WITH linezip AS (
                -- Match edges to ZIP codes (from addr and edges)
                SELECT DISTINCT tlid, zip FROM (
                    SELECT tlid, zip 
                    FROM source_db.main.addr
                    WHERE zip IS NOT NULL AND zip != ''
                    UNION
                    SELECT tlid, zipr AS zip 
                    FROM source_db.main.edges
                    WHERE zipr IS NOT NULL AND zipr != ''
                    UNION
                    SELECT tlid, zipl AS zip 
                    FROM source_db.main.edges
                    WHERE zipl IS NOT NULL AND zipl != ''
                ) zips
            )
            SELECT DISTINCT
                ROW_NUMBER() OVER (ORDER BY fullname, zip) as fid,
                fullname as street,
                compute_metaphone(name, 5) as street_phone,
                (paflag = 'P') as paflag,
                zip
            FROM source_db.main.featnames f
            JOIN linezip l ON f.tlid = l.tlid
            {('WHERE f.tlid IN (SELECT tlid FROM edge) AND' if state_fips else 'WHERE')} 
                f.name IS NOT NULL AND f.name != ''
        """)
        
        feature_count = target_conn.execute("SELECT COUNT(*) FROM feature").fetchone()[0]
        logger.info(f"Inserted {feature_count:,} records into feature table")
        
        # Step 3: Populate feature_edge junction table
        logger.info("Populating feature_edge table...")
        
        target_conn.execute(f"""
            INSERT INTO feature_edge (fid, tlid)
            WITH linezip AS (
                SELECT DISTINCT tlid, zip FROM (
                    SELECT tlid, zip 
                    FROM source_db.main.addr
                    WHERE zip IS NOT NULL AND zip != ''
                    UNION
                    SELECT tlid, zipr AS zip 
                    FROM source_db.main.edges
                    WHERE zipr IS NOT NULL AND zipr != ''
                    UNION
                    SELECT tlid, zipl AS zip 
                    FROM source_db.main.edges
                    WHERE zipl IS NOT NULL AND zipl != ''
                ) zips
            )
            SELECT DISTINCT
                feature.fid,
                f.tlid
            FROM source_db.main.featnames f
            JOIN linezip l ON f.tlid = l.tlid
            JOIN feature ON feature.street = f.fullname AND feature.zip = l.zip
            {('WHERE f.tlid IN (SELECT tlid FROM edge) AND' if state_fips else 'WHERE')} 
                f.name IS NOT NULL AND f.name != ''
        """)
        
        feature_edge_count = target_conn.execute("SELECT COUNT(*) FROM feature_edge").fetchone()[0]
        logger.info(f"Inserted {feature_edge_count:,} records into feature_edge table")
        
        # Step 4: Populate range table from addr
        logger.info("Populating range table from addr...")
        
        # Helper function to extract digit suffix
        def digit_suffix(s):
            """Extract trailing digits from a string."""
            if not s:
                return None
            import re
            match = re.search(r'\d+$', str(s))
            return int(match.group()) if match else None
        
        # Helper function to extract non-digit prefix
        def nondigit_prefix(s):
            """Extract leading non-digit characters from a string."""
            if not s:
                return None
            import re
            match = re.match(r'^[^\d]+', str(s))
            return match.group() if match else None
        
        # Register helper functions
        target_conn.create_function("digit_suffix", digit_suffix, return_type="INTEGER", null_handling="special")
        target_conn.create_function("nondigit_prefix", nondigit_prefix, return_type="VARCHAR", null_handling="special")
        
        # Get TLIDs that exist in edge table
        tlid_exists = "AND a.tlid IN (SELECT tlid FROM edge)" if state_fips else ""
        
        target_conn.execute(f"""
            INSERT INTO range (tlid, fromhn, tohn, prenum, zip, side)
            SELECT 
                tlid,
                digit_suffix(fromhn) as fromhn,
                digit_suffix(tohn) as tohn,
                nondigit_prefix(fromhn) as prenum,
                zip,
                side
            FROM source_db.main.addr a
            WHERE 1=1 {tlid_exists}
        """)
        
        range_count = target_conn.execute("SELECT COUNT(*) FROM range").fetchone()[0]
        logger.info(f"Inserted {range_count:,} records into range table")
        
        # Step 5: Populate place table (if it exists in source)
        logger.info("Checking for place table in source database...")
        
        # Check if place table exists in source using system catalog
        try:
            place_test = target_conn.execute("""
                SELECT COUNT(*) FROM source_db.main.place LIMIT 1
            """).fetchone()
            place_exists = True
        except Exception:
            place_exists = False
        
        if place_exists:
            logger.info("Populating place table...")
            
            zip_filter = ""
            if state_fips:
                # Filter by state FIPS if provided
                zip_filter = f"""
                WHERE zip IN (
                    SELECT DISTINCT zip FROM range
                    UNION
                    SELECT DISTINCT zip FROM feature
                )
                """
            
            target_conn.execute(f"""
                INSERT INTO place (zip, city, state, city_phone, lat, lon, fips_county, fips_class, fips_place, priority)
                SELECT DISTINCT
                    zip,
                    city,
                    state,
                    compute_metaphone(city, 5) as city_phone,
                    lat,
                    lon,
                    fips_county,
                    fips_class,
                    fips_place,
                    priority
                FROM source_db.main.place
                {zip_filter}
            """)
            
            place_count = target_conn.execute("SELECT COUNT(*) FROM place").fetchone()[0]
            logger.info(f"Inserted {place_count:,} records into place table")
        else:
            logger.warning("No place table found in source database - skipping")
            place_count = 0
        
        # Step 6: Create indexes for performance
        logger.info("Creating indexes...")
        
        indexes = [
            ("idx_edge_tlid", "edge", ["tlid"]),
            ("idx_feature_street_phone_zip", "feature", ["street_phone", "zip"]),
            ("idx_feature_zip", "feature", ["zip"]),
            ("idx_feature_edge_fid", "feature_edge", ["fid"]),
            ("idx_feature_edge_tlid", "feature_edge", ["tlid"]),
            ("idx_range_tlid", "range", ["tlid"]),
            ("idx_range_tlid_side", "range", ["tlid", "side"]),
        ]
        
        for idx_name, table, columns in indexes:
            cols_str = ", ".join(columns)
            logger.info(f"Creating index {idx_name} on {table}({cols_str})")
            target_conn.execute(f"CREATE INDEX {idx_name} ON {table}({cols_str});")
        
        # Create spatial index on edge geometry
        logger.info("Creating spatial index on edge.geometry")
        target_conn.execute("CREATE INDEX idx_edge_geom ON edge USING RTREE (geometry);")
        
        # Place table indexes (if table was populated)
        if place_count > 0:
            logger.info("Creating place table indexes...")
            target_conn.execute("CREATE INDEX idx_place_city_phone_state ON place(city_phone, state);")
            target_conn.execute("CREATE INDEX idx_place_zip ON place(zip);")
        
        logger.info("Conversion complete!")
        
        # Print summary statistics
        logger.info("=" * 70)
        logger.info("CONVERSION SUMMARY")
        logger.info("=" * 70)
        logger.info(f"Edge records:         {edge_count:,}")
        logger.info(f"Feature records:      {feature_count:,}")
        logger.info(f"Feature_edge links:   {feature_edge_count:,}")
        logger.info(f"Range records:        {range_count:,}")
        if place_count > 0:
            logger.info(f"Place records:        {place_count:,}")
        
        # Database size
        import os
        if os.path.exists(target_db):
            size_mb = os.path.getsize(target_db) / (1024 * 1024)
            logger.info(f"Database size:        {size_mb:.2f} MB")
        
        logger.info("=" * 70)
        
    except Exception as e:
        logger.error(f"Conversion failed: {e}")
        raise
    finally:
        target_conn.close()


def main():
    """Command-line interface for database conversion."""
    import argparse
    
    parser = argparse.ArgumentParser(
        description="Convert TIGER/Line DuckDB database to DeGAUSS format"
    )
    parser.add_argument(
        "--source",
        required=True,
        help="Source database file (geocoder.duckdb)"
    )
    parser.add_argument(
        "--target",
        required=True,
        help="Target database file (degauss.duckdb)"
    )
    parser.add_argument(
        "--state",
        default=None,
        help="State FIPS code to filter (e.g., '13' for Georgia)"
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=10000,
        help="Batch size for processing (default: 10000)"
    )
    
    args = parser.parse_args()
    
    convert_to_degauss(
        source_db=args.source,
        target_db=args.target,
        state_fips=args.state,
        batch_size=args.batch_size
    )


if __name__ == "__main__":
    main()
