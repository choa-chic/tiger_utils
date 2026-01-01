"""
Convert TIGER/Line DuckDB database to DeGAUSS format.

This script converts the raw TIGER/Line tables (addr, edges, featnames) 
from geocoder.duckdb into the optimized format expected by DeGAUSS geocoder.

The DeGAUSS format includes:
- Combined address range and geometry data
- Pre-computed metaphone phonetic codes for street names
- Optimized schema for fast geocoding lookups
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
    Create the DeGAUSS database schema.
    
    Args:
        conn: DuckDB connection to degauss database
    """
    logger.info("Creating DeGAUSS schema...")
    
    # Main geocoding table combining edges + addresses
    conn.execute("""
        CREATE TABLE IF NOT EXISTS geocoder (
            tlid BIGINT,
            statefp VARCHAR(2),
            countyfp VARCHAR(3),
            -- Street name components
            fullname VARCHAR(255),
            name VARCHAR(255),
            predirabrv VARCHAR(10),
            pretypabrv VARCHAR(50),
            suftypabrv VARCHAR(50),
            sufdirabrv VARCHAR(10),
            -- Metaphone codes for fuzzy matching
            name_metaphone VARCHAR(10),
            fullname_metaphone VARCHAR(10),
            -- Address ranges
            lfromadd VARCHAR(12),
            ltoadd VARCHAR(12),
            rfromadd VARCHAR(12),
            rtoadd VARCHAR(12),
            -- ZIP codes
            zipl VARCHAR(5),
            zipr VARCHAR(5),
            -- Geometry
            geometry GEOMETRY,
            -- Computed fields
            lat DOUBLE,
            lon DOUBLE,
            PRIMARY KEY (tlid)
        );
    """)
    
    # Street name variations table (from featnames)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS street_names (
            tlid BIGINT,
            fullname VARCHAR(255),
            name VARCHAR(255),
            predirabrv VARCHAR(10),
            pretypabrv VARCHAR(50),
            suftypabrv VARCHAR(50),
            sufdirabrv VARCHAR(10),
            name_metaphone VARCHAR(10),
            fullname_metaphone VARCHAR(10),
            PRIMARY KEY (tlid, fullname)
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
        # Step 1: Populate main geocoder table
        logger.info("Populating geocoder table from edges...")
        
        # Insert edges data (one row per tlid, deduplicating)
        target_conn.execute(f"""
            INSERT INTO geocoder
            SELECT 
                tlid,
                statefp,
                countyfp,
                fullname,
                fullname as name,
                '' as predirabrv,
                '' as pretypabrv,
                '' as suftypabrv,
                '' as sufdirabrv,
                compute_metaphone(fullname, 5) as name_metaphone,
                compute_metaphone(fullname, 5) as fullname_metaphone,
                lfromadd,
                ltoadd,
                rfromadd,
                rtoadd,
                zipl,
                zipr,
                geom as geometry,
                ST_Y(ST_Centroid(geom)) as lat,
                ST_X(ST_Centroid(geom)) as lon
            FROM (
                SELECT DISTINCT ON (tlid)
                    tlid, statefp, countyfp, fullname, 
                    lfromadd, ltoadd, rfromadd, rtoadd,
                    zipl, zipr, geom
                FROM source_db.main.edges
                {where_clause}
                ORDER BY tlid
            ) deduplicated
        """)
        
        geocoder_count = target_conn.execute("SELECT COUNT(*) FROM geocoder").fetchone()[0]
        logger.info(f"Inserted {geocoder_count:,} records into geocoder table")
        
        # Step 2: Populate street_names table from featnames
        logger.info("Populating street_names table from featnames...")
        
        where_clause_featnames = f"WHERE tlid IN (SELECT tlid FROM geocoder)" if state_fips else ""
        
        target_conn.execute(f"""
            INSERT INTO street_names
            SELECT DISTINCT
                f.tlid,
                f.fullname,
                f.name,
                f.predirabrv,
                f.pretypabrv,
                f.suftypabrv,
                f.sufdirabrv,
                compute_metaphone(f.name, 5) as name_metaphone,
                compute_metaphone(f.fullname, 5) as fullname_metaphone
            FROM source_db.main.featnames f
            {where_clause_featnames}
        """)
        
        names_count = target_conn.execute("SELECT COUNT(*) FROM street_names").fetchone()[0]
        logger.info(f"Inserted {names_count:,} records into street_names table")
        
        # Step 3: Create indexes for performance
        logger.info("Creating indexes...")
        
        indexes = [
            ("idx_geocoder_state_county", "geocoder", ["statefp", "countyfp"]),
            ("idx_geocoder_name_metaphone", "geocoder", ["name_metaphone"]),
            ("idx_geocoder_fullname_metaphone", "geocoder", ["fullname_metaphone"]),
            ("idx_geocoder_zip", "geocoder", ["zipl", "zipr"]),
            ("idx_geocoder_tlid", "geocoder", ["tlid"]),
            ("idx_street_names_tlid", "street_names", ["tlid"]),
            ("idx_street_names_metaphone", "street_names", ["name_metaphone", "fullname_metaphone"]),
        ]
        
        for idx_name, table, columns in indexes:
            cols_str = ", ".join(columns)
            logger.info(f"Creating index {idx_name} on {table}({cols_str})")
            target_conn.execute(f"CREATE INDEX {idx_name} ON {table}({cols_str});")
        
        # Create spatial index on geometry
        logger.info("Creating spatial index on geocoder.geometry")
        target_conn.execute("CREATE INDEX idx_geocoder_geom ON geocoder USING RTREE (geometry);")
        
        logger.info("Conversion complete!")
        
        # Print summary statistics
        logger.info("=" * 70)
        logger.info("CONVERSION SUMMARY")
        logger.info("=" * 70)
        logger.info(f"Geocoder records:     {geocoder_count:,}")
        logger.info(f"Street name variants: {names_count:,}")
        
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
