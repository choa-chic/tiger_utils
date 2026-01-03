"""
parquet_to_degauss_duckdb.py - ETL: Convert unioned TIGER/Line Parquet files to DeGAUSS-format DuckDB

- Reads unioned Parquet files (edges, featnames, addr) from DuckDB views
- Materializes minimal columns into permanent tables (which are subsequently dropped)
- Transforms and loads into a new DuckDB with DeGAUSS geocoder schema (edge, feature, feature_edge, range, place)
- Computes metaphones, joins, and geometry conversion as needed


-- EDGE stpres the line geometries and their IDs (tlid).
-- FEATURE stores the  name(s) with metaphones and zip(s) of each edge.
-- FEATURE_EDGE links features to edges.
-- RANGE stores the from/to house number ranges for each edge.

Usage:
    python -m tiger_utils.load_db.degauss.parquet_to_degauss_duckdb --year 2021

    python parquet_to_degauss_duckdb.py --parquet-db path/to/unioned.duckdb --output-db path/to/degauss.duckdb
"""
import duckdb
import argparse
from pathlib import Path
import jellyfish
import sys
from tiger_utils.utils.logger import get_logger

logger = get_logger()

# --- Utility: Metaphone (DeGAUSS uses 5-char metaphone)
def metaphone5(s):
    return jellyfish.metaphone(s)[:5] if s else ""

# --- Schema for DeGAUSS geocoder ---
"""
For DuckDB - Specifying the length for the VARCHAR, STRING, and TEXT types is not required and has no effect on the system. 
Specifying the length will not improve performance or reduce storage space of the strings in the database. 
"""
DEGAUSS_SCHEMA = [
    "CREATE TABLE IF NOT EXISTS edge (tlid INT PRIMARY KEY, geometry BLOB)",
    "CREATE TABLE IF NOT EXISTS feature (fid INT PRIMARY KEY, street TEXT, street_phone TEXT, paflag BOOLEAN, zip TEXT)",
    "CREATE TABLE IF NOT EXISTS feature_edge (fid INT, tlid INT)",
    "CREATE TABLE IF NOT EXISTS range (tlid INT, fromhn TEXT, tohn TEXT, prenum TEXT, zip TEXT, side TEXT)",
    # place is loaded from prebuilt SQL inserts
]

def find_available_years(base_dir: Path) -> list:
    db_root = base_dir / "database" / "parquet"
    if not db_root.exists():
        return []
    return [d.name for d in db_root.iterdir() if d.is_dir() and (d / "tiger_line.duckdb").exists()]

def get_default_paths(base_dir: Path, year: str):
    pq_db = base_dir / "database" / "parquet" / year / "tiger_line.duckdb"
    out_db = base_dir / "database" / "parquet" / year / f"degauss_{year}.duckdb"
    return pq_db, out_db

def duckdb_optimization(con_out, parquet_db):
    """
    Apply DuckDB optimizations if needed.
    Currently a placeholder for future optimizations.
    """
    compression_pragmas = [
        "PRAGMA storage_compression='zstd'",
        "PRAGMA default_compression='zstd'",
        "PRAGMA parquet_compression='zstd'",
    ]
    for p in compression_pragmas:
        try:
            con_out.execute(p)
            logger.info("Enabled DB compression via: %s", p)
            break
        except Exception:
            logger.debug("Compression pragma not supported: %s", p)
    # Attach the source DuckDB so we can reference its views/tables as src.*
    con_out.execute(f"ATTACH DATABASE '{parquet_db}' AS src")

    # Tune DuckDB for parallel work
    try:
        con_out.execute("PRAGMA threads=%d" % (max(1, duckdb.default_connection().execute('SELECT 1').fetchone()[0])))
    except Exception:
        # ignore if threads pragma not available or this call fails
        pass

def main(parquet_db, output_db):
    con_out = duckdb.connect(str(output_db))
    # Try to enable on-disk compression for DuckDB if supported (best-effort)
    
    duckdb_optimization(con_out, parquet_db)

    logger.info("Materializing parquet-derived tables from %s", parquet_db)
    # Materialize minimal columns into temporary permanent tables in the output DB
    con_out.execute("DROP TABLE IF EXISTS pq_addr")
    con_out.execute("CREATE TABLE pq_addr AS SELECT tlid, zip, fromhn, tohn, side FROM src.tiger_addr")
    con_out.execute("CREATE OR REPLACE INDEX idx_addr_tlid ON pq_addr(tlid)")

    con_out.execute("DROP TABLE IF EXISTS pq_featnames")
    con_out.execute("CREATE TABLE pq_featnames AS SELECT tlid, fullname, paflag FROM src.tiger_featnames")
    con_out.execute("CREATE OR REPLACE INDEX idx_featnames_tlid ON pq_featnames(tlid)")
    
    con_out.execute("DROP TABLE IF EXISTS pq_edges")
    con_out.execute("CREATE TABLE pq_edges AS SELECT tlid, geometry FROM src.tiger_edges")
    con_out.execute("CREATE OR REPLACE INDEX idx_edges_tlid ON pq_edges(tlid)")

    # Build distinct feature candidates (street, paflag, zip) using SQL set operations
    logger.info("Extracting distinct street names for metaphone computation")
    con_out.execute("DROP TABLE IF EXISTS feat_candidates")
    con_out.execute(
        "CREATE TABLE feat_candidates AS SELECT f.fullname AS street, f.paflag AS paflag, a.zip AS zip " \
        "FROM pq_featnames f " \
        "LEFT JOIN pq_addr a USING (tlid)"
    )

    # Pull distinct streets into pandas to compute metaphone vectorized
    df = con_out.execute("SELECT street, paflag, zip FROM feat_candidates").fetchdf().drop_duplicates()
    if not df.empty:
        df["street_phone"] = df["street"].fillna("").apply(metaphone5)
    else:
        df["street_phone"] = []

    # Register DataFrame and create feature_distinct table
    con_out.register("_df_feat", df)
    con_out.execute("DROP TABLE IF EXISTS feature_distinct")
    con_out.execute("CREATE TABLE feature_distinct AS SELECT * FROM _df_feat")

    # Create feature table with generated fid using window function
    con_out.execute("DROP TABLE IF EXISTS feature")
    con_out.execute(
        "CREATE TABLE feature AS SELECT row_number() OVER () AS fid, street, street_phone, paflag, zip FROM feature_distinct"
    )

    # Build feature_edge via SQL joins
    logger.info("Building feature_edge table via SQL joins")
    con_out.execute("DROP TABLE IF EXISTS feature_edge")
    con_out.execute(
        "CREATE TABLE feature_edge AS SELECT f.fid, t.tlid FROM pq_featnames t JOIN feature_distinct d ON t.fullname = d.street JOIN feature f ON f.street = d.street AND (f.zip = d.zip OR (f.zip IS NULL AND d.zip IS NULL))"
    )

    # Create range table directly from pq_addr
    con_out.execute("DROP TABLE IF EXISTS range")
    con_out.execute("CREATE TABLE range AS SELECT tlid, fromhn, tohn, side, zip FROM pq_addr")

    # Create edge table from pq_edges
    con_out.execute("DROP TABLE IF EXISTS edge")
    con_out.execute("CREATE TABLE edge AS SELECT tlid, geometry FROM pq_edges")

    # Load place table SQL (prebuilt inserts)
    try:
        place_sql_path = Path(__file__).resolve().parent / "sql" / "place.sql"
        if place_sql_path.exists():
            logger.info("Loading place table from %s", place_sql_path)
            sql_text = place_sql_path.read_text(encoding="utf-8")
            try:
                con_out.execute(sql_text)
            except Exception:
                # Fall back to executing individual statements
                for stmt in [s.strip() for s in sql_text.split(';') if s.strip()]:
                    try:
                        con_out.execute(stmt)
                    except Exception:
                        logger.debug("Skipping place SQL statement due to error")
        else:
            logger.debug("place.sql not found at %s", place_sql_path)
    except Exception as e:
        logger.error("Failed to load place.sql: %s", e)

    con_out.commit()
    logger.info("DeGAUSS-format DuckDB created at %s", output_db)

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
    for name, sql in indexes:
        try:
            logger.info("Creating index %s", name)
            con_out.execute(sql)
        except Exception as e:
            logger.debug("Failed to create index %s: %s", name, e)

    # Clean up intermediate/materialized tables to save space
    try:
        logger.info("Dropping intermediate tables to reclaim space")
        con_out.execute("DROP TABLE IF EXISTS pq_addr")
        con_out.execute("DROP TABLE IF EXISTS pq_featnames")
        con_out.execute("DROP TABLE IF EXISTS pq_edges")
        con_out.execute("DROP TABLE IF EXISTS feat_candidates")
        con_out.execute("DROP TABLE IF EXISTS feature_distinct")
        # Attempt to unregister the temporary DataFrame registration if supported
        try:
            con_out.unregister("_df_feat")
        except Exception:
            # ignore if unregister not available or fails
            pass
        con_out.commit()
        logger.info("Intermediate tables dropped")
    except Exception as e:
        logger.debug("Failed to drop intermediate tables: %s", e)

    # Run ANALYZE and VACUUM to optimize the database
    try:
        logger.info("Running ANALYZE on database to refresh statistics")
        con_out.execute("ANALYZE")
    except Exception as e:
        logger.debug("ANALYZE failed: %s", e)

    try:
        logger.info("Running VACUUM to compact the database file")
        con_out.execute("VACUUM")
    except Exception as e:
        logger.debug("VACUUM failed: %s", e)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Convert unioned TIGER/Line Parquet DuckDB to DeGAUSS-format DuckDB")
    parser.add_argument("--parquet-db", help="Path to DuckDB with unioned Parquet views (tiger_edges, tiger_featnames, tiger_addr)")
    parser.add_argument("--output-db", help="Path to output DeGAUSS-format DuckDB")
    parser.add_argument("--year", help="Year to use if not specifying --parquet-db")
    args = parser.parse_args()

    base_dir = Path(__file__).resolve().parents[3]  # root of workspace
    years = find_available_years(base_dir)
    if not args.parquet_db:
        if not years:
            logger.error("No available years found under database/parquet/YYYY/tiger_line.duckdb")
            sys.exit(1)
        if not args.year:
            if len(years) == 1:
                year = years[0]
            else:
                logger.error("Multiple years found: %s. Please specify --year.", ", ".join(years))
                sys.exit(1)
        else:
            year = args.year
            if year not in years:
                logger.error("Year %s not found. Available: %s", year, ", ".join(years))
                sys.exit(1)
        pq_db, out_db = get_default_paths(base_dir, year)
    else:
        pq_db = Path(args.parquet_db)
        if args.output_db:
            out_db = Path(args.output_db)
        else:
            # Try to infer year from path
            parts = pq_db.parts
            try:
                year = next(p for p in parts if p.isdigit() and len(p) == 4)
            except StopIteration:
                logger.error("Could not infer year from parquet-db path. Please specify --output-db.")
                sys.exit(1)
            out_db = pq_db.parent / f"degauss_{year}.duckdb"
    main(pq_db, out_db)
