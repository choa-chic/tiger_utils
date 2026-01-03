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

def build_linezip(con_out):
    """
    Build the linezip table matching the original SQLite logic:
    - For all tlid/zip pairs from tiger_addr
    - For all street edges (mtfcc LIKE 'S%') with zipr or zipl from tiger_edges
    """
    logger.info("Building linezip table (edge-to-ZIP mapping)")
    con_out.execute("DROP TABLE IF EXISTS linezip")
    con_out.execute("""
        CREATE TABLE linezip AS
        SELECT DISTINCT tlid, zip FROM pq_addr
        UNION
        SELECT DISTINCT tlid, zipr AS zip FROM src.tiger_edges
            WHERE mtfcc LIKE 'S%' AND zipr IS NOT NULL AND zipr <> ''
        UNION
        SELECT DISTINCT tlid, zipl AS zip FROM src.tiger_edges
            WHERE mtfcc LIKE 'S%' AND zipl IS NOT NULL AND zipl <> ''
    """)
    con_out.execute("CREATE INDEX IF NOT EXISTS linezip_tlid ON linezip(tlid)")

def build_feature_bin(con_out):
    """
    Build feature_bin table as in the original SQL:
    - For each edge in linezip, join to featnames, compute metaphone, and assign zip.
    """
    logger.info("Building feature_bin table (features with metaphone and zip)")
    con_out.execute("DROP TABLE IF EXISTS feature_bin")
    # Join linezip to pq_featnames, filter for non-empty names
    df = con_out.execute("""
        SELECT DISTINCT f.fullname AS street, f.paflag AS paflag, l.zip AS zip
        FROM linezip l
        JOIN pq_featnames f ON l.tlid = f.tlid
        WHERE f.fullname IS NOT NULL AND f.fullname <> ''
    """).fetchdf()
    if not df.empty:
        df["street_phone"] = df["street"].fillna("").apply(metaphone5)
    else:
        df["street_phone"] = []
    con_out.register("_df_feature_bin", df)
    con_out.execute("""
        CREATE TABLE feature_bin AS
        SELECT NULL AS fid, street, street_phone, paflag, zip FROM _df_feature_bin
    """)
    # Add index for later joins
    con_out.execute("CREATE INDEX IF NOT EXISTS feature_bin_idx ON feature_bin(street, zip)")

def build_feature_table(con_out):
    """
    Assign fid (row_number) to feature_bin and create the feature table.
    """
    logger.info("Building feature table (assigning fid)")
    con_out.execute("DROP TABLE IF EXISTS feature")
    con_out.execute("""
        CREATE TABLE feature AS
        SELECT row_number() OVER () AS fid, street, street_phone, paflag, zip
        FROM feature_bin
    """)

def build_feature_edge(con_out):
    """
    Build feature_edge table as in the original SQL:
    - Join linezip, pq_featnames, and feature_bin on tlid, zip, street, paflag.
    """
    logger.info("Building feature_edge table")
    con_out.execute("DROP TABLE IF EXISTS feature_edge")
    con_out.execute("""
        CREATE TABLE feature_edge AS
        SELECT DISTINCT f.fid, fn.tlid
        FROM linezip l
        JOIN pq_featnames fn ON l.tlid = fn.tlid
        JOIN feature_bin b ON l.zip = b.zip AND fn.fullname = b.street AND fn.paflag = b.paflag
        JOIN feature f ON b.street = f.street AND b.zip = f.zip AND b.paflag = f.paflag
    """)
    con_out.execute("CREATE INDEX IF NOT EXISTS feature_edge_fid_idx ON feature_edge(fid)")

def build_edge_table(con_out):
    """
    Insert edges for each tlid in linezip with non-empty fullname.
    """
    logger.info("Building edge table (geometry for named street edges)")
    con_out.execute("DROP TABLE IF EXISTS edge")
    con_out.execute("""
        CREATE TABLE edge AS
        SELECT e.tlid, e.geometry
        FROM (SELECT DISTINCT tlid FROM linezip) l
        JOIN pq_edges e ON l.tlid = e.tlid
        JOIN pq_featnames f ON e.tlid = f.tlid
        WHERE f.fullname IS NOT NULL AND f.fullname <> ''
    """)

def digit_suffix(s):
    """
    Extract digit suffix from a string (e.g., 'A123' -> '123').
    """
    import re
    if not s:
        return ""
    m = re.search(r"(\d+)$", s)
    return m.group(1) if m else ""

def nondigit_prefix(s):
    """
    Extract non-digit prefix from a string (e.g., 'A123' -> 'A').
    """
    import re
    if not s:
        return ""
    m = re.match(r"^([^\d]*)", s)
    return m.group(1) if m else ""

def build_range_table(con_out):
    """
    Insert ranges from pq_addr, splitting house numbers into digit suffix and non-digit prefix.
    """
    logger.info("Building range table (splitting house numbers)")
    df = con_out.execute("""
        SELECT tlid, fromhn, tohn, zip, side FROM pq_addr
    """).fetchdf()
    if not df.empty:
        df["fromhn_digit"] = df["fromhn"].apply(digit_suffix)
        df["tohn_digit"] = df["tohn"].apply(digit_suffix)
        df["prenum"] = df["fromhn"].apply(nondigit_prefix)
    else:
        df["fromhn_digit"] = []
        df["tohn_digit"] = []
        df["prenum"] = []
    con_out.register("_df_range", df)
    con_out.execute("DROP TABLE IF EXISTS range")
    con_out.execute("""
        CREATE TABLE range AS
        SELECT tlid, fromhn_digit AS fromhn, tohn_digit AS tohn, prenum, zip, side FROM _df_range
    """)
    con_out.execute("CREATE INDEX IF NOT EXISTS range_tlid_idx ON range(tlid)")

def main(parquet_db, output_db):
    con_out = duckdb.connect(str(output_db))
    duckdb_optimization(con_out, parquet_db)

    logger.info("Materializing parquet-derived tables from %s", parquet_db)
    # Materialize minimal columns into temporary permanent tables in the output DB
    con_out.execute("DROP TABLE IF EXISTS pq_addr")
    con_out.execute("CREATE TABLE pq_addr AS SELECT tlid, zip, fromhn, tohn, side FROM src.tiger_addr")
    con_out.execute("CREATE INDEX IF NOT EXISTS idx_addr_tlid ON pq_addr(tlid)")

    con_out.execute("DROP TABLE IF EXISTS pq_featnames")
    con_out.execute("CREATE TABLE pq_featnames AS SELECT tlid, fullname, paflag FROM src.tiger_featnames")
    con_out.execute("CREATE INDEX IF NOT EXISTS idx_featnames_tlid ON pq_featnames(tlid)")

    con_out.execute("DROP TABLE IF EXISTS pq_edges")
    con_out.execute("CREATE TABLE pq_edges AS SELECT tlid, geometry FROM src.tiger_edges")
    con_out.execute("CREATE INDEX IF NOT EXISTS idx_edges_tlid ON pq_edges(tlid)")

    # --- Build tables matching the original SQLite logic ---
    build_linezip(con_out)
    build_feature_bin(con_out)
    build_feature_table(con_out)
    build_feature_edge(con_out)
    build_edge_table(con_out)
    build_range_table(con_out)

    # Load place table SQL (prebuilt inserts)
    try:
        place_sql_path = Path(__file__).resolve().parent / "sql" / "place.sql"
        if place_sql_path.exists():
            logger.info("Loading place table from %s", place_sql_path)
            sql_text = place_sql_path.read_text(encoding="utf-8")
            try:
                con_out.execute(sql_text)
            except Exception:
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
        con_out.execute("DROP TABLE IF EXISTS linezip")
        con_out.execute("DROP TABLE IF EXISTS feature_bin")
        try:
            con_out.unregister("_df_feature_bin")
        except Exception:
            pass
        try:
            con_out.unregister("_df_range")
        except Exception:
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
