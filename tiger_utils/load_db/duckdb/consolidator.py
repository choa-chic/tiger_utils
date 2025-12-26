"""
consolidator.py
Consolidates per-county TIGER/Line tables into unified geocoding-optimized tables.
"""
import duckdb
from pathlib import Path
from tiger_utils.utils.logger import get_logger

def consolidate_tables(db_path: str, drop_source_tables: bool = False, table_types_to_consolidate=None):
    """
    Consolidate per-county TIGER/Line tables into unified tables for geocoding.
    
    Args:
        db_path: Path to DuckDB database
        drop_source_tables: If True, drop per-county tables after consolidation
        table_types_to_consolidate: List of table types to consolidate (default: ['edges', 'featnames', 'addr'])
    """
    logger = get_logger()
    logger.info(f"Starting table consolidation for {db_path}")
    
    con = duckdb.connect(db_path)
    con.execute("INSTALL spatial;")
    con.execute("LOAD spatial;")
    # Only process selected table types
    all_table_types = {
        'edges': ['statefp', 'countyfp', 'tlid', 'fullname', 'lfromadd', 'ltoadd', 
                  'rfromadd', 'rtoadd', 'zipl', 'zipr', 'geometry'],
        'featnames': ['tlid', 'fullname', 'name', 'predirabrv', 'pretypabrv', 
                      'suftypabrv', 'sufdirabrv'],
        'addr': ['tlid', 'fromhn', 'tohn', 'side', 'zip', 'plus4']
    }
    if table_types_to_consolidate is None:
        table_types_to_consolidate = ['edges', 'featnames', 'addr']
    table_types = {k: v for k, v in all_table_types.items() if k in table_types_to_consolidate}
    
    for table_type, expected_cols in table_types.items():
        logger.info(f"Consolidating {table_type} tables...")
        
        # Find all per-county tables of this type
        query = f"""
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_name LIKE 'tl_%_{table_type}'
            ORDER BY table_name
        """
        source_tables = [row[0] for row in con.execute(query).fetchall()]
        
        if not source_tables:
            logger.warning(f"No {table_type} tables found to consolidate")
            continue
            
        logger.info(f"Found {len(source_tables)} {table_type} tables to consolidate")
        
        # Create consolidated table from first source table
        consolidated_name = table_type
        first_table = source_tables[0]
        
        # Check if consolidated table already exists
        exists = con.execute(
            f"SELECT 1 FROM information_schema.tables WHERE table_name = '{consolidated_name}'"
        ).fetchone()
        
        if exists:
            logger.info(f"Table {consolidated_name} already exists, appending data...")
        else:
            logger.info(f"Creating consolidated table {consolidated_name} from {first_table}")
            con.execute(f"CREATE TABLE {consolidated_name} AS SELECT * FROM {first_table};")
            source_tables = source_tables[1:]  # Skip first since we used it to create table
        
        # Insert data from remaining tables
        for source_table in source_tables:
            logger.info(f"Inserting data from {source_table} into {consolidated_name}")
            try:
                con.execute(f"INSERT INTO {consolidated_name} SELECT * FROM {source_table};")
            except Exception as e:
                logger.error(f"Failed to insert from {source_table}: {e}")
                continue
        
        # Get row count
        count = con.execute(f"SELECT COUNT(*) FROM {consolidated_name}").fetchone()[0]
        logger.info(f"Consolidated table {consolidated_name} has {count:,} rows")
        
        # Drop source tables if requested
        if drop_source_tables:
            for source_table in source_tables:
                logger.info(f"Dropping source table {source_table}")
                con.execute(f"DROP TABLE IF EXISTS {source_table};")
    
    # Create indexes for geocoding performance
    logger.info("Creating indexes for geocoding performance...")
    
    indexes = [
        # edges indexes
        ("idx_edges_state_county", "edges", ["statefp", "countyfp"]),
        ("idx_edges_fullname", "edges", ["fullname"]),
        ("idx_edges_zipl", "edges", ["zipl"]),
        ("idx_edges_zipr", "edges", ["zipr"]),
        ("idx_edges_tlid", "edges", ["tlid"]),
        # featnames indexes
        ("idx_featnames_tlid", "featnames", ["tlid"]),
        ("idx_featnames_name", "featnames", ["name"]),
        ("idx_featnames_fullname", "featnames", ["fullname"]),
        # addr indexes
        ("idx_addr_tlid", "addr", ["tlid"]),
        ("idx_addr_zip", "addr", ["zip"]),
    ]
    
    for idx_name, table_name, columns in indexes:
        # Check if table exists
        table_exists = con.execute(
            f"SELECT 1 FROM information_schema.tables WHERE table_name = '{table_name}'"
        ).fetchone()
        
        if not table_exists:
            logger.warning(f"Table {table_name} does not exist, skipping index {idx_name}")
            continue
            
        # Check if index already exists
        try:
            con.execute(f"DROP INDEX IF EXISTS {idx_name};")
        except:
            pass
            
        cols_str = ", ".join(columns)
        logger.info(f"Creating index {idx_name} on {table_name}({cols_str})")
        try:
            con.execute(f"CREATE INDEX {idx_name} ON {table_name}({cols_str});")
        except Exception as e:
            logger.error(f"Failed to create index {idx_name}: {e}")
    
    # Create spatial indexes
    logger.info("Creating spatial indexes...")
    for table_name in ['edges', 'places', 'counties', 'zcta5']:
        table_exists = con.execute(
            f"SELECT 1 FROM information_schema.tables WHERE table_name = '{table_name}'"
        ).fetchone()
        
        if not table_exists:
            logger.warning(f"Table {table_name} does not exist, skipping spatial index")
            continue
        
        # Check if geometry column exists
        has_geom = con.execute(
            f"SELECT 1 FROM information_schema.columns WHERE table_name = '{table_name}' AND column_name = 'geometry'"
        ).fetchone()
        
        if not has_geom:
            logger.warning(f"Table {table_name} has no geometry column, skipping spatial index")
            continue
            
        idx_name = f"idx_{table_name}_geom"
        logger.info(f"Creating spatial index {idx_name} on {table_name}")
        try:
            con.execute(f"DROP INDEX IF EXISTS {idx_name};")
            con.execute(f"CREATE INDEX {idx_name} ON {table_name} USING RTREE (geometry);")
        except Exception as e:
            logger.error(f"Failed to create spatial index {idx_name}: {e}")
    
    con.close()
    logger.info("Table consolidation complete!")

def main():
    import argparse
    parser = argparse.ArgumentParser(
        description="Consolidate per-county TIGER/Line tables into unified geocoding tables."
    )
    parser.add_argument(
        "--db",
        default=None,
        help="DuckDB database file (default: <project_root>/database/geocoder.duckdb)",
    )
    parser.add_argument(
        "--drop-source",
        action="store_true",
        help="Drop per-county source tables after consolidation",
    )
    parser.add_argument(
        "--tables",
        nargs="*",
        default=None,
        help="Table types to consolidate (default: edges featnames addr)",
    )
    args = parser.parse_args()
    # Determine project root and default db path
    if args.db:
        db_path = args.db
    else:
        project_root = Path(__file__).resolve().parent.parent.parent.parent
        db_path = str(project_root / "database" / "geocoder.duckdb")
    # Use user-specified or default table types
    table_types = args.tables if args.tables else None
    consolidate_tables(db_path, drop_source_tables=args.drop_source, table_types_to_consolidate=table_types)

if __name__ == "__main__":
    main()
