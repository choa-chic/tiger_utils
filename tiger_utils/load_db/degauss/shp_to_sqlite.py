"""
shp_to_sqlite.py - Python equivalent of the C shp2sqlite tool.

Loads ESRI Shapefiles and DBF files into SQLite tables with WKB geometry.
Uses fiona for shapefile reading and shapely for geometry handling.

Key differences from C shp2sqlite:
- Uses WKB (Well-Known Binary) format for geometries instead of PostGIS/hwgeom format
- Handles both .shp (geometry) and .dbf (attributes only) files
- Supports batch inserts for performance
- Provides progress tracking
"""

import sqlite3
from pathlib import Path
from typing import Optional, Dict, List, Tuple, Any
import warnings

try:
    import fiona
    from shapely.geometry import shape, mapping
    from shapely import wkb
except ImportError:
    raise ImportError(
        "shp_to_sqlite requires: pip install fiona shapely"
    )

from tiger_utils.utils.logger import get_logger

logger = get_logger()


class ShapefileToSQLiteConverter:
    """Convert ESRI Shapefiles to SQLite tables with WKB geometry."""

    def __init__(
        self,
        db_path: str,
        table_name: str,
        batch_size: int = 1000,
        geometry_column: str = "the_geom",
        simple_geometries: bool = False,
        append_mode: bool = False,
        drop_table: bool = False,
    ):
        """
        Initialize converter.

        Args:
            db_path: Path to SQLite database
            table_name: Target table name
            batch_size: Number of records per batch insert
            geometry_column: Name of geometry column (default: "the_geom")
            simple_geometries: If True, convert MULTI* to single geom types
            append_mode: If True, append to existing table (skip CREATE TABLE)
            drop_table: If True, drop and recreate table
        """
        self.db_path = str(db_path)
        self.table_name = table_name
        self.batch_size = batch_size
        self.geometry_column = geometry_column
        self.simple_geometries = simple_geometries
        self.append_mode = append_mode
        self.drop_table = drop_table
        self.conn = None
        self.has_geometry = False
        self.field_types = {}

    def connect(self) -> None:
        """Connect to SQLite database."""
        self.conn = sqlite3.connect(self.db_path)
        self.conn.execute("PRAGMA temp_store=MEMORY;")
        self.conn.execute("PRAGMA journal_mode=WAL;")
        self.conn.execute("PRAGMA synchronous=NORMAL;")
        self.conn.execute("PRAGMA cache_size=500000;")
        self.conn.execute("PRAGMA count_changes=0;")

    def close(self) -> None:
        """Close database connection."""
        if self.conn:
            self.conn.commit()
            self.conn.close()
            self.conn = None

    def load_shapefile(self, shp_path: str) -> None:
        """
        Load a shapefile into SQLite.

        Args:
            shp_path: Path to .shp file
        """
        self.connect()
        try:
            with fiona.open(shp_path) as src:
                self.has_geometry = True
                self._process_records(src, shp_path)
        finally:
            self.close()

    def load_dbf_only(self, dbf_path: str) -> None:
        """
        Load a DBF file (attributes only, no geometry).

        Args:
            dbf_path: Path to .dbf file
        """
        self.connect()
        try:
            with fiona.open(dbf_path) as src:
                self.has_geometry = False
                self._process_records(src, dbf_path)
        finally:
            self.close()

    def _process_records(self, src: fiona.Collection, source_path: str) -> None:
        """
        Process records from shapefile or DBF.

        Args:
            src: Fiona collection
            source_path: Path to source file (for logging)
        """
        # Extract field information
        schema = src.schema
        self.field_types = {prop[0]: prop[1] for prop in schema["properties"].items()}

        # Create table if needed
        if not self.append_mode:
            if self.drop_table:
                self._drop_table()
            self._create_table(schema)

        # Batch insert records
        batch = []
        for idx, record in enumerate(src):
            batch.append(record)
            if len(batch) >= self.batch_size:
                self._insert_batch(batch)
                batch = []
                logger.debug(f"Inserted {idx + 1} records from {source_path}")

        # Insert remaining records
        if batch:
            self._insert_batch(batch)
            logger.info(
                f"Completed loading {idx + 1} records from {Path(source_path).name}"
            )

    def _drop_table(self) -> None:
        """Drop table if it exists."""
        self.conn.execute(f'DROP TABLE IF EXISTS "{self.table_name}";')
        self.conn.commit()
        logger.debug(f"Dropped table {self.table_name}")

    def _create_table(self, schema: Dict[str, Any]) -> None:
        """
        Create table based on shapefile schema.

        Args:
            schema: Fiona schema dictionary
        """
        columns = []

        # Add geometry column first if present
        if self.has_geometry:
            columns.append(f'"{self.geometry_column}" BLOB')

        # Add attribute columns
        for prop_name, prop_type in schema["properties"].items():
            sql_type = self._fiona_type_to_sqlite(prop_type)
            # Escape column names
            safe_name = self._escape_column_name(prop_name)
            columns.append(f'"{safe_name}" {sql_type}')

        col_def = ", ".join(columns)
        create_stmt = f'CREATE TABLE "{self.table_name}" ({col_def});'

        self.conn.execute(create_stmt)
        self.conn.commit()
        logger.debug(f"Created table {self.table_name}")

    def _insert_batch(self, records: List[Dict[str, Any]]) -> None:
        """
        Insert batch of records.

        Args:
            records: List of records (GeoJSON-like dictionaries)
        """
        if not records:
            return

        # Build column list
        columns = []
        if self.has_geometry:
            columns.append(f'"{self.geometry_column}"')

        # Get columns from first record's properties
        for key in records[0]["properties"].keys():
            safe_name = self._escape_column_name(key)
            columns.append(f'"{safe_name}"')

        col_list = ", ".join(columns)
        placeholders = ", ".join(["?" for _ in columns])
        insert_stmt = f'INSERT INTO "{self.table_name}" ({col_list}) VALUES ({placeholders})'

        # Prepare data
        rows = []
        for record in records:
            row = []

            # Add geometry as WKB if present
            if self.has_geometry:
                geom = shape(record["geometry"])
                if self.simple_geometries:
                    geom = self._simplify_geometry(geom)
                geom_wkb = geom.wkb
                row.append(geom_wkb)

            # Add properties in order
            for key in records[0]["properties"].keys():
                value = record["properties"].get(key)
                row.append(value)

            rows.append(tuple(row))

        self.conn.executemany(insert_stmt, rows)

    def _simplify_geometry(self, geom):
        """
        Convert MULTI* geometries to single types if requested.

        Args:
            geom: Shapely geometry

        Returns:
            Simplified geometry (first geom if MULTI, otherwise unchanged)
        """
        if geom.geom_type.startswith("Multi"):
            # Return first geometry from collection
            return list(geom.geoms)[0]
        return geom

    @staticmethod
    def _escape_column_name(name: str) -> str:
        """
        Escape column names, handling reserved words and special cases.

        Args:
            name: Original column name

        Returns:
            Escaped column name (lowercase unless starts with _)
        """
        # Reserved words and special cases to avoid
        reserved = {
            "gid",
            "tableoid",
            "cmax",
            "xmax",
            "cmin",
            "xmin",
            "primary",
            "select",
            "from",
            "where",
        }

        # Lowercase unless starts with underscore or quote identifiers
        if not name.startswith("_"):
            name = name.lower()

        # Prefix reserved words with _
        if name.lower() in reserved:
            name = f"_{name}"

        return name

    @staticmethod
    def _fiona_type_to_sqlite(fiona_type: str) -> str:
        """
        Convert Fiona type to SQLite type.

        Args:
            fiona_type: Fiona property type string

        Returns:
            SQLite type string
        """
        # Parse fiona type strings like "int:10", "str:100", etc.
        base_type = fiona_type.split(":")[0].lower()

        type_map = {
            "int": "INTEGER",
            "float": "REAL",
            "bool": "BOOLEAN",
            "str": "VARCHAR(255)",
            "date": "DATE",
            "time": "TIME",
            "datetime": "DATETIME",
        }

        # Handle int with width (e.g., "int:8")
        if fiona_type.startswith("int:"):
            return "INTEGER"

        # Handle str with width (e.g., "str:100")
        if fiona_type.startswith("str:"):
            try:
                width = int(fiona_type.split(":")[1])
                return f"VARCHAR({width})"
            except (ValueError, IndexError):
                return "VARCHAR(255)"

        return type_map.get(base_type, "TEXT")


def convert_shapefile(
    shp_path: str,
    db_path: str,
    table_name: str,
    append: bool = False,
    geometry_column: str = "the_geom",
    simple_geometries: bool = False,
    dbf_only: bool = False,
    drop_table: bool = False,
) -> None:
    """
    High-level function to convert shapefile to SQLite.

    Args:
        shp_path: Path to shapefile (.shp or .dbf)
        db_path: Path to SQLite database
        table_name: Target table name
        append: If True, append to existing table
        geometry_column: Name of geometry column
        simple_geometries: If True, convert MULTI* geometries to simple
        dbf_only: If True, read .dbf file only (no geometry)
        drop_table: If True, drop and recreate table
    """
    converter = ShapefileToSQLiteConverter(
        db_path=db_path,
        table_name=table_name,
        geometry_column=geometry_column,
        simple_geometries=simple_geometries,
        append_mode=append,
        drop_table=drop_table,
    )

    if dbf_only:
        converter.load_dbf_only(shp_path)
    else:
        converter.load_shapefile(shp_path)
