from pathlib import Path
import sys
import os

db_path = Path((__file__)).parent.parent / "database"

# Ensure the database directory exists
os.makedirs(db_path, exist_ok=True)

YEAR_DEFAULT = 2021

db_degauss_path = db_path / "parquet" / str(YEAR_DEFAULT)

db_duck_dg_source = db_degauss_path / f"degauss_{YEAR_DEFAULT}.duckdb"
db_sqlite_dg_target = db_degauss_path / f"geocoder_{YEAR_DEFAULT}.db"