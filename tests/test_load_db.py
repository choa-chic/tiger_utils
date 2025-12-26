import unittest
from tiger_utils.load_db import duckdb, postgis, sqlite

class TestLoadDbModules(unittest.TestCase):
    def test_duckdb_module(self):
        self.assertTrue(hasattr(duckdb, "DuckDBLoader"))
    def test_postgis_module(self):
        self.assertTrue(hasattr(postgis, "PostGISLoader"))
    def test_sqlite_module(self):
        self.assertTrue(hasattr(sqlite, "SQLiteLoader"))

if __name__ == "__main__":
    unittest.main()
