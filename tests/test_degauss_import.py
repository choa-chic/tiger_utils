"""
Test for DeGAUSS-style SQLite loading with temporary tables.
"""
import unittest
import sqlite3
import tempfile
import os
from pathlib import Path

# Test that the SQL files can be loaded
class TestDeGAUSSSQLFiles(unittest.TestCase):
    
    def setUp(self):
        """Create a temporary database for testing"""
        self.temp_db = tempfile.NamedTemporaryFile(delete=False, suffix='.db')
        self.temp_db.close()
        self.db_path = self.temp_db.name
        
    def tearDown(self):
        """Clean up temporary database"""
        if os.path.exists(self.db_path):
            os.unlink(self.db_path)
    
    def test_sql_files_exist(self):
        """Test that SQL files exist in the correct location"""
        from tiger_utils.load_db.degauss import db_setup
        sql_dir = db_setup.SQL_DIR
        
        self.assertTrue((sql_dir / "create.sql").exists(), "create.sql should exist")
        self.assertTrue((sql_dir / "setup.sql").exists(), "setup.sql should exist")
        self.assertTrue((sql_dir / "convert.sql").exists(), "convert.sql should exist")
        self.assertTrue((sql_dir / "index.sql").exists(), "index.sql should exist")
    
    def test_create_schema(self):
        """Test that create_schema creates the expected tables"""
        from tiger_utils.load_db.degauss import db_setup
        
        db_setup.create_schema(self.db_path)
        
        conn = sqlite3.connect(self.db_path)
        cur = conn.cursor()
        
        # Check that all expected tables exist
        cur.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
        tables = [row[0] for row in cur.fetchall()]
        
        expected_tables = ['edge', 'feature', 'feature_edge', 'place', 'range']
        for table in expected_tables:
            self.assertIn(table, tables, f"Table {table} should be created")
        
        conn.close()
    
    def test_create_temp_tables(self):
        """Test that temporary tables SQL is valid and can be executed"""
        from tiger_utils.load_db.degauss import db_setup
        
        # Note: SQLite temporary tables only exist within the connection that creates them
        # So we test that the SQL executes successfully rather than persisting
        db_setup.create_schema(self.db_path)
        
        # This will execute the temporary table creation SQL
        # The tables won't persist after the connection closes, which is expected
        try:
            db_setup.create_temp_tables(self.db_path)
        except Exception as e:
            self.fail(f"Failed to create temporary tables: {e}")
    
    def test_create_indexes(self):
        """Test that indexes are created"""
        from tiger_utils.load_db.degauss import db_setup
        
        db_setup.create_schema(self.db_path)
        db_setup.create_indexes(self.db_path)
        
        conn = sqlite3.connect(self.db_path)
        cur = conn.cursor()
        
        # Check that indexes exist
        cur.execute("SELECT name FROM sqlite_master WHERE type='index' ORDER BY name")
        indexes = [row[0] for row in cur.fetchall()]
        
        expected_indexes = [
            'feature_edge_fid_idx',
            'feature_street_phone_zip_idx',
            'place_city_phone_state_idx',
            'place_zip_priority_idx',
            'range_tlid_idx'
        ]
        
        for index in expected_indexes:
            self.assertIn(index, indexes, f"Index {index} should be created")
        
        conn.close()
    
    def test_sql_file_syntax(self):
        """Test that SQL files have valid syntax by executing them"""
        from tiger_utils.load_db.degauss import db_setup
        sql_dir = db_setup.SQL_DIR
        
        conn = sqlite3.connect(self.db_path)
        
        # Test create.sql
        with open(sql_dir / "create.sql", 'r') as f:
            try:
                conn.executescript(f.read())
            except sqlite3.Error as e:
                self.fail(f"create.sql has invalid syntax: {e}")
        
        # Test setup.sql  
        with open(sql_dir / "setup.sql", 'r') as f:
            try:
                conn.executescript(f.read())
            except sqlite3.Error as e:
                self.fail(f"setup.sql has invalid syntax: {e}")
        
        # Test index.sql
        with open(sql_dir / "index.sql", 'r') as f:
            try:
                conn.executescript(f.read())
            except sqlite3.Error as e:
                self.fail(f"index.sql has invalid syntax: {e}")
        
        conn.close()


class TestImporterFunctions(unittest.TestCase):
    """Test importer functions exist and are callable"""
    
    def test_import_tiger_exists(self):
        """Test that import_tiger function exists"""
        from tiger_utils.load_db.degauss import importer
        self.assertTrue(hasattr(importer, 'import_tiger'))
        self.assertTrue(callable(importer.import_tiger))
    
    def test_db_setup_functions_exist(self):
        """Test that db_setup functions exist"""
        from tiger_utils.load_db.degauss import db_setup
        
        self.assertTrue(hasattr(db_setup, 'create_schema'))
        self.assertTrue(hasattr(db_setup, 'create_indexes'))
        self.assertTrue(hasattr(db_setup, 'create_temp_tables'))
        self.assertTrue(hasattr(db_setup, 'transform_temp_to_final'))
    
    def test_shp_to_temp_tables_functions_exist(self):
        """Test that shp_to_temp_tables functions exist"""
        from tiger_utils.load_db.degauss import shp_to_temp_tables
        
        self.assertTrue(hasattr(shp_to_temp_tables, 'load_edges_to_temp'))
        self.assertTrue(hasattr(shp_to_temp_tables, 'load_featnames_to_temp'))
        self.assertTrue(hasattr(shp_to_temp_tables, 'load_addr_to_temp'))
        self.assertTrue(hasattr(shp_to_temp_tables, 'load_tiger_files_to_temp'))


if __name__ == '__main__':
    unittest.main()
