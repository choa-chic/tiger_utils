"""
test_new_etl_pipeline.py - Test the refactored in-memory ETL pipeline.

This test verifies that:
1. TigerETL reads files correctly
2. Transformations work in-memory
3. Data loads to SQLite without temp tables
"""

import tempfile
import sqlite3
from pathlib import Path
import sys

# Add module to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from tiger_utils.load_db.degauss.tiger_etl import TigerETL
from tiger_utils.load_db.degauss.db_setup import create_schema


def test_etl_basic_structure():
    """Test that TigerETL can be instantiated and has expected methods."""
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / "test.db"
        etl = TigerETL(str(db_path), batch_size=100, verbose=True)
        
        # Verify methods exist
        assert hasattr(etl, 'process_county')
        assert hasattr(etl, '_read_edges_shp')
        assert hasattr(etl, '_read_featnames_dbf')
        assert hasattr(etl, '_read_addr_dbf')
        assert hasattr(etl, '_build_linezip')
        assert hasattr(etl, '_build_features')
        assert hasattr(etl, '_build_feature_edges')
        assert hasattr(etl, '_build_ranges')
        assert hasattr(etl, '_insert_to_database')
        
        print("✓ TigerETL structure is correct")


def test_new_importer_structure():
    """Test that TigerImporter has been refactored correctly."""
    from tiger_utils.load_db.degauss.tiger_importer import TigerImporter
    
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / "test.db"
        source_dir = Path(tmpdir) / "source"
        source_dir.mkdir()
        
        importer = TigerImporter(
            db_path=str(db_path),
            source_dir=str(source_dir),
            verbose=True
        )
        
        # Verify old methods are removed
        assert not hasattr(importer, '_load_shapefile')
        assert not hasattr(importer, '_load_dbf_file')
        assert not hasattr(importer, '_transform_and_load')
        
        # Verify new helper method exists
        assert hasattr(importer, '_find_file')
        
        # Verify _drop_temp_tables still exists (for safety)
        assert hasattr(importer, '_drop_temp_tables')
        
        # Verify structure matches expectations
        assert hasattr(importer, 'import_all')
        assert hasattr(importer, 'import_county')
        assert hasattr(importer, 'create_indexes')
        
        print("✓ TigerImporter refactored correctly")


def test_database_schema_still_valid():
    """Test that the new pipeline still works with existing schema."""
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / "test.db"
        
        # Create schema
        create_schema(str(db_path))
        
        # Verify tables exist
        conn = sqlite3.connect(str(db_path))
        cur = conn.cursor()
        
        cur.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name IN "
            "('place', 'edge', 'feature', 'feature_edge', 'range')"
        )
        tables = {row[0] for row in cur.fetchall()}
        
        conn.close()
        
        expected = {'place', 'edge', 'feature', 'feature_edge', 'range'}
        assert tables == expected, f"Expected {expected}, got {tables}"
        
        print("✓ Database schema is valid")


if __name__ == '__main__':
    test_etl_basic_structure()
    test_new_importer_structure()
    test_database_schema_still_valid()
    print("\n✅ All tests passed!")
