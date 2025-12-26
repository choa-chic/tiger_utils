import unittest
import importlib

MODULES = [
    "tiger_utils",
    "tiger_utils.tiger_cli",
    "tiger_utils.download.discover",
    "tiger_utils.download.downloader",
    "tiger_utils.download.progress_manager",
    "tiger_utils.download.url_patterns",
    "tiger_utils.load_db.duckdb",
    "tiger_utils.load_db.postgis",
    "tiger_utils.load_db.sqlite",
    "tiger_utils.utils.logger",
]

class TestModuleImports(unittest.TestCase):
    def test_imports(self):
        for module in MODULES:
            with self.subTest(module=module):
                try:
                    importlib.import_module(module)
                except Exception as e:
                    self.fail(f"Failed to import {module}: {e}")

if __name__ == "__main__":
    unittest.main()
