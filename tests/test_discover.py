import unittest
from tiger_utils.download import discover

class TestDiscover(unittest.TestCase):
    def test_discover_functions_exist(self):
        self.assertTrue(hasattr(discover, "discover_states"))
        self.assertTrue(callable(discover.discover_states))
        self.assertTrue(hasattr(discover, "discover_counties"))
        self.assertTrue(callable(discover.discover_counties))

if __name__ == "__main__":
    unittest.main()
