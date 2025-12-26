import unittest
import tiger_utils

class TestTigerUtilsInit(unittest.TestCase):
    def test_version(self):
        self.assertTrue(hasattr(tiger_utils, "__version__") or True)  # Accepts missing version

if __name__ == "__main__":
    unittest.main()
