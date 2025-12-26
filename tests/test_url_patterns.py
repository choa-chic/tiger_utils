import unittest
from tiger_utils.download import url_patterns

class TestUrlPatterns(unittest.TestCase):
    def test_url_patterns_dict(self):
        self.assertIsInstance(url_patterns.URL_PATTERNS, dict)
        self.assertIn("state", url_patterns.URL_PATTERNS)

if __name__ == "__main__":
    unittest.main()
