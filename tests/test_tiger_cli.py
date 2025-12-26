import unittest
from tiger_utils.tiger_cli import main as tiger_main

class TestTigerCli(unittest.TestCase):
    def test_main_callable(self):
        self.assertTrue(callable(tiger_main))

if __name__ == "__main__":
    unittest.main()
