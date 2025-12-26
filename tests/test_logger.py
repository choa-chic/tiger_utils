import unittest
from tiger_utils.utils import logger

class TestLogger(unittest.TestCase):
    def test_logger_get_logger(self):
        log = logger.get_logger("test")
        self.assertIsNotNone(log)
        log.info("Logger test message")

if __name__ == "__main__":
    unittest.main()
