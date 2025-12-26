import unittest
from tiger_utils.utils import logger

class TestLoggerModule(unittest.TestCase):
    def test_get_logger(self):
        log = logger.get_logger("test_logger_module")
        self.assertIsNotNone(log)
        log.info("Logger module test message")

if __name__ == "__main__":
    unittest.main()
