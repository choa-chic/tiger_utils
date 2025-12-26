import unittest
from tiger_utils.download import downloader

class TestDownloader(unittest.TestCase):
    def test_downloader_class_exists(self):
        self.assertTrue(hasattr(downloader, "Downloader"))
        self.assertTrue(callable(downloader.Downloader))

if __name__ == "__main__":
    unittest.main()
