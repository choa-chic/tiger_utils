import unittest
from tiger_utils.download import progress_manager

class TestProgressManager(unittest.TestCase):
    def test_progress_manager_class(self):
        pm = progress_manager.ProgressManager("test_progress.json")
        self.assertIsInstance(pm, progress_manager.ProgressManager)
        pm.set_status("foo", "done")
        status = pm.get_status("foo")
        self.assertEqual(status, "done")
        pm.clear()

if __name__ == "__main__":
    unittest.main()
