import unittest

from utils.summary import _extract_paths_from_name_status, _select_patch_candidate_paths


class SummaryHelperTests(unittest.TestCase):
    def test_extract_paths_prefers_new_path_on_rename(self):
        rows = [
            "M\tsrc/main.py",
            "R100\told/path.py\tnew/path.py",
        ]
        self.assertEqual(_extract_paths_from_name_status(rows), ["src/main.py", "new/path.py"])

    def test_patch_candidates_skip_large_non_version_files(self):
        rows = [
            ("src/app.py", 1201, 0),
            ("Dockerfile", 1500, 0),
            ("src/small.py", 10, 2),
        ]
        self.assertEqual(_select_patch_candidate_paths(rows, 10), ["Dockerfile", "src/small.py"])


if __name__ == "__main__":
    unittest.main()
