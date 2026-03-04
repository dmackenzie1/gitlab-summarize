import unittest

from utils.parsing import is_noisy_path


class ParsingTests(unittest.TestCase):
    def test_media_and_binary_assets_are_noisy(self):
        self.assertTrue(is_noisy_path("assets/logo.png"))
        self.assertTrue(is_noisy_path("videos/demo.mp4"))
        self.assertTrue(is_noisy_path("docs/guide.pdf"))

    def test_source_code_file_is_not_noisy(self):
        self.assertFalse(is_noisy_path("src/service/api.py"))


if __name__ == "__main__":
    unittest.main()
