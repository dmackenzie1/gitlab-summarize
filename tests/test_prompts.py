import unittest

from utils.prompts import BRANCH_SUMMARY_RETURN_SPEC


class PromptSpecTests(unittest.TestCase):
    def test_branch_summary_requires_compact_bullets(self):
        self.assertIn("Exactly 3-5 bullet points total.", BRANCH_SUMMARY_RETURN_SPEC)
        self.assertIn("about 5-10 words", BRANCH_SUMMARY_RETURN_SPEC)


if __name__ == "__main__":
    unittest.main()
