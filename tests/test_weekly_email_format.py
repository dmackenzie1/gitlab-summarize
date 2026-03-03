import unittest
from pathlib import Path

from utils.activity_logs import ActivitySummaryResult
from utils.summary import PipelineContext, RepoWorkItem, _clean_wrapped_hyphenation, _render_weekly_email_html


class WeeklyEmailFormatTests(unittest.TestCase):
    def test_hyphenation_cleanup(self):
        self.assertEqual(_clean_wrapped_hyphenation("his- torical da- tabase execu- tion"), "historical database execution")

    def test_combined_repo_section_and_single_title(self):
        context = PipelineContext(
            projects=[{"project_name": "emss/aegis", "ssh_url": "git@example.com:aegis.git"}],
            remote="origin",
            days=10,
            out_dir=Path("."),
            cache_dir=Path("."),
            use_temp=False,
            include_ollama=False,
            ollama_client=None,
            max_patch_chars=1000,
            max_prompt_chars=1000,
            max_files_in_patch=10,
            artifacts_root=Path("."),
            prompt_cache_dir=Path("."),
            errors_dir=Path("."),
            project_summaries_dir=Path("."),
            activity_result=ActivitySummaryResult(
                rollups_by_project_name={"emss/aegis": "- Added socket smoke test"},
                highlights_for_master=[("emss/aegis", "- Added socket smoke test")],
            ),
        )
        item = RepoWorkItem(
            item={},
            repo_display="emss/aegis",
            repo_key="emss__aegis",
            repo_dir=Path("."),
            repo_art_dir=Path("."),
            branch_rollups=[
                ("origin/feature", "- Added da- tabase migration\n- Added da- tabase migration\n- Node bumped to 20")
            ],
            activity_rollup="- Added socket smoke test",
        )
        context.repo_items = [item]
        context.branches_analyzed = 3

        html = _render_weekly_email_html(context, model="qwen", days=10)

        self.assertEqual(html.count("Weekly Engineering Summary"), 1)
        self.assertIn("Management Summary", html)
        self.assertIn("Key Changes", html)
        self.assertNotIn("da- tabase", html)
        self.assertNotIn("### Activity", html)


if __name__ == "__main__":
    unittest.main()
