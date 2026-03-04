#!/usr/bin/env python3
import argparse
import datetime as dt
import logging
import os
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()

from utils.notifications import EmailData, send_pipeline_notification_email
from utils.ollama import OllamaClient
from utils.summary import (
    build_master_summary,
    build_project_rollup,
    build_repo_branches,
    init_context,
    load_config,
    process_activity_ranges,
    render_outputs,
    sync_repos,
)
from utils.models import RunResult

REMOTE_DEFAULT = "origin"
OLLAMA_URL_DEFAULT = os.getenv("OLLAMA_URL", "http://localhost:11434/api/generate")
OLLAMA_MODEL_DEFAULT = os.getenv("OLLAMA_MODEL", "qwen3.5:9b")

DAYS_DEFAULT = int(os.getenv("DAYS", "10"))
MAX_PROMPT_CHARS_DEFAULT = int(os.getenv("MAX_PROMPT_CHARS", "120000"))
MAX_PATCH_CHARS_DEFAULT = int(os.getenv("MAX_PATCH_CHARS", "90000"))
MAX_FILES_IN_PATCH_DEFAULT = int(os.getenv("MAX_FILES_IN_PATCH", "40"))
OLLAMA_KEEP_ALIVE_DEFAULT = os.getenv("OLLAMA_KEEP_ALIVE", "5m")
OLLAMA_TIMEOUT_DEFAULT = int(os.getenv("OLLAMA_TIMEOUT", "240"))
OLLAMA_RETRIES_DEFAULT = int(os.getenv("OLLAMA_RETRIES", "3"))
AIDER_CMD_DEFAULT = os.getenv("AIDER_CMD", "aider")
AIDER_MODEL_DEFAULT = os.getenv("AIDER_MODEL", "qwen3.5:27b")

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Weekly project summary pipeline")
    parser.add_argument("--projects", default="data/projects.json", help="Path to repositories JSON")
    parser.add_argument("--only-default", action="store_true", help="Only include entries where is_default=true")
    parser.add_argument("--days", type=int, default=DAYS_DEFAULT)

    parser.add_argument("--out-dir", default=".", help="Directory to write weeklySummary.markup outputs")
    parser.add_argument("--cache-dir", default="repo_cache")
    parser.add_argument("--temp", action="store_true")
    parser.add_argument("--offline", action="store_true", help="Use local repo cache only; skip git fetch")
    parser.add_argument("--summarizer", choices=["ollama", "aider"], default="ollama")
    parser.add_argument("--aider-cmd", default=AIDER_CMD_DEFAULT, help="aider executable path")
    parser.add_argument("--aider-model", default=AIDER_MODEL_DEFAULT, help="Model name for aider runs")

    parser.add_argument("--ollama-url", default=OLLAMA_URL_DEFAULT)
    parser.add_argument("--ollama-model", default=OLLAMA_MODEL_DEFAULT)
    parser.add_argument("--ollama-timeout", type=int, default=OLLAMA_TIMEOUT_DEFAULT)
    parser.add_argument("--ollama-retries", type=int, default=OLLAMA_RETRIES_DEFAULT)
    parser.add_argument("--ollama-keep-alive", default=OLLAMA_KEEP_ALIVE_DEFAULT, help="Ollama keep_alive value")

    parser.add_argument("--max-patch-chars", type=int, default=MAX_PATCH_CHARS_DEFAULT)
    parser.add_argument("--max-prompt-chars", type=int, default=MAX_PROMPT_CHARS_DEFAULT)
    parser.add_argument("--max-files", type=int, default=MAX_FILES_IN_PATCH_DEFAULT)
    parser.add_argument("--resummarize", action="store_true", help="Rebuild prompts and summaries from saved patch artifacts")
    return parser.parse_args()

def _build_ollama_client(args: argparse.Namespace) -> OllamaClient:
    logging.info("Initializing Ollama client")
    return OllamaClient(
        url=args.ollama_url,
        model=args.ollama_model,
        timeout_s=args.ollama_timeout,
        retries=args.ollama_retries,
        keep_alive=(args.ollama_keep_alive or None),
    )

def main() -> int:
    args = parse_args()
    start_time = dt.datetime.now(dt.timezone.utc)

    result = RunResult(
        exit_code=1,
        projects_processed=0,
        branches_analyzed=0,
        artifacts_root=Path(args.out_dir).resolve() / "artifacts",
        errors=[],
    )
    run_error = None

    try:
        logging.info("Phase 1/8: Loading repositories configuration")
        projects = load_config(Path(args.projects).resolve(), only_default=args.only_default)

        ollama_client = _build_ollama_client(args)
        context = init_context(
            projects=projects,
            remote=REMOTE_DEFAULT,
            days=args.days,
            out_dir=Path(args.out_dir).resolve(),
            cache_dir=Path(args.cache_dir).resolve(),
            use_temp=args.temp,
            offline=args.offline,
            include_ollama=(args.summarizer == "ollama"),
            ollama_client=ollama_client,
            summarizer=args.summarizer,
            aider_cmd=args.aider_cmd,
            aider_model=args.aider_model,
            max_patch_chars=args.max_patch_chars,
            max_prompt_chars=args.max_prompt_chars,
            max_files_in_patch=args.max_files,
            force_resummarize=args.resummarize,
        )

        logging.info("Phase 2/8: Syncing repositories")
        sync_repos(context)

        logging.info("Phase 3/8: Processing activity ranges")
        process_activity_ranges(context)
        for repo_item in context.repo_items:
            repo_item.activity_rollup = context.activity_result.rollups_by_project_name.get(repo_item.repo_display)

        logging.info("Phase 4/8: Building branch summaries and project rollups")
        for repo_item in context.repo_items:
            logging.info("Phase 4.1/8: build_repo_branches repo=%s", repo_item.repo_display)
            build_repo_branches(context, repo_item)
            logging.info("Phase 4.2/8: build_project_rollup repo=%s", repo_item.repo_display)
            build_project_rollup(context, repo_item)

        logging.info("Phase 5/8: Combining activity stages (already incorporated in project rollups)")
        logging.info("Phase 6/8: Building master summary")
        build_master_summary(context)
        logging.info("Phase 7/8: Rendering outputs")
        result = render_outputs(context)
        return_code = result.exit_code
    except Exception as exc:  # noqa: BLE001
        run_error = str(exc)
        logging.exception("Pipeline failed")
        return_code = 1
    finally:
        end_time = dt.datetime.now(dt.timezone.utc)
        email_data = EmailData(
            start_time=start_time,
            end_time=end_time,
            projects_processed=result.projects_processed,
            branches_analyzed=result.branches_analyzed,
            artifacts_dir=result.artifacts_root,
            error=run_error,
            warnings=result.errors,
        )
        logging.info("Phase 8/8: Sending completion email")
        send_pipeline_notification_email(email_data)

    return return_code

if __name__ == "__main__":
    raise SystemExit(main())
