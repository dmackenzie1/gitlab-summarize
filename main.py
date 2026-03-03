#!/usr/bin/env python3
from __future__ import annotations

import argparse
import datetime as dt
import logging
from pathlib import Path

from utils.notifications import PipelineEmailNotification, send_pipeline_completion_email
from utils.ollama import OllamaClient
from utils.summary import (
    PipelineRunResult,
    build_master_summary,
    build_rollups,
    init_pipeline_context,
    load_config,
    process_activity_stage,
    process_repo_branches,
    render_outputs,
    sync_repos,
)

REMOTE_DEFAULT = "origin"
OLLAMA_URL_DEFAULT = "http://localhost:11434/api/generate"
OLLAMA_MODEL_DEFAULT = "qwen2.5-coder:32b"

DAYS_DEFAULT = 10
MAX_PROMPT_CHARS_DEFAULT = 120_000
MAX_PATCH_CHARS_DEFAULT = 90_000
MAX_FILES_IN_PATCH_DEFAULT = 40
OLLAMA_KEEP_ALIVE_DEFAULT = "5m"


def setup_logging() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Weekly project summary pipeline")
    parser.add_argument("--projects", default="data/monitored.json", help="Path to monitored repositories JSON")
    parser.add_argument("--only-default", action="store_true", help="Only include entries where is_default=true")
    parser.add_argument("--days", type=int, default=DAYS_DEFAULT)

    parser.add_argument("--out-dir", default=".", help="Directory to write weeklySummary.markup outputs")
    parser.add_argument("--cache-dir", default="repo_cache")
    parser.add_argument("--temp", action="store_true")

    parser.add_argument("--no-ollama", action="store_true", help="Skip LLM summarization and only produce git/activity artifacts")
    parser.add_argument("--ollama-url", default=OLLAMA_URL_DEFAULT)
    parser.add_argument("--ollama-model", default=OLLAMA_MODEL_DEFAULT)
    parser.add_argument("--ollama-timeout", type=int, default=240)
    parser.add_argument("--ollama-retries", type=int, default=3)
    parser.add_argument("--ollama-keep-alive", default=OLLAMA_KEEP_ALIVE_DEFAULT, help="Ollama keep_alive value")

    parser.add_argument("--max-patch-chars", type=int, default=MAX_PATCH_CHARS_DEFAULT)
    parser.add_argument("--max-prompt-chars", type=int, default=MAX_PROMPT_CHARS_DEFAULT)
    parser.add_argument("--max-files", type=int, default=MAX_FILES_IN_PATCH_DEFAULT)
    return parser.parse_args()


def _build_ollama_client(args: argparse.Namespace) -> OllamaClient | None:
    if args.no_ollama:
        logging.info("Phase 1/7: Ollama disabled via --no-ollama")
        return None
    logging.info("Phase 1/7: Initializing Ollama client")
    return OllamaClient(
        url=args.ollama_url,
        model=args.ollama_model,
        timeout_s=args.ollama_timeout,
        retries=args.ollama_retries,
        keep_alive=(args.ollama_keep_alive or None),
    )


def _run_pipeline(args: argparse.Namespace, ollama_client: OllamaClient | None) -> PipelineRunResult:
    projects = load_config(Path(args.projects).resolve(), only_default=args.only_default)
    context = init_pipeline_context(
        projects=projects,
        remote=REMOTE_DEFAULT,
        days=args.days,
        out_dir=Path(args.out_dir).resolve(),
        cache_dir=Path(args.cache_dir).resolve(),
        use_temp=args.temp,
        include_ollama=not args.no_ollama,
        ollama_client=ollama_client,
        max_patch_chars=args.max_patch_chars,
        max_prompt_chars=args.max_prompt_chars,
        max_files_in_patch=args.max_files,
    )
    process_activity_stage(context)
    sync_repos(context)
    process_repo_branches(context)
    build_rollups(context)
    build_master_summary(context)
    return render_outputs(context)


def main() -> int:
    args = parse_args()
    setup_logging()
    start_time = dt.datetime.now(dt.timezone.utc)

    result = PipelineRunResult(exit_code=1, projects_processed=0, branches_analyzed=0, artifacts_root=Path(args.out_dir).resolve() / "artifacts", errors=[])
    run_error: str | None = None

    try:
        ollama_client = _build_ollama_client(args)
        result = _run_pipeline(args, ollama_client)
        return_code = result.exit_code
    except Exception as exc:  # noqa: BLE001
        run_error = str(exc)
        logging.exception("Pipeline failed")
        return_code = 1
    finally:
        end_time = dt.datetime.now(dt.timezone.utc)
        notification = PipelineEmailNotification(
            start_time=start_time,
            end_time=end_time,
            projects_processed=result.projects_processed,
            branches_analyzed=result.branches_analyzed,
            artifacts_dir=result.artifacts_root,
            error=run_error,
            warnings=result.errors,
        )
        logging.info("Phase 7/7: Sending completion email")
        send_pipeline_completion_email(notification)

    return return_code


if __name__ == "__main__":
    raise SystemExit(main())
