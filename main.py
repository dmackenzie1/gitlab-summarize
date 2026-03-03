#!/usr/bin/env python3
from __future__ import annotations

import argparse
import logging
from pathlib import Path

from utils.ollama import OllamaClient
from utils.summary import generate_weekly_summary, read_monitored

REMOTE_DEFAULT = "origin"
OLLAMA_URL_DEFAULT = "http://localhost:11434/api/generate"
OLLAMA_MODEL_DEFAULT = "qwen2.5-coder:32b"

DAYS_DEFAULT = 10
MAX_PROMPT_CHARS_DEFAULT = 55_000
MAX_PATCH_CHARS_DEFAULT = 40_000
MAX_FILES_IN_PATCH_DEFAULT = 12


def setup_logging(quiet: bool) -> None:
    logging.basicConfig(
        level=(logging.WARNING if quiet else logging.INFO),
        format="%(asctime)s - %(levelname)s - %(message)s",
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Diff-based weekly summary pipeline with optional Ollama summarization.")
    parser.add_argument("--monitored", default="data/monitored.json", help="Path to monitored.json")
    parser.add_argument("--only-default", action="store_true", help="Only include entries where is_default=true")

    parser.add_argument("--remote", default=REMOTE_DEFAULT)
    parser.add_argument("--days", type=int, default=DAYS_DEFAULT)

    parser.add_argument("--out-dir", default=".", help="Directory to write weeklySummary.markup outputs")
    parser.add_argument("--cache-dir", default="repo_cache")
    parser.add_argument("--temp", action="store_true")

    parser.add_argument("--no-ollama", action="store_true")
    parser.add_argument("--ollama-url", default=OLLAMA_URL_DEFAULT)
    parser.add_argument("--ollama-model", default=OLLAMA_MODEL_DEFAULT)
    parser.add_argument("--ollama-timeout", type=int, default=240)
    parser.add_argument("--ollama-retries", type=int, default=3)
    parser.add_argument("--ollama-backoff", type=float, default=0.4)
    parser.add_argument("--ollama-keep-alive", default="20m", help="Ollama keep_alive value; empty disables")

    parser.add_argument("--max-patch-chars", type=int, default=MAX_PATCH_CHARS_DEFAULT)
    parser.add_argument("--max-prompt-chars", type=int, default=MAX_PROMPT_CHARS_DEFAULT)
    parser.add_argument("--max-files", type=int, default=MAX_FILES_IN_PATCH_DEFAULT)

    parser.add_argument("--fail-fast", action="store_true", help="Stop the run on first unrecoverable Ollama error")
    parser.add_argument("--quiet", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    setup_logging(args.quiet)

    monitored_items = read_monitored(Path(args.monitored).resolve(), only_default=args.only_default)
    ollama_client = None
    if not args.no_ollama:
        ollama_client = OllamaClient(
            url=args.ollama_url,
            model=args.ollama_model,
            timeout_s=args.ollama_timeout,
            retries=args.ollama_retries,
            backoff_s=args.ollama_backoff,
            keep_alive=(args.ollama_keep_alive or None),
        )

    return generate_weekly_summary(
        monitored_items=monitored_items,
        remote=args.remote,
        days=args.days,
        out_dir=Path(args.out_dir).resolve(),
        cache_dir=Path(args.cache_dir).resolve(),
        use_temp=args.temp,
        include_ollama=not args.no_ollama,
        ollama_client=ollama_client,
        max_patch_chars=args.max_patch_chars,
        max_prompt_chars=args.max_prompt_chars,
        max_files_in_patch=args.max_files,
        continue_on_error=not args.fail_fast,
    )


if __name__ == "__main__":
    raise SystemExit(main())
