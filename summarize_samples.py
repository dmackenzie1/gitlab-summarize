import argparse
import logging
import re
from pathlib import Path

from emss_gitlab.ollama import call_ollama


def setup_logging():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")


def clean_temp_directory(temp_path: Path):
    temp_path.mkdir(parents=True, exist_ok=True)
    for item in temp_path.iterdir():
        if item.is_file():
            logging.info(f"Deleting {item}")
            item.unlink()


def _sanitize_for_ollama(text: str, *, max_chars: int = 120_000) -> str:
    """
    Make prompt text safer for JSON transport + model consumption.
    - normalize newlines
    - strip NUL/control chars
    - expand tabs
    - trim excessive whitespace
    - cap size
    """
    if text is None:
        return ""

    # Normalize line endings early
    text = text.replace("\r\n", "\n").replace("\r", "\n")

    # Remove NULs (common in weird logs/copies)
    text = text.replace("\x00", "")

    # Remove other control chars except \n and \t
    # Keep \n; convert \t to spaces below
    text = "".join(ch for ch in text if (ch == "\n" or ch == "\t" or ord(ch) >= 32))

    # Convert tabs to spaces (tabs can create JSON escapes / formatting weirdness downstream)
    text = text.replace("\t", "    ")

    # Optionally collapse super-long runs of spaces
    text = re.sub(r"[ ]{8,}", "    ", text)

    # Cap size (protect ollama + your wrapper)
    if len(text) > max_chars:
        text = text[:max_chars] + "\n\n[TRUNCATED]\n"

    return text.strip()


def _build_prompt(narrative_lines: list[str], *, source_name: str) -> str:
    # Keep the content itself clean
    body = "\n".join(narrative_lines or [])
    body = _sanitize_for_ollama(body, max_chars=110_000)

    # Also keep the whole prompt clean and structured
    prompt = f"""You are summarizing GitLab activity in a JSON file.
    Look at notes, titles, bodies, and fields with long text.  I want to process the value of the values, not the key names,

Goal:
Produce a concise summary of activity.

Output format:
- One short paragraph (3-6 sentences)
- Then bullet points of the most important items (max 8 bullets)
- Prefer concrete changes over chatter
- If the input is empty or unclear, say so briefly

Source file: {source_name}

Activity:
{body}
"""
    return _sanitize_for_ollama(prompt, max_chars=120_000)


def _normalize_ollama_result(result):
    """
    call_ollama() may return:
      - str
      - (text, error)
    Normalize to (text: str|None, err: str|None)
    """
    if isinstance(result, tuple):
        if len(result) == 2:
            text, err = result
            text = text if isinstance(text, str) and text.strip() else None
            err = err if isinstance(err, str) and err.strip() else None
            return text, err
        return None, f"Unexpected tuple return from call_ollama (len={len(result)}): {result!r}"

    if isinstance(result, str):
        text = result.strip()
        return (text if text else None), None

    return None, f"Unexpected return type from call_ollama: {type(result).__name__}"


def summarize_sample_files(sample_dir: Path, temp_dir: Path):
    sample_files = sorted(sample_dir.glob("*.json"))
    if not sample_files:
        logging.warning(f"No .json files found in {sample_dir}")
        return

    for sample_file in sample_files:
        logging.info(f"Processing sample file: {sample_file}")

        try:
            raw_text = sample_file.read_text(encoding="utf-8")
        except UnicodeDecodeError:
            raw_text = sample_file.read_text(encoding="utf-8", errors="replace")

        prompt = _build_prompt(raw_text, source_name=sample_file.name)

        raw = call_ollama(prompt, model="llama3.1:latest")
        summary_text, err = _normalize_ollama_result(raw)

        if err:
            logging.error(f"Ollama error for {sample_file.name}: {err}")
            (temp_dir / f"{sample_file.stem}_error.txt").write_text(err + "\n", encoding="utf-8")
            continue

        if not summary_text:
            logging.error(f"Ollama returned no summary text for {sample_file.name}")
            continue

        summary_path = temp_dir / f"{sample_file.stem}_summary.txt"
        summary_path.write_text(summary_text + "\n", encoding="utf-8")
        logging.info(f"Summary saved to {summary_path}")


def main():
    setup_logging()
    parser = argparse.ArgumentParser(description="Summarize activity from sample files.")
    parser.add_argument("--sample-dir", type=Path, default=Path("./sample"), help="Directory containing sample files")
    parser.add_argument("--temp-dir", type=Path, default=Path("./out/temp"), help="Temporary directory for summaries")
    args = parser.parse_args()

    clean_temp_directory(args.temp_dir)

    logging.info("Step 1/2: Summarizing sample files")
    summarize_sample_files(args.sample_dir, args.temp_dir)

    logging.info("Step 2/2: Done")


if __name__ == "__main__":
    main()