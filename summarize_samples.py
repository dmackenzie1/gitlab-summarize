import os
import sys
import argparse
import logging
from pathlib import Path
from datetime import datetime, timedelta
from emss_gitlab.activity_parse import parse_activity_page
from emss_gitlab.ollama import call_ollama

def setup_logging():
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def clean_temp_directory(temp_path: Path):
    if temp_path.exists():
        for item in temp_path.iterdir():
            if item.is_file():
                logging.info(f"Deleting {item}")
                item.unlink()
    else:
        temp_path.mkdir(parents=True, exist_ok=True)

def summarize_sample_files(sample_dir: Path, temp_dir: Path, days: int):
    for sample_file in sample_dir.glob('*.json'):
        logging.info(f"Processing sample file: {sample_file}")
        with open(sample_file, 'r', encoding='utf-8') as f:
            html_content = f.read()
        activity_summary = parse_activity_page(html_content)
        prompt = f"Summarize the following activity for the last {days} days:\n"
        prompt += "\n".join(activity_summary['narrative'])
        summary = call_ollama(prompt, model="qwen3.5:9b")
        summary_path = temp_dir / f"{sample_file.stem}_summary.txt"
        with open(summary_path, 'w', encoding='utf-8') as f:
            f.write(summary)
        logging.info(f"Summary saved to {summary_path}")

def main():
    setup_logging()
    parser = argparse.ArgumentParser(description="Summarize activity from sample files.")
    parser.add_argument('--sample-dir', type=Path, default=Path('./sample'), help='Directory containing sample files')
    parser.add_argument('--temp-dir', type=Path, default=Path('./out/temp'), help='Temporary directory for summaries')
    parser.add_argument('--days', type=int, default=7, help='Number of days to look back')
    args = parser.parse_args()

    clean_temp_directory(args.temp_dir)

    logging.info("Step 1/2: Summarizing sample files")
    summarize_sample_files(args.sample_dir, args.temp_dir, args.days)

    logging.info("Step 2/2: Done")

if __name__ == "__main__":
    main()
