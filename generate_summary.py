import os
import sys
import argparse
import logging
from pathlib import Path
from datetime import datetime, timedelta
from emss_gitlab.client import GitLabClient
from emss_gitlab.activity_parse import parse_activity_page
from emss_gitlab.report import generate_markdown_report
from emss_gitlab.ollama import call_ollama

def setup_logging():
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def clean_output_directory(out_path: Path):
    if out_path.exists():
        for item in out_path.iterdir():
            if item.is_dir() and item.name != 'cache':
                logging.info(f"Deleting {item}")
                for subitem in item.iterdir():
                    subitem.unlink()
                item.rmdir()
            elif item.is_file():
                logging.info(f"Deleting {item}")
                item.unlink()
    else:
        out_path.mkdir(parents=True, exist_ok=True)

def main():
    setup_logging()
    parser = argparse.ArgumentParser(description="Generate a weekly Markdown summary for the EMSS GitLab group.")
    parser.add_argument('--group', required=True, help='GitLab group name')
    parser.add_argument('--days', type=int, default=7, help='Number of days to look back')
    parser.add_argument('--out', type=Path, default=Path('./out'), help='Output directory')
    args = parser.parse_args()

    clean_output_directory(args.out)

    temp_dir = args.out / 'temp'
    temp_dir.mkdir(parents=True, exist_ok=True)

    logging.info("Step 1/8: Setting up GitLab client")
    gitlab_client = GitLabClient()

    logging.info("Step 2/8: Parsing activity page")
    activity_path = Path('./activity_dump.html')
    if not activity_path.exists():
        logging.error("Activity dump file not found. Please provide the activity page dump as ./activity_dump.html")
        sys.exit(1)
    activity_summary = parse_activity_page(html_content=activity_path.read_text())

    logging.info("Step 3/8: Fetching additional data from GitLab API")
    if gitlab_client.token:
        project_stats = gitlab_client.get_group_project_stats(group_name=args.group, days=args.days)
    else:
        logging.warning("GitLab token missing. Running in best effort mode.")
        project_stats = None

    logging.info("Step 4/8: Collecting diffs and generating prompts")
    project_details = {}
    for project_name, stats in project_stats.items():
        project_temp_dir = temp_dir / project_name
        project_temp_dir.mkdir(parents=True, exist_ok=True)
        project_details[project_name] = {
            'open_mrs': [],
            'merged_mrs': [],
            'active_branches': []
        }
        for mr in stats['open_mrs']:
            diff_path = project_temp_dir / f"{project_name}_{mr['iid']}_diff.txt"
            diff_content = gitlab_client.get_diff(project_id=mr['project_id'], mr_iid=mr['iid'], branch_name=None)
            diff_path.write_text(diff_content)
            prompt = f"Review the following diff for {project_name} MR {mr['iid']}:\n{diff_content}"
            response = call_ollama(prompt=prompt, model="qwen2.5-coder:32b")
            response_path = project_temp_dir / f"{project_name}_{mr['iid']}_response.txt"
            response_path.write_text(response)
            project_details[project_name]['open_mrs'].append({
                'mr_iid': mr['iid'],
                'diff': diff_content,
                'response': response
            })

        for mr in stats['merged_mrs']:
            diff_path = project_temp_dir / f"{project_name}_{mr['iid']}_diff.txt"
            diff_content = gitlab_client.get_diff(project_id=mr['project_id'], mr_iid=mr['iid'], branch_name=None)
            diff_path.write_text(diff_content)
            prompt = f"Review the following diff for {project_name} MR {mr['iid']}:\n{diff_content}"
            response = call_ollama(prompt=prompt)
            response_path = project_temp_dir / f"{project_name}_{mr['iid']}_response.txt"
            response_path.write_text(response)
            project_details[project_name]['merged_mrs'].append({
                'mr_iid': mr['iid'],
                'diff': diff_content,
                'response': response
            })

        for branch in stats['active_branches']:
            diff_path = project_temp_dir / f"{project_name}_{branch['name']}_diff.txt"
            diff_content = gitlab_client.get_diff(project_id=project_stats[project_name]['open_mrs'][0]['project_id'], branch_name=branch['name'], mr_iid=None)
            diff_path.write_text(diff_content)
            prompt = f"Review the following diff for {project_name} branch {branch['name']}:\n{diff_content}"
            response = call_ollama(prompt=prompt, model="qwen2.5-coder:32b")
            response_path = project_temp_dir / f"{project_name}_{branch['name']}_response.txt"
            response_path.write_text(response)
            project_details[project_name]['active_branches'].append({
                'branch_name': branch['name'],
                'diff': diff_content,
                'response': response
            })

    logging.info("Step 5/8: Generating project summaries with Ollama")
    project_summaries = {}
    for project_name, details in project_details.items():
        prompt = f"Summarize the following details for {project_name}:\n"
        prompt += "Open MRs:\n"
        for mr in details['open_mrs']:
            prompt += f"- MR {mr['mr_iid']}:\n{mr['response']}\n"
        prompt += "Merged MRs:\n"
        for mr in details['merged_mrs']:
            prompt += f"- MR {mr['mr_iid']}:\n{mr['response']}\n"
        prompt += "Active Branches:\n"
        for branch in details['active_branches']:
            prompt += f"- Branch {branch['branch_name']}:\n{branch['response']}\n"
        project_summaries[project_name] = call_ollama(prompt=prompt, model="qwen3.5:9b")

    logging.info("Step 6/8: Generating Markdown report")
    report_path = args.out / 'weekly_summary.md'
    report_content = generate_markdown_report(out_path=report_path, days=args.days, activity_summary=activity_summary, project_stats=project_stats, project_summaries=project_summaries)

    logging.info("Step 7/8: Writing report to file")
    with open(report_path, 'w') as f:
        f.write(report_content)

    logging.info("Step 8/8: Done")

if __name__ == "__main__":
    main()
