from datetime import datetime, timedelta

def generate_markdown_report(out_path: str, days: int, activity_summary: dict, project_stats: dict, project_summaries: dict) -> str:
    end_date = datetime.now()
    start_date = end_date - timedelta(days=days)
    report = f"# Weekly Summary ({start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')})\n\n"

    if not project_stats:
        report += "## Token Missing\n\n"
        report += "GitLab token is missing. Please set the GITLAB_TOKEN environment variable.\n\n"
        report += "## Group Activity Highlights\n\n"
        report += "\n".join(activity_summary['narrative'])
        return report

    report += "## Group Activity Highlights\n\n"
    report += "\n".join(activity_summary['narrative'])
    report += "\n\n## Project Statistics and Summaries\n\n"

    for project_name, stats in project_stats.items():
        report += f"### {project_name}\n\n"
        report += f"- Open MRs: {len(stats['open_mrs'])}\n"
        report += f"- Merged MRs: {len(stats['merged_mrs'])}\n"
        report += f"- Active Branches: {len(stats['active_branches'])}\n\n"
        report += f"#### Summary\n\n"
        report += project_summaries[project_name]
        report += "\n\n"

    return report
