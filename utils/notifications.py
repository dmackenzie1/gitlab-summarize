import datetime as dt
import logging
import smtplib
from dataclasses import dataclass
from email.message import EmailMessage
import html
import os
from pathlib import Path

def _csv_env(name: str, default: str) -> str:
    raw = os.getenv(name, default)
    return ", ".join([item.strip() for item in raw.split(",") if item.strip()])

SMTP_HOST = os.getenv("SMTP_HOST", "ndc-relay.ndc.nasa.gov")
SMTP_PORT = int(os.getenv("SMTP_PORT", "25"))
EMAIL_FROM = os.getenv("EMAIL_FROM", "emss-no-reply@mail.nasa.gov")
EMAIL_TO = _csv_env("SMTP_TO", "david.l.mackenzie@nasa.gov")

@dataclass
class PipelineEmailNotification:
    start_time: dt.datetime
    end_time: dt.datetime
    projects_processed: int
    branches_analyzed: int
    artifacts_dir: Path
    error: str | None = None
    warnings: list[str] | None = None

def send_pipeline_completion_email(notification: PipelineEmailNotification) -> None:
    duration = notification.end_time - notification.start_time
    status = "SUCCESS" if notification.error is None else "FAILURE"

    body = [
        f"Weekly project summary pipeline status: {status}",
        "",
        f"Start time (UTC): {notification.start_time.isoformat()}",
        f"End time (UTC): {notification.end_time.isoformat()}",
        f"Duration: {duration}",
        f"Projects processed: {notification.projects_processed}",
        f"Branches analyzed: {notification.branches_analyzed}",
        f"Artifacts directory: {notification.artifacts_dir}",
    ]

    if notification.warnings:
        body.append("")
        body.append("Warnings:")
        body.extend(f"- {warning}" for warning in notification.warnings[:20])

    if notification.error:
        body.append("")
        body.append("Error:")
        body.append(notification.error)

    message = EmailMessage()
    message["Subject"] = f"Weekly Summary Pipeline {status}"
    message["From"] = EMAIL_FROM
    message["To"] = EMAIL_TO
    message.set_content("\n".join(body) + "\n")

    summary_markup_path = notification.artifacts_dir.parent / "weeklySummary.email.markup"
    if summary_markup_path.exists():
        summary_html = summary_markup_path.read_text(encoding="utf-8")
        status_section = "\n".join(
            [
                '<section style="margin-bottom:18px;padding:12px;border:1px solid #ddd;border-radius:6px;background:#fafafa">',
                f"<h3 style=\"margin:0 0 8px 0\">Pipeline status: {html.escape(status)}</h3>",
                "<ul style=\"margin:0;padding-left:18px\">",
                f"<li>Start (UTC): {html.escape(notification.start_time.isoformat())}</li>",
                f"<li>End (UTC): {html.escape(notification.end_time.isoformat())}</li>",
                f"<li>Duration: {html.escape(str(duration))}</li>",
                f"<li>Projects processed: {notification.projects_processed}</li>",
                f"<li>Branches analyzed: {notification.branches_analyzed}</li>",
                "</ul>",
                "</section>",
            ]
        )
        if "</body>" in summary_html:
            html_body = summary_html.replace("</body>", f"{status_section}\n</body>", 1)
        else:
            html_body = f"<html><body>{summary_html}\n{status_section}</body></html>"

        message.add_alternative(html_body, subtype="html")

    try:
        with smtplib.SMTP(SMTP_HOST, SMTP_PORT, timeout=15) as smtp:
            smtp.send_message(message)
    except Exception as exc:  # noqa: BLE001
        logging.warning("Completion email send failed: %s", exc)
