from __future__ import annotations

import datetime as dt
import logging
import smtplib
from dataclasses import dataclass
from email.message import EmailMessage
from pathlib import Path

SMTP_HOST = "ndc-relay.ndc.nasa.gov"
SMTP_PORT = 25
EMAIL_FROM = "emss-no-reply@mail.nasa.gov"
EMAIL_TO = "david.l.mackenzie@nasa.gov"


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

    try:
        with smtplib.SMTP(SMTP_HOST, SMTP_PORT, timeout=15) as smtp:
            smtp.send_message(message)
    except Exception as exc:  # noqa: BLE001
        logging.warning("Completion email send failed: %s", exc)
