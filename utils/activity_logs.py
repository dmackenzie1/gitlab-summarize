from __future__ import annotations

import csv
import datetime as dt
import json
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

from utils.ollama import OllamaClient
from utils.parsing import sanitize_prompt, stable_json_hash, truncate

MAX_TEXT_FIELD_CHARS = 700
ACTIVITY_CHUNK_ROWS = 200


@dataclass
class ActivitySummaryResult:
    rollups_by_project_name: dict[str, str]
    rollups_by_project_id: dict[int, str]
    highlights_for_master: list[tuple[str, str]]


def _slug(text: str) -> str:
    token = re.sub(r"[^a-zA-Z0-9._-]+", "_", text.strip())
    return token.strip("_") or "unknown"


def _parse_dt(value: str | None) -> dt.datetime | None:
    if not value:
        return None
    raw = value.strip()
    if not raw:
        return None
    if raw.endswith("Z"):
        raw = raw[:-1] + "+00:00"
    try:
        return dt.datetime.fromisoformat(raw)
    except ValueError:
        return None


def _extract_project_id(payload: object, source_file: Path) -> int | None:
    if isinstance(payload, dict):
        for key in ("project_id", "projectId"):
            val = payload.get(key)
            if isinstance(val, int):
                return val
            if isinstance(val, str) and val.isdigit():
                return int(val)
        events = payload.get("events")
        if isinstance(events, list):
            for event in events:
                if isinstance(event, dict):
                    pid = event.get("project_id")
                    if isinstance(pid, int):
                        return pid
                    if isinstance(pid, str) and pid.isdigit():
                        return int(pid)
    if isinstance(payload, list):
        for event in payload:
            if isinstance(event, dict):
                pid = event.get("project_id")
                if isinstance(pid, int):
                    return pid
                if isinstance(pid, str) and pid.isdigit():
                    return int(pid)
    filename = source_file.name
    patterns = [r"events[-_](\d+)", r"project[-_](\d+)", r"(^|\D)(\d{3,})(\D|$)"]
    for pat in patterns:
        match = re.search(pat, filename)
        if not match:
            continue
        token = match.group(1) if pat != patterns[-1] else match.group(2)
        if token.isdigit():
            return int(token)
    return None


def _activity_events(payload: object) -> list[dict]:
    if isinstance(payload, list):
        return [row for row in payload if isinstance(row, dict)]
    if isinstance(payload, dict):
        events = payload.get("events")
        if isinstance(events, list):
            return [row for row in events if isinstance(row, dict)]
    return []


def _csv_rows_to_events(rows: list[dict[str, str]]) -> list[dict]:
    events: list[dict] = []
    for row in rows:
        timestamp = row.get("timestamp") or row.get("created_at") or ""
        actor = row.get("actor") or row.get("author") or row.get("author_username") or "unknown"
        action = row.get("action_type") or row.get("action") or row.get("action_name") or "activity"
        description = row.get("description") or row.get("summary") or row.get("title_or_text") or "(no details)"
        project_id = row.get("project_id") or row.get("projectId") or ""
        project_name = row.get("project_name") or row.get("projectName") or ""
        url = row.get("url") or row.get("web_url") or ""
        events.append(
            {
                "created_at": timestamp,
                "author_username": actor,
                "action_name": action,
                "target_title": description,
                "project_id": project_id,
                "project_name": project_name,
                "url": url,
            }
        )
    return events


def _event_author(event: dict) -> str:
    author = event.get("author")
    if isinstance(author, dict):
        for key in ("username", "name"):
            val = author.get(key)
            if isinstance(val, str) and val.strip():
                return val.strip()
    for key in ("author_username", "author_name"):
        val = event.get(key)
        if isinstance(val, str) and val.strip():
            return val.strip()
    return "unknown"


def _event_action(event: dict) -> str:
    action = str(event.get("action_name") or event.get("action") or event.get("event_name") or "activity").strip()
    target_type = str(event.get("target_type") or "").strip()
    if target_type and target_type.lower() not in action.lower():
        return f"{action} {target_type}".strip()
    return action or "activity"


def _event_text(event: dict) -> str:
    note = event.get("note")
    if isinstance(note, dict):
        body = note.get("body")
        if isinstance(body, str) and body.strip():
            return body.strip()
    push = event.get("push_data")
    if isinstance(push, dict):
        pieces: list[str] = []
        commit_title = push.get("commit_title")
        branch = push.get("ref")
        commit_count = push.get("commit_count")
        if isinstance(commit_title, str) and commit_title.strip():
            pieces.append(commit_title.strip())
        if isinstance(branch, str) and branch.strip():
            pieces.append(f"branch: {branch.strip()}")
        if isinstance(commit_count, int):
            pieces.append(f"commits: {commit_count}")
        if pieces:
            return " | ".join(pieces)
    for key in ("target_title", "title", "body"):
        val = event.get(key)
        if isinstance(val, str) and val.strip():
            return val.strip()
    return "(no details)"


def _event_url(event: dict) -> str:
    for key in ("target_web_url", "url", "web_url"):
        value = event.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip()
    note = event.get("note")
    if isinstance(note, dict):
        url = note.get("url")
        if isinstance(url, str) and url.strip():
            return url.strip()
    return ""


def _clean_text_for_llm(value: str) -> str:
    if not value:
        return ""
    value = value.encode("ascii", "replace").decode("ascii")
    value = sanitize_prompt(value)
    value = re.sub(r"\s+", " ", value).strip()
    value = value.replace("\t", " ")
    return truncate(value, MAX_TEXT_FIELD_CHARS, suffix=" ...")


def _summarize_chunks(
    *,
    rows: list[dict],
    project_name: str,
    source_name: str,
    ollama_client: OllamaClient,
    cache_dir: Path,
    max_prompt_chars: int,
) -> tuple[str | None, str | None, list[str]]:
    cache_dir.mkdir(parents=True, exist_ok=True)
    chunk_summaries: list[str] = []
    chunk_errors: list[str] = []

    for idx in range(0, len(rows), ACTIVITY_CHUNK_ROWS):
        chunk_rows = rows[idx : idx + ACTIVITY_CHUNK_ROWS]
        body = [
            "Summarize GitLab project activity. Focus on high-signal changes.",
            f"Project: {project_name}",
            f"Source: {source_name}",
            f"Chunk: {idx // ACTIVITY_CHUNK_ROWS + 1}",
            "",
        ]
        for event in chunk_rows:
            body.append(
                f"- {event['created_at'] or 'unknown_date'} | {event['author']} | {event['action']} | "
                f"{event['title_or_text']} | {event['url']}"
            )
        prompt = truncate("\n".join(body), max_prompt_chars, suffix="\n[...prompt truncated...]\n")
        key = {
            "scope": "activity_chunk",
            "project": project_name,
            "source": source_name,
            "idx": idx,
            "model": ollama_client.model,
            "prompt": prompt,
        }
        cache_file = cache_dir / f"{stable_json_hash(key)}.txt"
        if cache_file.exists():
            chunk_summaries.append(cache_file.read_text(encoding="utf-8"))
            continue
        result = ollama_client.generate(prompt)
        if result.error:
            chunk_errors.append(result.error.message)
            continue
        text = (result.text or "").strip()
        cache_file.write_text(text + "\n", encoding="utf-8")
        chunk_summaries.append(text)

    if not chunk_summaries:
        return None, "chunk summarization failed", chunk_errors

    if len(chunk_summaries) == 1:
        return chunk_summaries[0], None, chunk_errors

    rollup_prompt = [
        "Create a concise project activity summary from chunk summaries.",
        f"Project: {project_name}",
        f"Source: {source_name}",
        "Return sections: Activity highlights, MRs / merges, Notable discussions/comments, Other activity.",
    ]
    for i, summary in enumerate(chunk_summaries, 1):
        rollup_prompt.append(f"\n### Chunk {i}\n{summary}")
    merged_prompt = truncate("\n".join(rollup_prompt), max_prompt_chars, suffix="\n[...prompt truncated...]\n")
    result = ollama_client.generate(merged_prompt)
    if result.error:
        return None, result.error.message, chunk_errors
    return (result.text or "").strip(), None, chunk_errors


def process_activity_logs(
    *,
    out_dir: Path,
    days: int,
    include_ollama: bool,
    ollama_client: Optional[OllamaClient],
    max_prompt_chars: int,
) -> ActivitySummaryResult:
    activity_dir = Path("project_activity")
    artifacts_dir = out_dir / "artifacts" / "activity_logs"
    errors_dir = artifacts_dir / "errors"
    cache_dir = out_dir / "artifacts" / "cache"
    artifacts_dir.mkdir(parents=True, exist_ok=True)
    errors_dir.mkdir(parents=True, exist_ok=True)

    project_meta_file = Path("data/projects.json")
    project_name_by_id: dict[int, str] = {}
    if project_meta_file.exists():
        try:
            payload = json.loads(project_meta_file.read_text(encoding="utf-8"))
            if isinstance(payload, list):
                for row in payload:
                    if not isinstance(row, dict):
                        continue
                    pid = row.get("project_id")
                    pname = row.get("project_name")
                    if isinstance(pid, int) and isinstance(pname, str) and pname.strip():
                        project_name_by_id[pid] = pname.strip()
        except json.JSONDecodeError:
            pass

    if not activity_dir.exists() or not activity_dir.is_dir():
        return ActivitySummaryResult({}, {}, [])

    now = dt.datetime.now(dt.timezone.utc)
    window_start = now - dt.timedelta(days=days)

    project_rollup_inputs: dict[int | str, list[tuple[str, str]]] = {}
    project_name_lookup: dict[int | str, str] = {}

    source_files = sorted(activity_dir.glob("*.json")) + sorted(activity_dir.glob("*.csv"))
    for source_file in source_files:
        try:
            if source_file.suffix.lower() == ".json":
                payload = json.loads(source_file.read_text(encoding="utf-8"))
                events = _activity_events(payload)
                project_id = _extract_project_id(payload, source_file)
            else:
                with source_file.open("r", encoding="utf-8", newline="") as f_csv:
                    csv_rows = list(csv.DictReader(f_csv))
                events = _csv_rows_to_events(csv_rows)
                project_id = _extract_project_id(events, source_file)
                payload = events
        except Exception as exc:  # noqa: BLE001
            ts = dt.datetime.now().strftime("%Y%m%d-%H%M%S")
            (errors_dir / f"{ts}.unknown.{source_file.stem}.log").write_text(
                json.dumps({"source_file": str(source_file), "error": str(exc)}, indent=2), encoding="utf-8"
            )
            continue
        inferred_project_name = ""
        if events:
            raw_name = events[0].get("project_name")
            if isinstance(raw_name, str):
                inferred_project_name = raw_name.strip()
        project_name = project_name_by_id.get(
            project_id or -1,
            inferred_project_name or (f"project_{project_id}" if project_id is not None else "unknown_project"),
        )
        source_slug = _slug(source_file.stem)
        project_slug = _slug(project_name if project_name != "unknown_project" else str(project_id or "unknown"))

        normalized_rows: list[dict] = []
        for event in events:
            created_raw = event.get("created_at")
            created_at = created_raw.strip() if isinstance(created_raw, str) else ""
            created_dt = _parse_dt(created_at)
            date_status = "dated" if created_dt else "unknown_date"
            if created_dt and created_dt.tzinfo is None:
                created_dt = created_dt.replace(tzinfo=dt.timezone.utc)
            if created_dt and created_dt.tzinfo is not None:
                created_dt_utc = created_dt.astimezone(dt.timezone.utc)
            else:
                created_dt_utc = None

            include = True
            if created_dt_utc is not None:
                include = created_dt_utc >= window_start

            row = {
                "created_at": created_at,
                "author": _event_author(event),
                "action": _event_action(event),
                "title_or_text": _event_text(event),
                "url": _event_url(event),
                "project_id": project_id if project_id is not None else "",
                "project_name": project_name,
                "source_file": source_file.name,
                "date_status": date_status,
                "_created_dt": created_dt_utc,
                "_include": include,
            }
            normalized_rows.append(row)

        filtered_rows = [row for row in normalized_rows if row["_include"]]
        filtered_rows.sort(key=lambda row: (row["_created_dt"] is None, row["_created_dt"] or dt.datetime.max.replace(tzinfo=dt.timezone.utc)))

        csv_path = artifacts_dir / f"{project_slug}.{source_slug}.condensed.csv"
        jsonl_path = artifacts_dir / f"{project_slug}.{source_slug}.condensed.jsonl"
        meta_path = artifacts_dir / f"{project_slug}.{source_slug}.metadata.json"
        summary_path = artifacts_dir / f"{project_slug}.{source_slug}.activity.summary.md"

        fieldnames = [
            "created_at",
            "author",
            "action",
            "title_or_text",
            "url",
            "project_id",
            "project_name",
            "source_file",
            "date_status",
        ]
        with csv_path.open("w", encoding="utf-8", newline="") as f_csv:
            writer = csv.DictWriter(f_csv, fieldnames=fieldnames)
            writer.writeheader()
            for row in filtered_rows:
                writer.writerow({k: row[k] for k in fieldnames})

        with jsonl_path.open("w", encoding="utf-8") as f_jsonl:
            for row in filtered_rows:
                payload_row = {k: row[k] for k in fieldnames}
                f_jsonl.write(json.dumps(payload_row, ensure_ascii=False) + "\n")

        metadata = {
            "input_file": str(source_file),
            "events_total": len(events),
            "events_kept_after_filter": len(filtered_rows),
            "window_days_used": days,
            "processed_at": dt.datetime.now().astimezone().isoformat(),
        }
        meta_path.write_text(json.dumps(metadata, indent=2), encoding="utf-8")

        llm_rows = []
        for row in filtered_rows:
            llm_rows.append(
                {
                    "created_at": _clean_text_for_llm(row["created_at"]),
                    "author": _clean_text_for_llm(row["author"]),
                    "action": _clean_text_for_llm(row["action"]),
                    "title_or_text": _clean_text_for_llm(row["title_or_text"]),
                    "url": _clean_text_for_llm(row["url"]),
                }
            )

        if not llm_rows:
            summary_text = "## Activity highlights\n- No activity events in the selected time window.\n"
        elif include_ollama and ollama_client is not None:
            summary_text, err, chunk_errors = _summarize_chunks(
                rows=llm_rows,
                project_name=project_name,
                source_name=source_file.name,
                ollama_client=ollama_client,
                cache_dir=cache_dir,
                max_prompt_chars=max_prompt_chars,
            )
            if err:
                ts = dt.datetime.now().strftime("%Y%m%d-%H%M%S")
                excerpt = "\n".join(
                    f"- {row['created_at']} | {row['author']} | {row['action']} | {row['title_or_text']}"
                    for row in llm_rows[:8]
                )
                (errors_dir / f"{ts}.{project_slug}.{source_slug}.log").write_text(
                    json.dumps(
                        {
                            "project_id": project_id,
                            "project_name": project_name,
                            "source_file": source_file.name,
                            "model": ollama_client.model,
                            "url": ollama_client.url,
                            "timeout_s": ollama_client.timeout_s,
                            "error": err,
                            "chunk_errors": chunk_errors,
                            "sanitized_input_excerpt": excerpt,
                        },
                        indent=2,
                    ),
                    encoding="utf-8",
                )
                summary_text = (
                    "## Activity highlights\n"
                    "- Activity summary unavailable (Ollama error).\n\n"
                    "## MRs / merges\n- unavailable\n\n"
                    "## Notable discussions/comments\n- unavailable\n\n"
                    "## Other activity\n- unavailable\n"
                )
        else:
            top = llm_rows[:10]
            bullet_lines = "\n".join(
                f"- {row['created_at'] or 'unknown_date'}: {row['author']} {row['action']} — {row['title_or_text']}"
                for row in top
            )
            summary_text = (
                "## Activity highlights\n"
                "- Generated without Ollama (`--no-ollama`).\n"
                f"{bullet_lines}\n\n"
                "## MRs / merges\n- N/A\n\n"
                "## Notable discussions/comments\n- N/A\n\n"
                "## Other activity\n- N/A\n"
            )

        summary_path.write_text(summary_text.strip() + "\n", encoding="utf-8")

        project_key: int | str = project_id if project_id is not None else project_slug
        project_name_lookup[project_key] = project_name
        project_rollup_inputs.setdefault(project_key, []).append((source_file.name, summary_text.strip()))

    rollups_by_name: dict[str, str] = {}
    rollups_by_id: dict[int, str] = {}
    highlights: list[tuple[str, str]] = []

    for project_key, summaries in project_rollup_inputs.items():
        project_name = project_name_lookup[project_key]
        project_slug = _slug(project_name)
        rollup_path = artifacts_dir / f"{project_slug}.activity.rollup.md"
        lines = [f"# Activity rollup: {project_name}", ""]
        for source_name, summary_text in summaries:
            lines.append(f"## Source: {source_name}")
            lines.append(summary_text)
            lines.append("")
        rollup_text = "\n".join(lines).strip() + "\n"
        rollup_path.write_text(rollup_text, encoding="utf-8")
        rollups_by_name[project_name] = rollup_text
        if isinstance(project_key, int):
            rollups_by_id[project_key] = rollup_text

        first_highlight = "No highlights available"
        for line in rollup_text.splitlines():
            token = line.strip()
            if token.startswith("- "):
                first_highlight = token[2:].strip()
                break
        highlights.append((project_name, first_highlight))

    return ActivitySummaryResult(rollups_by_name, rollups_by_id, highlights)
