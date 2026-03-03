from __future__ import annotations

import datetime as dt
import json
import logging
import re
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

from utils.email_markup import render_email_markup
from utils.activity_logs import process_activity_logs
from utils.git import (
    branch_has_recent_commits,
    diff_name_status,
    diff_numstat,
    diff_patch,
    diff_stat,
    ensure_clone,
    fetch_all,
    get_default_remote_branch,
    list_remote_branches,
    merge_base,
    recent_merge_commits,
    repo_dir_name_from_project,
    rev_parse,
)
from utils.ollama import OllamaClient
from utils.parsing import (
    chunk_text,
    coerce_text,
    is_noisy_path,
    path_is_version_signal,
    sanitize_prompt,
    stable_json_hash,
    truncate,
    unique_preserve_order,
)

_VERSION_PATTERNS = [
    re.compile(r"\bnode\b[^\n]*\b(\d+\.\d+\.\d+|\d+)\b", re.IGNORECASE),
    re.compile(r"\bnpm\b[^\n]*\b(\d+\.\d+\.\d+|\d+)\b", re.IGNORECASE),
    re.compile(r"FROM\s+([^\s:]+):([^\s]+)", re.IGNORECASE),
    re.compile(r"\bpython\b[^\n]*\b(\d+\.\d+(\.\d+)?)\b", re.IGNORECASE),
    re.compile(r"\bkaniko\b[^\n]*\b(v?\d+\.\d+\.\d+)\b", re.IGNORECASE),
]


def read_projects(path: Path, only_default: bool) -> list[dict]:
    if not path.exists():
        raise FileNotFoundError(f"Projects file not found: {path}")
    obj = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(obj, list):
        raise ValueError("projects.json must be a JSON array")
    items: list[dict] = []
    for entry in obj:
        if not isinstance(entry, dict):
            continue
        if only_default and not bool(entry.get("is_default", False)):
            continue
        if entry.get("ssh_url"):
            items.append(entry)
    return items


def extract_version_signals(repo_dir: Path, base: str, head: str, paths: list[str]) -> list[str]:
    from utils.git import git

    signals: list[str] = []
    for path in paths:
        res = git(repo_dir, "diff", "--unified=0", f"{base}..{head}", "--", path)
        if not res.ok:
            continue
        for line in res.stdout.splitlines():
            if not line.startswith(("+", "-")) or line.startswith(("+++", "---")):
                continue
            text = line[1:].strip()
            if not text:
                continue
            if any(p.search(line) for p in _VERSION_PATTERNS):
                signals.append(f"{path}: {text[:240]}")
    return unique_preserve_order(signals)


def build_branch_prompt(repo: str, branch: str, parent: str, diffstat: str, patch: str, version_signals: list[str]) -> str:
    vs = "\n".join(f"- {entry}" for entry in version_signals[:40]) or "(none detected)"
    return sanitize_prompt(
        "You are summarizing code changes for a weekly engineering report.\n"
        "Use only the diff evidence provided; do NOT rely on commit messages/authors.\n\n"
        f"Repo: {repo}\nBranch: {branch}\nParent baseline: {parent}\n\n"
        f"DIFFSTAT:\n{diffstat}\n\n"
        f"VERSION/BUILD SIGNALS:\n{vs}\n\n"
        f"PATCH (curated; may be truncated):\n{patch}\n\n"
        "Return:\n"
        "- 3-8 bullet points: behavior/capability changes\n"
        "- 0-3 bullet points: risks/breaking changes\n"
        "- 0-3 bullet points: significant version/build changes\n"
    )


def build_repo_rollup_prompt(repo: str, branch_summaries: list[tuple[str, str]]) -> str:
    parts = [
        "You are producing a repo-level weekly rollup from branch summaries.",
        "Do not invent changes.",
        f"Repo: {repo}",
        "",
        "BRANCH SUMMARIES:",
    ]
    for branch, summary in branch_summaries:
        parts.append(f"\n### {branch}\n{summary.strip()}\n")
    parts.append("\nReturn:\n- 4-10 bullet points\n- 0-5 risks/migrations\n- 0-5 significant build/version changes\n")
    return sanitize_prompt("\n".join(parts))


def _cache_path(cache_root: Path, key: dict) -> Path:
    return cache_root / f"{stable_json_hash(key)}.txt"


def _render_weekly_markup(
    model: str,
    days: int,
    repo_sections: list[str],
    master_summary: str,
    activity_highlights: list[tuple[str, str]],
) -> str:
    stamp = dt.datetime.now().astimezone().strftime("%Y-%m-%d %H:%M %Z")
    header = [
        "# Weekly Engineering Summary",
        f"- Generated: {stamp}",
        f"- Window: last {days} days",
        f"- Model: {model}",
        "",
    ]
    content = "\n".join(repo_sections)
    activity_section = ["## Activity highlights"]
    if activity_highlights:
        for project_name, highlight in activity_highlights:
            activity_section.append(f"- {project_name}: {highlight}")
    else:
        activity_section.append("- No project activity logs found in project_activity/.")
    return "\n".join(header + ["## Master Summary", master_summary.strip(), ""] + activity_section + ["", content, ""])


@dataclass
class PipelineRunResult:
    exit_code: int
    projects_processed: int
    branches_analyzed: int
    artifacts_root: Path
    errors: list[str]


def run_summary_pipeline(
    *,
    projects: list[dict],
    remote: str,
    days: int,
    out_dir: Path,
    cache_dir: Path,
    use_temp: bool,
    include_ollama: bool,
    ollama_client: Optional[OllamaClient],
    max_patch_chars: int,
    max_prompt_chars: int,
    max_files_in_patch: int,
 ) -> PipelineRunResult:
    out_dir.mkdir(parents=True, exist_ok=True)
    artifacts_root = out_dir / "artifacts"
    artifacts_root.mkdir(parents=True, exist_ok=True)
    prompt_cache_dir = artifacts_root / "cache"
    prompt_cache_dir.mkdir(parents=True, exist_ok=True)
    errors_dir = artifacts_root / "errors"
    errors_dir.mkdir(parents=True, exist_ok=True)
    project_summaries_dir = artifacts_root / "project_summaries"
    project_summaries_dir.mkdir(parents=True, exist_ok=True)

    def persist_project_summary(repo_key: str, lines: list[str], sections: list[str]) -> None:
        project_summary_text = "\n".join(lines).strip() + "\n"
        (project_summaries_dir / f"{repo_key}.summary.markup").write_text(project_summary_text, encoding="utf-8")
        sections.append(project_summary_text + "\n")

    def call_or_cache(prompt: str, scope: dict) -> tuple[str | None, str | None]:
        if not include_ollama or ollama_client is None:
            return None, "Ollama disabled"
        cache_key = {
            "scope": scope,
            "model": ollama_client.model,
            "prompt_version": "v2",
            "timeout": ollama_client.timeout_s,
            "retries": ollama_client.retries,
            "prompt": prompt,
        }
        cp = _cache_path(prompt_cache_dir, cache_key)
        if cp.exists():
            return cp.read_text(encoding="utf-8"), None
        result = ollama_client.generate(sanitize_prompt(prompt))
        if result.error:
            return None, result.error.message
        cleaned = sanitize_prompt(coerce_text(result.text)).strip()
        cp.write_text(cleaned + "\n", encoding="utf-8")
        return cleaned, None

    repo_sections: list[str] = []
    project_summaries_for_master: list[tuple[str, str]] = []

    activity_result = process_activity_logs(
        out_dir=out_dir,
        days=days,
        include_ollama=include_ollama,
        ollama_client=ollama_client,
        max_prompt_chars=max_prompt_chars,
    )

    work_root = Path(tempfile.mkdtemp(prefix="weekly_repo_cache_")) if use_temp else cache_dir
    if not use_temp:
        work_root.mkdir(parents=True, exist_ok=True)

    branches_analyzed = 0
    errors: list[str] = []

    for item in projects:
        ssh_url = str(item.get("ssh_url", "")).strip()
        project_name = str(item.get("project_name", "")).strip()
        repo_display = project_name or ssh_url
        repo_key = repo_dir_name_from_project(project_name, ssh_url)
        repo_dir = work_root / repo_key
        repo_art_dir = artifacts_root / repo_key
        repo_art_dir.mkdir(parents=True, exist_ok=True)

        lines = [f"## Repo: {repo_display}", ""]
        activity_rollup = activity_result.rollups_by_project_name.get(repo_display)
        if not activity_rollup:
            project_id = item.get("project_id")
            if isinstance(project_id, int):
                activity_rollup = activity_result.rollups_by_project_id.get(project_id)
        ok, err = ensure_clone(ssh_url, repo_dir)
        if not ok:
            lines.append(f"- Clone error: {err}")
            errors.append(f"{repo_display}: clone error: {err}")
            persist_project_summary(repo_key, lines, repo_sections)
            continue
        fetch_all(repo_dir)

        parent = get_default_remote_branch(repo_dir, remote) or f"{remote}/main"
        branches, b_err = list_remote_branches(repo_dir, remote)
        if b_err or branches is None:
            msg = b_err or "unknown"
            lines.append(f"- Branch listing error: {msg}")
            errors.append(f"{repo_display}: branch listing error: {msg}")
            persist_project_summary(repo_key, lines, repo_sections)
            continue
        active = [b for b in branches if branch_has_recent_commits(repo_dir, b, f"{days} days ago")]
        lines.append(f"- Active branches in window: {len(active)}")

        merges = recent_merge_commits(repo_dir, days=days)
        if merges:
            lines.append("- Recent merge commits:")
            for m in merges[:8]:
                lines.append(f"  - {m['date']} {m['subject']} ({m['author']})")

        branch_rollups: list[tuple[str, str]] = []
        branches_analyzed += len(active)

        for branch in active:
            safe_branch = branch.replace("/", "__")
            if branch == parent:
                (repo_art_dir / f"{safe_branch}.summary.txt").write_text(
                    "Baseline branch (parent). No diff computed.\n", encoding="utf-8"
                )
                continue

            base = merge_base(repo_dir, parent, branch)
            head = rev_parse(repo_dir, branch)
            if not base or not head:
                continue

            stat = diff_stat(repo_dir, base, head)
            name_status = diff_name_status(repo_dir, base, head)
            num = diff_numstat(repo_dir, base, head)
            candidate_paths = [path for path, _ in num if not is_noisy_path(path)]
            chosen_paths = candidate_paths[:max_files_in_patch]
            changed_paths = [row.split("\t")[-1] for row in name_status if "\t" in row]
            version_paths = [path for path in changed_paths if path_is_version_signal(path)]
            version_signals = extract_version_signals(repo_dir, base, head, version_paths)
            patch = truncate(diff_patch(repo_dir, base, head, chosen_paths), max_patch_chars)

            prompt = truncate(
                build_branch_prompt(repo_display, branch, parent, stat, patch, version_signals),
                max_prompt_chars,
                suffix="\n\n[...prompt truncated...]\n",
            )
            (repo_art_dir / f"{safe_branch}.patch.txt").write_text(patch, encoding="utf-8")
            (repo_art_dir / f"{safe_branch}.prompt.txt").write_text(prompt, encoding="utf-8")

            summary_text: str | None = None
            if include_ollama:
                # hierarchical summarization for very large patch payloads
                patch_chunks = chunk_text(patch, max(1000, max_patch_chars // 2), overlap=500)
                if len(patch_chunks) > 1:
                    chunk_summaries: list[str] = []
                    for i, chunk in enumerate(patch_chunks, 1):
                        chunk_prompt = build_branch_prompt(repo_display, f"{branch} [chunk {i}/{len(patch_chunks)}]", parent, stat, chunk, version_signals)
                        chunk_prompt = truncate(chunk_prompt, max_prompt_chars, suffix="\n\n[...prompt truncated...]\n")
                        chunk_summary, chunk_err = call_or_cache(chunk_prompt, {"repo": repo_display, "branch": branch, "chunk": i})
                        if chunk_err:
                            summary_text = None
                            ts = dt.datetime.now().strftime("%Y%m%d-%H%M%S")
                            (errors_dir / f"{ts}_{repo_key}_{safe_branch}_chunk{i}.log").write_text(
                                json.dumps({"repo": repo_display, "branch": branch, "model": ollama_client.model if ollama_client else "", "error": chunk_err}, indent=2),
                                encoding="utf-8",
                            )
                            break
                        chunk_summaries.append(chunk_summary or "")
                    if chunk_summaries:
                        merged_prompt = build_repo_rollup_prompt(repo_display, [(f"chunk-{idx+1}", val) for idx, val in enumerate(chunk_summaries)])
                        summary_text, final_err = call_or_cache(merged_prompt, {"repo": repo_display, "branch": branch, "stage": "chunk_rollup"})
                        if final_err:
                            summary_text = None
                else:
                    summary_text, call_err = call_or_cache(prompt, {"repo": repo_display, "branch": branch})
                    if call_err:
                        ts = dt.datetime.now().strftime("%Y%m%d-%H%M%S")
                        (errors_dir / f"{ts}_{repo_key}_{safe_branch}.log").write_text(
                            json.dumps(
                                {
                                    "repo": repo_display,
                                    "branch": branch,
                                    "model": ollama_client.model if ollama_client else "",
                                    "url": ollama_client.url if ollama_client else "",
                                    "error": call_err,
                                    "max_patch_chars": max_patch_chars,
                                    "max_prompt_chars": max_prompt_chars,
                                },
                                indent=2,
                            ),
                            encoding="utf-8",
                        )

            if summary_text:
                (repo_art_dir / f"{safe_branch}.summary.txt").write_text(summary_text + "\n", encoding="utf-8")
                lines.append(f"### {branch}")
                lines.append(summary_text)
                lines.append("")
                branch_rollups.append((branch, summary_text))
            else:
                (repo_art_dir / f"{safe_branch}.summary.txt").write_text("ERROR: summary unavailable\n", encoding="utf-8")
                lines.append(f"### {branch}")
                lines.append("- Summary unavailable (see artifacts/errors logs).")

        if branch_rollups and include_ollama:
            rollup_prompt = truncate(build_repo_rollup_prompt(repo_display, branch_rollups), max_prompt_chars, suffix="\n\n[...prompt truncated...]\n")
            (repo_art_dir / "repo_rollup.prompt.txt").write_text(rollup_prompt, encoding="utf-8")
            rollup, rollup_err = call_or_cache(rollup_prompt, {"repo": repo_display, "stage": "repo_rollup"})
            if rollup:
                (repo_art_dir / "repo_rollup.summary.txt").write_text(rollup + "\n", encoding="utf-8")
                project_summaries_for_master.append((repo_display, rollup))
            elif rollup_err:
                errors.append(f"{repo_display}: repo rollup failed: {rollup_err}")

        lines.append("### Activity")
        if activity_rollup:
            lines.append(activity_rollup.strip())
        else:
            lines.append("- No activity rollup found for this project.")
        lines.append("")

        persist_project_summary(repo_key, lines, repo_sections)

    master_summary = "No repo summaries available."
    if project_summaries_for_master and include_ollama:
        master_prompt = build_repo_rollup_prompt("ALL_REPOS", project_summaries_for_master)
        master_prompt = truncate(master_prompt, max_prompt_chars, suffix="\n\n[...prompt truncated...]\n")
        (artifacts_root / "master_summary.prompt.txt").write_text(master_prompt, encoding="utf-8")
        master_text, master_err = call_or_cache(master_prompt, {"stage": "master_summary"})
        if master_text:
            master_summary = master_text
            (artifacts_root / "master_summary.summary.txt").write_text(master_text + "\n", encoding="utf-8")
        elif master_err:
            (artifacts_root / "master_summary.summary.txt").write_text(f"ERROR: {master_err}\n", encoding="utf-8")

    weekly_markup = _render_weekly_markup(
        ollama_client.model if ollama_client else "disabled",
        days,
        repo_sections,
        master_summary,
        activity_result.highlights_for_master,
    )
    weekly_file = out_dir / "weeklySummary.markup"
    weekly_file.write_text(weekly_markup, encoding="utf-8")

    weekly_email = render_email_markup(weekly_markup, title="Weekly Engineering Summary")
    (out_dir / "weeklySummary.email.markup").write_text(weekly_email, encoding="utf-8")

    logging.info("Wrote %s", weekly_file)
    return PipelineRunResult(
        exit_code=0,
        projects_processed=len(projects),
        branches_analyzed=branches_analyzed,
        artifacts_root=artifacts_root,
        errors=errors,
    )
