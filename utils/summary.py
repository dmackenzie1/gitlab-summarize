from __future__ import annotations

import datetime as dt
import json
import logging
import re
import tempfile
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

from utils.activity_logs import ActivitySummaryResult, process_activity_logs
from utils.email_markup import render_email_markup
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


@dataclass
class PipelineRunResult:
    exit_code: int
    projects_processed: int
    branches_analyzed: int
    artifacts_root: Path
    errors: list[str]


@dataclass
class RepoWorkItem:
    item: dict
    repo_display: str
    repo_key: str
    repo_dir: Path
    repo_art_dir: Path
    lines: list[str] = field(default_factory=list)
    activity_rollup: str | None = None
    parent: str = ""
    active_branches: list[str] = field(default_factory=list)
    branch_rollups: list[tuple[str, str]] = field(default_factory=list)


@dataclass
class PipelineContext:
    projects: list[dict]
    remote: str
    days: int
    out_dir: Path
    cache_dir: Path
    use_temp: bool
    include_ollama: bool
    ollama_client: Optional[OllamaClient]
    max_patch_chars: int
    max_prompt_chars: int
    max_files_in_patch: int
    artifacts_root: Path
    prompt_cache_dir: Path
    errors_dir: Path
    project_summaries_dir: Path
    activity_result: ActivitySummaryResult = field(default_factory=lambda: ActivitySummaryResult({}, []))
    work_root: Path | None = None
    repo_sections: list[str] = field(default_factory=list)
    project_summaries_for_master: list[tuple[str, str]] = field(default_factory=list)
    repo_items: list[RepoWorkItem] = field(default_factory=list)
    branches_analyzed: int = 0
    errors: list[str] = field(default_factory=list)
    master_summary: str = "No repo summaries available."


def _emit(stage: str, message: str) -> None:
    print(f"[{stage}] {message}", flush=True)


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


def load_config(path: Path, only_default: bool) -> list[dict]:
    if path.exists():
        _emit("CONFIG", f"loading repositories from {path}")
        return read_projects(path, only_default=only_default)
    fallback = Path("data/projects.json").resolve()
    _emit("CONFIG", f"{path} missing; falling back to {fallback}")
    return read_projects(fallback, only_default=only_default)


def init_pipeline_context(
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
) -> PipelineContext:
    out_dir.mkdir(parents=True, exist_ok=True)
    artifacts_root = out_dir / "artifacts"
    artifacts_root.mkdir(parents=True, exist_ok=True)
    prompt_cache_dir = artifacts_root / "cache"
    prompt_cache_dir.mkdir(parents=True, exist_ok=True)
    errors_dir = artifacts_root / "errors"
    errors_dir.mkdir(parents=True, exist_ok=True)
    project_summaries_dir = artifacts_root / "project_summaries"
    project_summaries_dir.mkdir(parents=True, exist_ok=True)
    return PipelineContext(
        projects=projects,
        remote=remote,
        days=days,
        out_dir=out_dir,
        cache_dir=cache_dir,
        use_temp=use_temp,
        include_ollama=include_ollama,
        ollama_client=ollama_client,
        max_patch_chars=max_patch_chars,
        max_prompt_chars=max_prompt_chars,
        max_files_in_patch=max_files_in_patch,
        artifacts_root=artifacts_root,
        prompt_cache_dir=prompt_cache_dir,
        errors_dir=errors_dir,
        project_summaries_dir=project_summaries_dir,
    )


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


def _persist_project_summary(context: PipelineContext, repo_key: str, lines: list[str]) -> None:
    text = "\n".join(lines).rstrip() + "\n"
    (context.project_summaries_dir / f"{repo_key}.summary.markup").write_text(text, encoding="utf-8")
    context.repo_sections.append(text)


def process_activity_stage(context: PipelineContext) -> None:
    _emit("ACTIVITY", "processing project_activity inputs")
    context.activity_result = process_activity_logs(
        out_dir=context.out_dir,
        days=context.days,
        include_ollama=context.include_ollama,
        ollama_client=context.ollama_client,
        max_prompt_chars=context.max_prompt_chars,
        log_item=lambda msg: _emit("ACTIVITY", msg),
    )


def sync_repos(context: PipelineContext) -> None:
    context.work_root = Path(tempfile.mkdtemp(prefix="weekly_repo_cache_")) if context.use_temp else context.cache_dir
    if not context.use_temp:
        context.work_root.mkdir(parents=True, exist_ok=True)
    _emit("REPO", f"sync root: {context.work_root}")

    for item in context.projects:
        ssh_url = str(item.get("ssh_url", "")).strip()
        project_name = str(item.get("project_name", "")).strip()
        repo_display = project_name or ssh_url
        repo_key = repo_dir_name_from_project(project_name, ssh_url)
        repo_dir = context.work_root / repo_key
        repo_art_dir = context.artifacts_root / repo_key
        repo_art_dir.mkdir(parents=True, exist_ok=True)
        work_item = RepoWorkItem(
            item=item,
            repo_display=repo_display,
            repo_key=repo_key,
            repo_dir=repo_dir,
            repo_art_dir=repo_art_dir,
            lines=[f"## Repo: {repo_display}", ""],
        )
        activity_rollup = context.activity_result.rollups_by_project_name.get(repo_display)
        work_item.activity_rollup = activity_rollup

        ok, err = ensure_clone(ssh_url, repo_dir)
        if not ok:
            work_item.lines.append(f"- Clone error: {err}")
            context.errors.append(f"{repo_display}: clone error: {err}")
            _emit("REPO", f"{repo_display} clone failed")
            context.repo_items.append(work_item)
            continue
        fetch_all(repo_dir)

        parent = get_default_remote_branch(repo_dir, context.remote) or f"{context.remote}/main"
        branches, b_err = list_remote_branches(repo_dir, context.remote)
        if b_err or branches is None:
            msg = b_err or "unknown"
            work_item.lines.append(f"- Branch listing error: {msg}")
            context.errors.append(f"{repo_display}: branch listing error: {msg}")
            _emit("REPO", f"{repo_display} branch listing failed")
            context.repo_items.append(work_item)
            continue

        active = [b for b in branches if branch_has_recent_commits(repo_dir, b, f"{context.days} days ago")]
        work_item.parent = parent
        work_item.active_branches = active
        work_item.lines.append(f"- Active branches in window: {len(active)}")
        _emit("REPO", f"{repo_display} active_branches={len(active)}")

        merges = recent_merge_commits(repo_dir, days=context.days)
        if merges:
            work_item.lines.append("- Recent merge commits:")
            for m in merges[:8]:
                work_item.lines.append(f"  - {m['date']} {m['subject']} ({m['author']})")
        context.repo_items.append(work_item)




def _call_or_cache(context: PipelineContext, prompt: str, scope: dict) -> tuple[str | None, str | None]:
    if not context.include_ollama or context.ollama_client is None:
        return None, None
    key = {
        "v": 4,
        "model": context.ollama_client.model,
        "timeout": context.ollama_client.timeout_s,
        "retries": context.ollama_client.retries,
        "scope": scope,
        "prompt": prompt,
    }
    cpath = _cache_path(context.prompt_cache_dir, key)
    if cpath.exists():
        return cpath.read_text(encoding="utf-8"), None
    result = context.ollama_client.generate(prompt)
    if result.error:
        return None, coerce_text(result.error.message)
    text = (result.text or "").strip()
    cpath.write_text(text + "\n", encoding="utf-8")
    return text, None

def process_repo_branches(context: PipelineContext) -> None:
    for repo_item in context.repo_items:
        if not repo_item.active_branches:
            continue
        context.branches_analyzed += len(repo_item.active_branches)

        for branch in repo_item.active_branches:
            _emit("BRANCH", f"{repo_item.repo_display} {branch}")
            safe_branch = branch.replace("/", "__")
            if branch == repo_item.parent:
                (repo_item.repo_art_dir / f"{safe_branch}.summary.txt").write_text(
                    "Baseline branch (parent). No diff computed.\n", encoding="utf-8"
                )
                continue

            base = merge_base(repo_item.repo_dir, repo_item.parent, branch)
            head = rev_parse(repo_item.repo_dir, branch)
            if not base or not head:
                continue

            stat = diff_stat(repo_item.repo_dir, base, head)
            name_status = diff_name_status(repo_item.repo_dir, base, head)
            num = diff_numstat(repo_item.repo_dir, base, head)
            candidate_paths = [path for path, _ in num if not is_noisy_path(path)]
            chosen_paths = candidate_paths[: context.max_files_in_patch]
            changed_paths = [row.split("\t")[-1] for row in name_status if "\t" in row]
            version_paths = [path for path in changed_paths if path_is_version_signal(path)]
            version_signals = extract_version_signals(repo_item.repo_dir, base, head, version_paths)
            patch = truncate(diff_patch(repo_item.repo_dir, base, head, chosen_paths), context.max_patch_chars)

            prompt = truncate(
                build_branch_prompt(repo_item.repo_display, branch, repo_item.parent, stat, patch, version_signals),
                context.max_prompt_chars,
                suffix="\n\n[...prompt truncated...]\n",
            )
            (repo_item.repo_art_dir / f"{safe_branch}.patch.txt").write_text(patch, encoding="utf-8")
            (repo_item.repo_art_dir / f"{safe_branch}.prompt.txt").write_text(prompt, encoding="utf-8")

            summary_text: str | None = None
            if context.include_ollama:
                patch_chunks = chunk_text(patch, max(1000, context.max_patch_chars // 2), overlap=500)
                if len(patch_chunks) > 1:
                    chunk_summaries: list[str] = []
                    for i, chunk in enumerate(patch_chunks, 1):
                        chunk_prompt = build_branch_prompt(
                            repo_item.repo_display,
                            f"{branch} [chunk {i}/{len(patch_chunks)}]",
                            repo_item.parent,
                            stat,
                            chunk,
                            version_signals,
                        )
                        chunk_prompt = truncate(chunk_prompt, context.max_prompt_chars, suffix="\n\n[...prompt truncated...]\n")
                        chunk_summary, chunk_err = _call_or_cache(context, chunk_prompt, {"repo": repo_item.repo_display, "branch": branch, "chunk": i})
                        if chunk_err:
                            summary_text = None
                            ts = dt.datetime.now().strftime("%Y%m%d-%H%M%S")
                            (context.errors_dir / f"{ts}_{repo_item.repo_key}_{safe_branch}_chunk{i}.log").write_text(
                                json.dumps(
                                    {
                                        "repo": repo_item.repo_display,
                                        "branch": branch,
                                        "model": context.ollama_client.model if context.ollama_client else "",
                                        "error": chunk_err,
                                    },
                                    indent=2,
                                ),
                                encoding="utf-8",
                            )
                            break
                        chunk_summaries.append(chunk_summary or "")
                    if chunk_summaries:
                        merged_prompt = build_repo_rollup_prompt(
                            repo_item.repo_display,
                            [(f"chunk-{idx + 1}", val) for idx, val in enumerate(chunk_summaries)],
                        )
                        summary_text, final_err = _call_or_cache(
                            context,
                            merged_prompt,
                            {"repo": repo_item.repo_display, "branch": branch, "stage": "chunk_rollup"},
                        )
                        if final_err:
                            summary_text = None
                else:
                    summary_text, call_err = _call_or_cache(context, prompt, {"repo": repo_item.repo_display, "branch": branch})
                    if call_err:
                        ts = dt.datetime.now().strftime("%Y%m%d-%H%M%S")
                        (context.errors_dir / f"{ts}_{repo_item.repo_key}_{safe_branch}.log").write_text(
                            json.dumps(
                                {
                                    "repo": repo_item.repo_display,
                                    "branch": branch,
                                    "model": context.ollama_client.model if context.ollama_client else "",
                                    "url": context.ollama_client.url if context.ollama_client else "",
                                    "error": call_err,
                                    "max_patch_chars": context.max_patch_chars,
                                    "max_prompt_chars": context.max_prompt_chars,
                                },
                                indent=2,
                            ),
                            encoding="utf-8",
                        )

            if summary_text:
                (repo_item.repo_art_dir / f"{safe_branch}.summary.txt").write_text(summary_text + "\n", encoding="utf-8")
                repo_item.lines.append(f"### {branch}")
                repo_item.lines.append(summary_text)
                repo_item.lines.append("")
                repo_item.branch_rollups.append((branch, summary_text))
            else:
                (repo_item.repo_art_dir / f"{safe_branch}.summary.txt").write_text("ERROR: summary unavailable\n", encoding="utf-8")
                repo_item.lines.append(f"### {branch}")
                repo_item.lines.append("- Summary unavailable (see artifacts/errors logs).")


def build_rollups(context: PipelineContext) -> None:
    for repo_item in context.repo_items:
        if repo_item.branch_rollups and context.include_ollama:
            _emit("ROLLUP", f"repo {repo_item.repo_display} branches={len(repo_item.branch_rollups)}")
            rollup_prompt = truncate(
                build_repo_rollup_prompt(repo_item.repo_display, repo_item.branch_rollups),
                context.max_prompt_chars,
                suffix="\n\n[...prompt truncated...]\n",
            )
            (repo_item.repo_art_dir / "repo_rollup.prompt.txt").write_text(rollup_prompt, encoding="utf-8")
            rollup, rollup_err = _call_or_cache(context, rollup_prompt, {"repo": repo_item.repo_display, "stage": "repo_rollup"})
            if rollup:
                (repo_item.repo_art_dir / "repo_rollup.summary.txt").write_text(rollup + "\n", encoding="utf-8")
                context.project_summaries_for_master.append((repo_item.repo_display, rollup))
            elif rollup_err:
                context.errors.append(f"{repo_item.repo_display}: repo rollup failed: {rollup_err}")

        repo_item.lines.append("### Activity")
        if repo_item.activity_rollup:
            repo_item.lines.append(repo_item.activity_rollup.strip())
        else:
            repo_item.lines.append("- No activity rollup found for this project.")
        repo_item.lines.append("")
        _persist_project_summary(context, repo_item.repo_key, repo_item.lines)


def build_master_summary(context: PipelineContext) -> None:
    if not context.project_summaries_for_master or not context.include_ollama or context.ollama_client is None:
        return
    _emit("ROLLUP", f"master from repos={len(context.project_summaries_for_master)}")
    master_prompt = build_repo_rollup_prompt("ALL_REPOS", context.project_summaries_for_master)
    master_prompt = truncate(master_prompt, context.max_prompt_chars, suffix="\n\n[...prompt truncated...]\n")
    (context.artifacts_root / "master_summary.prompt.txt").write_text(master_prompt, encoding="utf-8")
    key = {
        "v": 4,
        "model": context.ollama_client.model,
        "timeout": context.ollama_client.timeout_s,
        "retries": context.ollama_client.retries,
        "scope": {"stage": "master_summary"},
        "prompt": master_prompt,
    }
    cpath = _cache_path(context.prompt_cache_dir, key)
    if cpath.exists():
        context.master_summary = cpath.read_text(encoding="utf-8").strip()
        (context.artifacts_root / "master_summary.summary.txt").write_text(context.master_summary + "\n", encoding="utf-8")
        return
    result = context.ollama_client.generate(master_prompt)
    if result.error:
        (context.artifacts_root / "master_summary.summary.txt").write_text(f"ERROR: {result.error.message}\n", encoding="utf-8")
        return
    context.master_summary = (result.text or "").strip()
    cpath.write_text(context.master_summary + "\n", encoding="utf-8")
    (context.artifacts_root / "master_summary.summary.txt").write_text(context.master_summary + "\n", encoding="utf-8")




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


def render_outputs(context: PipelineContext) -> PipelineRunResult:
    _emit("RENDER", "writing weeklySummary.markup")
    weekly_markup = _render_weekly_markup(
        context.ollama_client.model if context.ollama_client else "disabled",
        context.days,
        context.repo_sections,
        context.master_summary,
        context.activity_result.highlights_for_master,
    )
    weekly_file = context.out_dir / "weeklySummary.markup"
    weekly_file.write_text(weekly_markup, encoding="utf-8")

    _emit("RENDER", "writing weeklySummary.email.markup")
    weekly_email = render_email_markup(weekly_markup, title="Weekly Engineering Summary")
    (context.out_dir / "weeklySummary.email.markup").write_text(weekly_email, encoding="utf-8")

    logging.info("Wrote %s", weekly_file)
    return PipelineRunResult(
        exit_code=0,
        projects_processed=len(context.projects),
        branches_analyzed=context.branches_analyzed,
        artifacts_root=context.artifacts_root,
        errors=context.errors,
    )
