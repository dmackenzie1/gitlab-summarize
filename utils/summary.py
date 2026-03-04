import datetime as dt
import json
import logging
import re
import tempfile
from difflib import SequenceMatcher
from dataclasses import dataclass, field
from pathlib import Path

from utils.activity_logs import process_activity_logs
from utils.aider import AiderClient
from utils.git import (
    git_branch_has_recent_commits,
    git_checkout,
    git_diff_name_status,
    git_diff_numstat,
    git_diff_patch,
    git_diff_stat,
    git_ensure_clone,
    git_fetch_all,
    git_get_default_remote_branch,
    git_list_remote_branches,
    git_merge_base,
    git_recent_merge_commits,
    git_repo_dir_name_from_project,
    git_rev_parse,
    git_current_branch,
)
from utils.models import ActivitySummaryResult
from utils.ollama import OllamaClient
from utils.parsing import (
    chunk_text,
    coerce_text,
    is_noisy_path,
    path_is_version_signal,
    stable_json_hash,
    truncate,
    unique_preserve_order,
)
from utils.prompts import build_branch_prompt, build_repo_rollup_prompt

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
    branch_change_stats: list[tuple[str, int, int, int]] = field(default_factory=list)
    merge_count: int = 0

@dataclass
class PipelineContext:
    projects: list[dict]
    remote: str
    days: int
    out_dir: Path
    cache_dir: Path
    use_temp: bool
    offline: bool
    include_ollama: bool
    ollama_client: OllamaClient | None
    summarizer: str
    aider_cmd: str
    aider_model: str
    max_patch_chars: int
    max_prompt_chars: int
    max_files_in_patch: int
    force_resummarize: bool
    artifacts_root: Path
    prompt_cache_dir: Path
    errors_dir: Path
    project_summaries_dir: Path
    activity_result: ActivitySummaryResult = field(default_factory=lambda: ActivitySummaryResult({}, []))
    work_root: Path | None = None
    project_summaries_for_master: list[tuple[str, str]] = field(default_factory=list)
    repo_items: list[RepoWorkItem] = field(default_factory=list)
    branches_analyzed: int = 0
    errors: list[str] = field(default_factory=list)
    started_at: dt.datetime = field(default_factory=lambda: dt.datetime.now(dt.timezone.utc))

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
    offline: bool,
    include_ollama: bool,
    ollama_client: OllamaClient | None,
    summarizer: str,
    aider_cmd: str,
    aider_model: str,
    max_patch_chars: int,
    max_prompt_chars: int,
    max_files_in_patch: int,
    force_resummarize: bool,
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
        offline=offline,
        include_ollama=include_ollama,
        ollama_client=ollama_client,
        summarizer=summarizer,
        aider_cmd=aider_cmd,
        aider_model=aider_model,
        max_patch_chars=max_patch_chars,
        max_prompt_chars=max_prompt_chars,
        max_files_in_patch=max_files_in_patch,
        force_resummarize=force_resummarize,
        artifacts_root=artifacts_root,
        prompt_cache_dir=prompt_cache_dir,
        errors_dir=errors_dir,
        project_summaries_dir=project_summaries_dir,
    )

def extract_version_signals(repo_dir: Path, base: str, head: str, paths: list[str]) -> list[str]:
    from utils.git import git_run

    signals: list[str] = []
    for path in paths:
        res = git_run(repo_dir, "diff", "--unified=0", f"{base}..{head}", "--", path)
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

def _cache_path(cache_root: Path, key: dict) -> Path:
    return cache_root / f"{stable_json_hash(key)}.txt"

def _persist_project_summary(context: PipelineContext, repo_key: str, lines: list[str]) -> None:
    text = "\n".join(lines).rstrip() + "\n"
    (context.project_summaries_dir / f"{repo_key}.summary.markup").write_text(text, encoding="utf-8")

def _clean_wrapped_hyphenation(text: str) -> str:
    if not text:
        return ""
    text = text.replace("\u00ad", "")
    text = re.sub(r"(?<=\w)-\s+(?=\w)", "", text)
    return re.sub(r"\s+", " ", text).strip()

_SPECULATIVE_PHRASES = [
    "may",
    "might",
    "could potentially",
    "likely includes",
    "potentially incorporates",
    "requires verification",
]

def _strip_noise(text: str) -> str:
    cleaned = _clean_wrapped_hyphenation(text)
    cleaned = re.sub(r"\b\d{4}-\d{2}-\d{2}t\d{2}:\d{2}:\d{2}(?:\.\d+)?z?\b", "", cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r"\b[a-z0-9_-]*autobot[a-z0-9_-]*\b.*", "", cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r"^#+\s*", "", cleaned)
    cleaned = re.sub(r"\s+", " ", cleaned).strip()
    return cleaned.rstrip(";")

def _is_major_version_bump(text: str) -> bool:
    match = re.search(r"(\d+)\.\d+(?:\.\d+)?\s*(?:->|→|to)\s*(\d+)\.\d+(?:\.\d+)?", text)
    if not match:
        return False
    return int(match.group(1)) != int(match.group(2))

def _is_speculative_without_evidence(text: str) -> bool:
    lowered = text.lower()
    if not any(phrase in lowered for phrase in _SPECULATIVE_PHRASES):
        return False
    evidence_tokens = ["failed", "failure", "error", "breaking", "action required"]
    return not (_is_major_version_bump(text) or any(token in lowered for token in evidence_tokens))

def _clean_bullet(text: str) -> str:
    cleaned = _strip_noise(text)
    cleaned = re.sub(r"\s*;+\s*$", "", cleaned)
    return cleaned

def _extract_bullets(text: str) -> list[str]:
    bullets: list[str] = []
    for raw in text.splitlines():
        line = raw.strip()
        if not line:
            continue
        if line.startswith(("- ", "* ", "• ")):
            candidate = line[2:].strip()
        elif re.match(r"^\d+[\.)]\s+", line):
            candidate = re.sub(r"^\d+[\.)]\s+", "", line)
        else:
            candidate = line
        candidate = _clean_bullet(candidate)
        if candidate and not _is_speculative_without_evidence(candidate):
            bullets.append(candidate)
    return bullets

def _normalized_text(text: str) -> str:
    cleaned = _clean_wrapped_hyphenation(text).lower()
    cleaned = re.sub(r"[^a-z0-9\s]", "", cleaned)
    return re.sub(r"\s+", " ", cleaned).strip()

def _dedupe_bullets(bullets: list[str]) -> list[str]:
    unique: list[str] = []
    normalized: list[str] = []
    for bullet in bullets:
        key = _normalized_text(bullet)
        if not key:
            continue
        if key in normalized:
            continue
        if any(SequenceMatcher(None, key, prior).ratio() >= 0.92 for prior in normalized):
            continue
        unique.append(bullet)
        normalized.append(key)
    return unique

def _score_bullet(text: str) -> int:
    score = 1
    lowered = text.lower()
    high_signal_terms = [
        "major", "migration", "breaking", "bump", "upgrade", "security", "schema", "database",
        "node", "python", "ci", "pipeline", "deploy", "feature", "api", "socket", "template",
    ]
    for term in high_signal_terms:
        if term in lowered:
            score += 2
    if re.search(r"\bv?\d+\.\d+(\.\d+)?\b", lowered):
        score += 1
    return score

def _top_bullets(bullets: list[str], limit: int) -> list[str]:
    ranked = sorted(bullets, key=lambda item: (_score_bullet(item), len(item)), reverse=True)
    return ranked[:limit]

def _normalize_for_cross_project(text: str) -> str:
    normalized = _normalized_text(text)
    normalized = re.sub(r"\bemss\b", "", normalized)
    normalized = re.sub(r"\b(repo|project|projects|pipeline|branch|merge|commit)s?\b", "", normalized)
    normalized = re.sub(r"\s+", " ", normalized).strip()
    return normalized

def _build_project_bullets(context: PipelineContext) -> dict[str, list[str]]:
    project_bullets: dict[str, list[str]] = {}
    for repo_item in sorted(context.repo_items, key=lambda item: item.repo_display.lower()):
        combined: list[str] = []
        for _, summary in repo_item.branch_rollups:
            combined.extend(_extract_bullets(summary))
        if repo_item.activity_rollup:
            combined.extend(_extract_bullets(repo_item.activity_rollup))
        project_bullets[repo_item.repo_display] = _dedupe_bullets(combined)
    return project_bullets

def _build_cross_project_updates(project_bullets: dict[str, list[str]]) -> tuple[list[str], set[str]]:
    bucket: dict[str, list[tuple[str, str]]] = {}
    for repo, bullets in project_bullets.items():
        for bullet in bullets:
            key = _normalize_for_cross_project(bullet)
            if key:
                bucket.setdefault(key, []).append((repo, bullet))

    repeated = [(key, entries) for key, entries in bucket.items() if len(entries) >= 2]
    repeated.sort(key=lambda item: (-len(item[1]), -_score_bullet(item[1][0][1]), item[0]))

    lines: list[str] = []
    used_keys: set[str] = set()
    for key, entries in repeated[:8]:
        used_keys.add(key)
        sample = entries[0][1]
        repos = sorted({repo for repo, _ in entries})
        if len(repos) > 4:
            repo_text = f"{len(repos)} repos ({', '.join(repos[:3])})"
        else:
            repo_text = ", ".join(repos)
        lines.append(f"{sample} (repos: {repo_text})")
    return lines[:8], used_keys

def _is_version_or_library_update(text: str) -> bool:
    lowered = text.lower()
    markers = ["version", "upgrade", "bump", "dependency", "dependencies", "node", "python", "npm", "library"]
    return any(marker in lowered for marker in markers) or bool(re.search(r"\bv?\d+\.\d+(\.\d+)?\b", lowered))

def _build_overall_highlights(context: PipelineContext, cross_project_updates: list[str], project_bullets: dict[str, list[str]]) -> list[str]:
    highlights: list[str] = []
    active_branch_total = sum(len(item.active_branches) for item in context.repo_items)
    if active_branch_total:
        highlights.append(f"{active_branch_total} active branches were analyzed across {len(project_bullets)} projects this week.")

    merge_total = sum(item.merge_count for item in context.repo_items)
    if merge_total:
        highlights.append(f"{merge_total} merge commits landed in the reporting window.")

    branch_deltas = [entry + (item.repo_display,) for item in context.repo_items for entry in item.branch_change_stats]
    if branch_deltas:
        branch, files_changed, adds, deletes, repo = max(branch_deltas, key=lambda row: row[2] + row[3])
        highlights.append(
            f"Largest code delta: {repo} {branch} touched {files_changed} files with {adds + deletes} total line changes ({adds} additions, {deletes} deletions)."
        )

    shared_versions = [line for line in cross_project_updates if _is_version_or_library_update(line)]
    if shared_versions:
        highlights.append(f"Shared version/library updates: {len(shared_versions)} patterns repeated across repos.")

    fallback = "Engineering delivery focused on meaningful runtime, API, and deployment changes this week."
    return (highlights[:6] or [fallback])[:6]

def _management_summary_from_bullets(bullets: list[str]) -> str:
    if not bullets:
        return "No material engineering changes were captured for this project."
    top = _top_bullets(bullets, 2)
    if len(top) == 1:
        return _clean_bullet(top[0]).rstrip(".") + "."
    return _sentence_from_bullets([_clean_bullet(top[0]), _clean_bullet(top[1])], 2)

def _important_unique_bullets(bullets: list[str], limit: int) -> list[str]:
    ranked = _top_bullets(_dedupe_bullets(bullets), max(limit * 2, limit))
    important = [b for b in ranked if _score_bullet(b) >= 3]
    return (important or ranked)[:limit]

def _action_needed(bullets: list[str], status: str) -> list[str]:
    needs_action = [
        b for b in bullets
        if any(token in b.lower() for token in ["migration", "manual", "todo", "action required", "failed", "error", "config required"])
    ]
    if status == "SUCCESS":
        needs_action = [b for b in needs_action if "verify pipeline" not in b.lower()]
    return _top_bullets(_dedupe_bullets(needs_action), 3)

def _sentence_from_bullets(bullets: list[str], limit: int) -> str:
    selected = bullets[:limit]
    if not selected:
        return "No significant engineering deltas were captured this week."
    text = "; ".join(selected)
    return text[0].upper() + text[1:] + ("." if not text.endswith(".") else "")

def _render_weekly_email_html(context: PipelineContext, days: int) -> str:
    generated = dt.datetime.now().astimezone().strftime("%Y-%m-%d %H:%M %Z")
    duration = dt.datetime.now(dt.timezone.utc) - context.started_at
    duration_min = max(1, int(duration.total_seconds() // 60))
    status = "SUCCESS" if not context.errors else "SUCCESS (with warnings)"

    project_bullets = _build_project_bullets(context)
    cross_project_updates, cross_keys = _build_cross_project_updates(project_bullets)
    overall_highlights = _build_overall_highlights(context, cross_project_updates, project_bullets)

    parts = [
        '<html><body style="font-family:Arial,Helvetica,sans-serif;color:#222;line-height:1.4">',
        '<h2 style="margin:0 0 8px">Weekly Engineering Summary</h2>',
        '<p style="margin:0 0 12px">'
        f"Generated: {generated}<br>"
        f"Window: last {days} days<br>"
        f"Pipeline status: {status} · Duration: ~{duration_min} min<br>"
        f"Projects analyzed: {len(context.projects)}"
        + "</p>",
        "<h3>Overall Highlights</h3>",
        "<ul>",
    ]
    for bullet in overall_highlights[:6]:
        parts.append(f"<li>{bullet}</li>")
    parts.append("</ul>")

    shared_version_updates = [note for note in cross_project_updates if _is_version_or_library_update(note)]
    if shared_version_updates:
        parts.append("<h3>Shared Library/Version Updates</h3><ul>")
        for note in shared_version_updates[:6]:
            parts.append(f"<li>{note}</li>")
        parts.append("</ul>")

    for repo_item in sorted(context.repo_items, key=lambda item: item.repo_display.lower()):
        combined = project_bullets.get(repo_item.repo_display, [])
        filtered = [b for b in combined if _normalize_for_cross_project(b) not in cross_keys]
        unique_details = _important_unique_bullets(filtered, 5)
        if not unique_details and not combined:
            continue

        key_changes = unique_details if unique_details else _important_unique_bullets(combined, 3)
        if len(key_changes) < 2 and combined:
            key_changes = _important_unique_bullets(combined, 2)

        parts.append(f"<h3>{repo_item.repo_display}</h3>")
        parts.append(f"<p><strong>Management Summary:</strong> {_management_summary_from_bullets(key_changes)}</p>")
        parts.append("<p><strong>Key Changes:</strong></p><ul>")
        for bullet in key_changes[:5]:
            parts.append(f"<li>{bullet}</li>")
        parts.append("</ul>")

        actions = _action_needed(key_changes, "SUCCESS" if not context.errors else "SUCCESS (with warnings)")
        if actions:
            parts.append("<p><strong>Action Needed:</strong></p><ul>")
            for bullet in actions:
                parts.append(f"<li>{bullet}</li>")
            parts.append("</ul>")

    parts.append("</body></html>\n")
    return "\n".join(parts)

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
        repo_key = git_repo_dir_name_from_project(project_name, ssh_url)
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

        ok, err = git_ensure_clone(ssh_url, repo_dir)
        if not ok:
            work_item.lines.append(f"- Clone error: {err}")
            context.errors.append(f"{repo_display}: clone error: {err}")
            _emit("REPO", f"{repo_display} clone failed")
            context.repo_items.append(work_item)
            continue
        if not context.offline:
            git_fetch_all(repo_dir)
        elif not (repo_dir / ".git").exists():
            work_item.lines.append("- Offline mode: local clone missing in cache.")
            context.errors.append(f"{repo_display}: offline mode requires an existing local clone")
            context.repo_items.append(work_item)
            continue

        parent = git_get_default_remote_branch(repo_dir, context.remote) or f"{context.remote}/main"
        branches, b_err = git_list_remote_branches(repo_dir, context.remote)
        if b_err or branches is None:
            msg = b_err or "unknown"
            work_item.lines.append(f"- Branch listing error: {msg}")
            context.errors.append(f"{repo_display}: branch listing error: {msg}")
            _emit("REPO", f"{repo_display} branch listing failed")
            context.repo_items.append(work_item)
            continue

        active = [b for b in branches if git_branch_has_recent_commits(repo_dir, b, f"{context.days} days ago")]
        work_item.parent = parent
        work_item.active_branches = active
        work_item.lines.append(f"- Active branches in window: {len(active)}")
        _emit("REPO", f"{repo_display} active_branches={len(active)}")

        merges = git_recent_merge_commits(repo_dir, days=context.days)
        work_item.merge_count = len(merges)
        if merges:
            work_item.lines.append("- Recent merge commits:")
            for m in merges[:8]:
                work_item.lines.append(f"  - {m['date']} {m['subject']} ({m['author']})")
        context.repo_items.append(work_item)

def _call_or_cache(context: PipelineContext, prompt: str, scope: dict, force_refresh: bool = False) -> tuple[str | None, str | None]:
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
    if cpath.exists() and not force_refresh:
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
            patch_path = repo_item.repo_art_dir / f"{safe_branch}.patch.txt"
            prompt_path = repo_item.repo_art_dir / f"{safe_branch}.prompt.txt"
            summary_path = repo_item.repo_art_dir / f"{safe_branch}.summary.txt"
            if branch == repo_item.parent:
                summary_path.write_text(
                    "Baseline branch (parent). No diff computed.\n", encoding="utf-8"
                )
                continue

            base = git_merge_base(repo_item.repo_dir, repo_item.parent, branch)
            head = git_rev_parse(repo_item.repo_dir, branch)
            if not base or not head:
                continue

            stat = git_diff_stat(repo_item.repo_dir, base, head)
            name_status = git_diff_name_status(repo_item.repo_dir, base, head)
            num = git_diff_numstat(repo_item.repo_dir, base, head)
            additions = sum(added for _, added, _ in num)
            deletions = sum(deleted for _, _, deleted in num)
            repo_item.branch_change_stats.append((branch, len(num), additions, deletions))

            if patch_path.exists() and prompt_path.exists() and summary_path.exists() and not context.force_resummarize:
                existing_summary = summary_path.read_text(encoding="utf-8").strip()
                _emit("BRANCH", f"{repo_item.repo_display} {branch} reused_existing_artifacts=true")
                if existing_summary and not existing_summary.startswith("ERROR:"):
                    repo_item.lines.append(f"### {branch}")
                    repo_item.lines.append(existing_summary)
                    repo_item.lines.append("")
                    repo_item.branch_rollups.append((branch, existing_summary))
                else:
                    repo_item.lines.append(f"### {branch}")
                    repo_item.lines.append("- Summary unavailable (existing artifact contains error).")
                continue

            candidate_paths = [path for path, _, _ in num if not is_noisy_path(path)]
            chosen_paths = candidate_paths[: context.max_files_in_patch]
            changed_paths = [row.split("	")[-1] for row in name_status if "	" in row]
            version_paths = [path for path in changed_paths if path_is_version_signal(path)]
            version_signals = extract_version_signals(repo_item.repo_dir, base, head, version_paths)

            if context.force_resummarize and patch_path.exists():
                patch = truncate(patch_path.read_text(encoding="utf-8"), context.max_patch_chars)
            else:
                patch = truncate(git_diff_patch(repo_item.repo_dir, base, head, chosen_paths), context.max_patch_chars)
                patch_path.write_text(patch, encoding="utf-8")

            prompt = truncate(
                build_branch_prompt(repo_item.repo_display, branch, repo_item.parent, stat, patch, version_signals),
                context.max_prompt_chars,
                suffix="\n\n[...prompt truncated...]\n",
            )
            prompt_path.write_text(prompt, encoding="utf-8")

            summary_text: str | None = None
            if context.summarizer == "aider":
                orig_branch = git_current_branch(repo_item.repo_dir)
                aider_client = AiderClient(context.aider_cmd, context.aider_model)
                checkout_err = None
                if orig_branch:
                    checkout_res = git_checkout(repo_item.repo_dir, branch)
                    if not checkout_res.ok:
                        checkout_err = checkout_res.stderr.strip() or "checkout failed"
                if checkout_err:
                    summary_text = None
                    ts = dt.datetime.now().strftime("%Y%m%d-%H%M%S")
                    (context.errors_dir / f"{ts}_{repo_item.repo_key}_{safe_branch}_checkout.log").write_text(
                        json.dumps({"repo": repo_item.repo_display, "branch": branch, "error": checkout_err}, indent=2),
                        encoding="utf-8",
                    )
                else:
                    aider_result = aider_client.summarize_branch_diff(
                        repo_dir=repo_item.repo_dir,
                        repo_display=repo_item.repo_display,
                        branch=branch,
                        parent_branch=repo_item.parent,
                        stat=stat,
                        patch=patch,
                    )
                    summary_text = aider_result.text
                    if aider_result.error:
                        call_err = aider_result.error
                        ts = dt.datetime.now().strftime("%Y%m%d-%H%M%S")
                        (context.errors_dir / f"{ts}_{repo_item.repo_key}_{safe_branch}_aider.log").write_text(
                            json.dumps({"repo": repo_item.repo_display, "branch": branch, "error": call_err}, indent=2),
                            encoding="utf-8",
                        )
                if orig_branch and orig_branch != branch:
                    git_checkout(repo_item.repo_dir, orig_branch)
            elif context.include_ollama:
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
                        chunk_summary, chunk_err = _call_or_cache(
                            context,
                            chunk_prompt,
                            {"repo": repo_item.repo_display, "branch": branch, "chunk": i},
                            force_refresh=context.force_resummarize,
                        )
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
                            force_refresh=context.force_resummarize,
                        )
                        if final_err:
                            summary_text = None
                else:
                    summary_text, call_err = _call_or_cache(
                        context,
                        prompt,
                        {"repo": repo_item.repo_display, "branch": branch},
                        force_refresh=context.force_resummarize,
                    )
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
                summary_path.write_text(summary_text + "\n", encoding="utf-8")
                repo_item.lines.append(f"### {branch}")
                repo_item.lines.append(summary_text)
                repo_item.lines.append("")
                repo_item.branch_rollups.append((branch, summary_text))
            else:
                summary_path.write_text("ERROR: summary unavailable\n", encoding="utf-8")
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
            rollup, rollup_err = _call_or_cache(context, rollup_prompt, {"repo": repo_item.repo_display, "stage": "repo_rollup"}, force_refresh=context.force_resummarize)
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
    if cpath.exists() and not context.force_resummarize:
        summary_text = cpath.read_text(encoding="utf-8").strip()
        (context.artifacts_root / "master_summary.summary.txt").write_text(summary_text + "\n", encoding="utf-8")
        return
    result = context.ollama_client.generate(master_prompt)
    if result.error:
        (context.artifacts_root / "master_summary.summary.txt").write_text(f"ERROR: {result.error.message}\n", encoding="utf-8")
        return
    summary_text = (result.text or "").strip()
    cpath.write_text(summary_text + "\n", encoding="utf-8")
    (context.artifacts_root / "master_summary.summary.txt").write_text(summary_text + "\n", encoding="utf-8")

def _render_weekly_markup(
    context: PipelineContext,
    days: int,
) -> str:
    stamp = dt.datetime.now().astimezone().strftime("%Y-%m-%d %H:%M %Z")
    duration = dt.datetime.now(dt.timezone.utc) - context.started_at
    duration_min = max(1, int(duration.total_seconds() // 60))
    status = "SUCCESS" if not context.errors else "SUCCESS (with warnings)"
    project_bullets = _build_project_bullets(context)
    cross_project_updates, cross_keys = _build_cross_project_updates(project_bullets)
    overall_highlights = _build_overall_highlights(context, cross_project_updates, project_bullets)

    header = [
        "# Weekly Engineering Summary",
        f"- Generated: {stamp}",
        f"- Window: last {days} days",
        f"- Pipeline status: {status} | Duration: ~{duration_min} min",
        f"- Projects analyzed: {len(context.projects)}",
        "",
    ]

    lines = header + ["## Overall Highlights"]
    for bullet in overall_highlights[:6]:
        lines.append(f"- {bullet}")

    shared_version_updates = [note for note in cross_project_updates if _is_version_or_library_update(note)]
    if shared_version_updates:
        lines.append("\n## Shared Library/Version Updates")
        for bullet in shared_version_updates[:6]:
            lines.append(f"- {bullet}")

    for repo_item in sorted(context.repo_items, key=lambda item: item.repo_display.lower()):
        combined = project_bullets.get(repo_item.repo_display, [])
        filtered = [b for b in combined if _normalize_for_cross_project(b) not in cross_keys]
        key_changes = _important_unique_bullets(filtered, 5)
        if len(key_changes) < 2 and combined:
            key_changes = _important_unique_bullets(combined, 2)

        lines.extend(["", f"## {repo_item.repo_display}", f"- Management Summary: {_management_summary_from_bullets(key_changes)}", "- Key Changes:"])
        for bullet in key_changes[:5]:
            lines.append(f"  - {bullet}")
        actions = _action_needed(key_changes, status)
        if actions:
            lines.append("- Action Needed:")
            for bullet in actions:
                lines.append(f"  - {bullet}")

    lines.append("")
    return "\n".join(lines)

def render_outputs(context: PipelineContext) -> PipelineRunResult:
    _emit("RENDER", "writing weeklySummary.markup")
    weekly_markup = _render_weekly_markup(
        context,
        context.days,
    )
    weekly_file = context.out_dir / "weeklySummary.markup"
    weekly_file.write_text(weekly_markup, encoding="utf-8")

    _emit("RENDER", "writing weeklySummary.email.markup")
    weekly_email = _render_weekly_email_html(
        context,
        context.days,
    )
    (context.out_dir / "weeklySummary.email.markup").write_text(weekly_email, encoding="utf-8")

    logging.info("Wrote %s", weekly_file)
    return PipelineRunResult(
        exit_code=0,
        projects_processed=len(context.projects),
        branches_analyzed=context.branches_analyzed,
        artifacts_root=context.artifacts_root,
        errors=context.errors,
    )
