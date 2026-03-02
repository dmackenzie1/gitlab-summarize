#!/usr/bin/env python3
"""
generate_summary.py (iteration)

Goal: weekly summary that is SHORT and based on branch *content*, not commit messages.
- Diff each active branch vs its parent (merge-base(parent, branch)..branch)
- Send a *curated* diff bundle to local Ollama for summarization
- weekly_summary.md contains ONLY:
    - repo/branch + parent + diff range
    - a few "version/build signals" (if detected)
    - Ollama's summary (diff-grounded)
  It does NOT include raw diffs/patches (those go to temp artifact files).

Fixes vs prior iteration
- UnicodeDecodeError: all subprocess output is treated as bytes and decoded as UTF-8 with errors='replace'
- Ollama input: JSON is built with json.dumps (no hand-built JSON); tabs are replaced; control chars removed
- Avoid gigantic prompts: excludes lockfiles/big/noisy files from patch; caps prompt chars
- Raw diffs are written to an artifacts directory (temp by default, or --artifacts-dir)

Run
  python3 generate_summary.py
  python3 generate_summary.py --days 10
  python3 generate_summary.py --no-ollama
  python3 generate_summary.py --temp
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import re
import sys
import tempfile
import urllib.request
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, List, Optional, Tuple


# ----------------------------
# Repo list
# Keep all repos here; comment out ones you don't want temporarily.
# ----------------------------

REPOS_DEFAULT = [
    # In-scope now:
    "git@jsfitpeegitlab.ndc.nasa.gov:emss/logs.git",
    # "git@jsfitpeegitlab.ndc.nasa.gov:emss/acme.git",
    "git@jsfitpeegitlab.ndc.nasa.gov:emss/aegis.git",
    "git@jsfitpeegitlab.ndc.nasa.gov:emss/coda.git",
    # "git@jsfitpeegitlab.ndc.nasa.gov:emss/coda-streamers.git",
    # "git@jsfitpeegitlab.ndc.nasa.gov:emss/docker-images.git",
    "git@jsfitpeegitlab.ndc.nasa.gov:emss/gitlab-templates.git",
    "git@jsfitpeegitlab.ndc.nasa.gov:emss/maestro.git",
    "git@jsfitpeegitlab.ndc.nasa.gov:emss/packages.git",
    "git@jsfitpeegitlab.ndc.nasa.gov:emss/talky-bot.git",
    # "git@jsfitpeegitlab.ndc.nasa.gov:emss/talky-capture.git",
    "git@jsfitpeegitlab.ndc.nasa.gov:emss/talky-transcribe.git",
    # "git@jsfitpeegitlab.ndc.nasa.gov:emss/texty-bot.git",
]

REMOTE_DEFAULT = "origin"

# Local Ollama
OLLAMA_URL_DEFAULT = "http://localhost:11434/api/generate"
OLLAMA_MODEL_DEFAULT = "qwen2.5-coder:32b"

# Caps
DAYS_DEFAULT = 10
MAX_PROMPT_CHARS_DEFAULT = 55_000
MAX_PATCH_CHARS_DEFAULT = 40_000
MAX_FILES_IN_PATCH_DEFAULT = 12

# Exclude these from patch sent to Ollama (still scanned for version/build signals)
NOISY_FILES = {
    "package-lock.json",
    "pnpm-lock.yaml",
    "yarn.lock",
    "uv.lock",
    "poetry.lock",
}
NOISY_SUFFIXES = (".lock", ".min.js", ".map")
NOISY_PATH_CONTAINS = ("/node_modules/", "/dist/", "/build/", "/.venv/")

# Files/paths we still scan for version/build signals
VERSION_SIGNAL_PATHS = [
    ".nvmrc",
    ".node-version",
    "package.json",
    "Dockerfile",
    "docker/",
    ".gitlab-ci.yml",
    ".gitlab-ci/",
    "pyproject.toml",
    "requirements.txt",
    "requirements-dev.txt",
    "uv.lock",
    "package-lock.json",
    "pnpm-lock.yaml",
    "yarn.lock",
    "poetry.lock",
]


@dataclass
class CmdResult:
    ok: bool
    cmd: List[str]
    stdout: str
    stderr: str
    returncode: int


def _decode(b: bytes) -> str:
    return (b or b"").decode("utf-8", errors="replace")


def run_cmd(cmd: List[str], cwd: Optional[Path] = None) -> CmdResult:
    try:
        import subprocess

        p = subprocess.run(
            cmd,
            cwd=str(cwd) if cwd else None,
            capture_output=True,
            text=False,
            check=False,
        )
        return CmdResult(
            ok=(p.returncode == 0),
            cmd=cmd,
            stdout=_decode(p.stdout),
            stderr=_decode(p.stderr),
            returncode=p.returncode,
        )
    except Exception as e:
        return CmdResult(ok=False, cmd=cmd, stdout="", stderr=str(e), returncode=127)


def git(repo_dir: Path, *args: str) -> CmdResult:
    return run_cmd(["git", "-C", str(repo_dir), *args])


def repo_name_from_url(url: str) -> str:
    m = re.search(r":([^/]+)/([^/]+)\.git$", url.strip())
    if m:
        return m.group(2)
    base = url.strip().split("/")[-1]
    return base[:-4] if base.endswith(".git") else base


def unique_preserve_order(items: Iterable[str]) -> List[str]:
    seen = set()
    out: List[str] = []
    for x in items:
        x = x.strip()
        if not x or x in seen:
            continue
        seen.add(x)
        out.append(x)
    return out


def ensure_clone(repo_url: str, target_dir: Path, verbose: bool) -> Tuple[bool, Optional[str]]:
    if (target_dir / ".git").exists():
        return True, None
    target_dir.parent.mkdir(parents=True, exist_ok=True)
    if verbose:
        print(f"[clone] {repo_url} -> {target_dir}", file=sys.stderr)
    res = run_cmd(["git", "clone", "--no-tags", repo_url, str(target_dir)])
    if not res.ok:
        return False, res.stderr.strip() or f"git clone failed (exit {res.returncode})"
    return True, None


def fetch_all(repo_dir: Path) -> CmdResult:
    return git(repo_dir, "fetch", "--all", "--prune")


def get_default_remote_branch(repo_dir: Path, remote: str) -> Optional[str]:
    res = git(repo_dir, "symbolic-ref", "-q", f"refs/remotes/{remote}/HEAD")
    if not res.ok:
        return None
    ref = res.stdout.strip()
    if ref.startswith("refs/remotes/"):
        return ref.replace("refs/remotes/", "")
    return None


def list_remote_branches(repo_dir: Path, remote: str) -> Tuple[Optional[List[str]], Optional[str]]:
    res = git(repo_dir, "for-each-ref", "--format=%(refname:short)", f"refs/remotes/{remote}")
    if not res.ok:
        return None, res.stderr.strip() or f"git for-each-ref failed (exit {res.returncode})"
    branches: List[str] = []
    for line in res.stdout.splitlines():
        line = line.strip()
        if not line or line.endswith("/HEAD") or line == remote:
            continue
        branches.append(line)
    branches.sort()
    return branches, None


def branch_has_recent_commits(repo_dir: Path, branch: str, since: str) -> bool:
    res = git(repo_dir, "rev-list", "--count", f"--since={since}", branch)
    if not res.ok:
        return False
    try:
        return int(res.stdout.strip() or "0") > 0
    except ValueError:
        return False


def merge_base(repo_dir: Path, a: str, b: str) -> Optional[str]:
    res = git(repo_dir, "merge-base", a, b)
    sha = res.stdout.strip() if res.ok else ""
    return sha or None


def rev_parse(repo_dir: Path, ref: str) -> Optional[str]:
    res = git(repo_dir, "rev-parse", ref)
    sha = res.stdout.strip() if res.ok else ""
    return sha or None


def diff_stat(repo_dir: Path, base: str, head: str) -> Optional[str]:
    res = git(repo_dir, "diff", "--stat", f"{base}..{head}")
    return res.stdout.strip() if res.ok else None


def diff_name_status(repo_dir: Path, base: str, head: str) -> Optional[List[str]]:
    res = git(repo_dir, "diff", "--name-status", f"{base}..{head}")
    if not res.ok:
        return None
    return [ln.strip() for ln in res.stdout.splitlines() if ln.strip()]


def diff_numstat(repo_dir: Path, base: str, head: str) -> List[Tuple[str, int]]:
    res = git(repo_dir, "diff", "--numstat", f"{base}..{head}")
    if not res.ok:
        return []
    out: List[Tuple[str, int]] = []
    for ln in res.stdout.splitlines():
        parts = ln.split("\t")
        if len(parts) != 3:
            continue
        add_s, del_s, path = parts
        if add_s == "-" or del_s == "-":
            continue
        try:
            out.append((path, int(add_s) + int(del_s)))
        except ValueError:
            continue
    out.sort(key=lambda x: x[1], reverse=True)
    return out


def is_noisy_path(path: str) -> bool:
    p = path.replace("\\", "/")
    if p.split("/")[-1] in NOISY_FILES:
        return True
    if p.endswith(NOISY_SUFFIXES):
        return True
    for frag in NOISY_PATH_CONTAINS:
        if frag in p:
            return True
    return False


def path_is_version_signal(path: str) -> bool:
    p = path.replace("\\", "/").lstrip("./")
    for sig in VERSION_SIGNAL_PATHS:
        if sig.endswith("/"):
            if p.startswith(sig):
                return True
        else:
            if p == sig or p.endswith("/" + sig):
                return True
    return False


def diff_patch(repo_dir: Path, base: str, head: str, paths: List[str], max_chars: int) -> str:
    res = git(repo_dir, "diff", f"{base}..{head}", "--patch", "--no-color", "--minimal", "--", *paths)
    txt = res.stdout if res.ok else ""
    if len(txt) > max_chars:
        txt = txt[:max_chars] + "\n\n[...truncated...]\n"
    return txt


_VERSION_PATTERNS = [
    re.compile(r"\bnode\b[^\n]*\b(\d+\.\d+\.\d+|\d+)\b", re.IGNORECASE),
    re.compile(r"\bnpm\b[^\n]*\b(\d+\.\d+\.\d+|\d+)\b", re.IGNORECASE),
    re.compile(r"FROM\s+([^\s:]+):([^\s]+)", re.IGNORECASE),
    re.compile(r"\bpython\b[^\n]*\b(\d+\.\d+(\.\d+)?)\b", re.IGNORECASE),
    re.compile(r"\bkaniko\b[^\n]*\b(v?\d+\.\d+\.\d+)\b", re.IGNORECASE),
]


def extract_version_signals_from_files(repo_dir: Path, base: str, head: str, paths: List[str]) -> List[str]:
    signals: List[str] = []
    for p in paths:
        res = git(repo_dir, "diff", "--unified=0", f"{base}..{head}", "--", p)
        if not res.ok:
            continue
        for ln in res.stdout.splitlines():
            if not ln.startswith(("+", "-")) or ln.startswith(("+++", "---")):
                continue
            if ln[1:].strip() == "":
                continue
            for rx in _VERSION_PATTERNS:
                if rx.search(ln):
                    s = ln[1:].strip()
                    if len(s) > 240:
                        s = s[:240] + "..."
                    signals.append(f"{p}: {s}")
                    break
    seen = set()
    out: List[str] = []
    for s in signals:
        if s in seen:
            continue
        seen.add(s)
        out.append(s)
    return out


def sanitize_prompt(text: str) -> str:
    text = text.replace("\t", "    ")
    text = re.sub(r"[\x00-\x08\x0b-\x1f\x7f]", "", text)
    return text


def call_ollama(ollama_url: str, model: str, prompt: str, timeout_s: int) -> Tuple[Optional[str], Optional[str]]:
    payload = {"model": model, "stream": False, "prompt": prompt}
    data = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    req = urllib.request.Request(ollama_url, data=data, headers={"Content-Type": "application/json"}, method="POST")
    try:
        with urllib.request.urlopen(req, timeout=timeout_s) as resp:
            body = resp.read().decode("utf-8", errors="replace")
    except Exception as e:
        return None, f"Ollama request failed: {e}"
    try:
        obj = json.loads(body)
    except Exception:
        snippet = body[:600] + ("..." if len(body) > 600 else "")
        return None, f"Could not parse Ollama JSON response. Snippet: {snippet}"
    text = obj.get("response", "")
    if not isinstance(text, str) or not text.strip():
        return None, "Ollama returned empty response"
    return text.strip(), None


def build_prompt(repo: str, branch: str, parent: str, diffstat: str, patch: str, version_signals: List[str]) -> str:
    vs = "\n".join(f"- {s}" for s in version_signals[:40]) or "(none detected)"
    return sanitize_prompt(
        "You are summarizing code changes for a weekly engineering report.\n"
        "Do NOT rely on commit messages or authors. Use only the diff evidence provided.\n\n"
        f"Repo: {repo}\n"
        f"Branch: {branch}\n"
        f"Parent: {parent}\n\n"
        f"DIFFSTAT:\n{diffstat}\n\n"
        "VERSION/BUILD SIGNALS (extracted from lockfiles/CI/Docker where applicable):\n"
        f"{vs}\n\n"
        "PATCH (curated; may be truncated):\n"
        f"{patch}\n\n"
        "Return:\n"
        "- 3-8 bullet points: what changed in behavior/capabilities, grounded in the diff\n"
        "- 0-3 bullet points: risks/breaking changes/migrations\n"
        "- 0-3 bullet points: notable version/build changes\n"
    )


def generate(
    repo_urls: List[str],
    remote: str,
    days: int,
    out_path: Path,
    cache_dir: Path,
    use_temp: bool,
    artifacts_dir: Optional[Path],
    include_ollama: bool,
    ollama_url: str,
    ollama_model: str,
    max_patch_chars: int,
    max_prompt_chars: int,
    max_files_in_patch: int,
    verbose: bool,
) -> int:
    logging.info(f"Generating summary for {len(repo_urls)} repos with days={days}")
    repo_urls = unique_preserve_order(repo_urls)
    since = f"{days}.days.ago"

    temp_artifacts_ctx = None
    if artifacts_dir is None:
        artifacts_root = out_path.parent / 'artifacts'
    else:
        artifacts_root = artifacts_dir

    artifacts_root.mkdir(parents=True, exist_ok=True)

    md: List[str] = []
    md.append("# Weekly Branch Content Summary\n\n")
    md.append(f"_Generated: {dt.datetime.now().astimezone().strftime('%Y-%m-%d %H:%M %Z')}_\n\n")
    md.append(f"_Branch activity window: commits since **{since}**_\n\n")
    md.append("_Diff method: merge-base(parent, branch)..branch_\n\n")
    md.append(f"_Mode: `{'temp' if use_temp else 'cache'}` | Remote: `{remote}` | Repos: {len(repo_urls)}_\n\n")
    md.append(f"_Artifacts: diffs/prompts stored under `{artifacts_root}` (not embedded in this markdown)_\n\n")
    md.append(f"_Ollama: {'enabled' if include_ollama else 'disabled'} | URL: `{ollama_url}` | Model: `{ollama_model}`_\n\n")

    def process_all(work_root: Path) -> None:
        for repo_url in repo_urls:
            repo = repo_name_from_url(repo_url)
            repo_dir = work_root / repo

            md.append(f"## Repo: {repo}\n\n")

            ok, err = ensure_clone(repo_url, repo_dir, verbose=verbose)
            if not ok:
                logging.error(f"Error cloning repo {repo_url}: {err}")
                md.append("**Error cloning repo**\n\n")
                md.append(f"{err}\n\n")
                continue

            fetch_res = fetch_all(repo_dir)
            if not fetch_res.ok:
                logging.error(f"Error fetching remotes for repo {repo}: {fetch_res.stderr.strip()}")
                md.append("**Error fetching remotes**\n\n")
                md.append(f"{fetch_res.stderr.strip()}\n\n")

            parent = get_default_remote_branch(repo_dir, remote) or f"{remote}/main"
            md.append(f"_Parent baseline_: `{parent}`\n\n")

            branches, b_err = list_remote_branches(repo_dir, remote)
            if b_err or branches is None:
                logging.error(f"Error listing branches for repo {repo}: {b_err or 'unknown'}")
                md.append("**Error listing branches**\n\n")
                md.append(f"{b_err or 'unknown'}\n\n")
                continue

            active = [br for br in branches if branch_has_recent_commits(repo_dir, br, since)]
            md.append(f"_Active branches_: {len(active)}/{len(branches)}\n\n")
            if not active:
                continue

            for br in active:
                if br == parent:
                    continue

                base = merge_base(repo_dir, parent, br)
                head = rev_parse(repo_dir, br)
                if not base or not head:
                    logging.error(f"Error resolving diff base/head for branch {br} in repo {repo}")
                    md.append(f"### Branch: {br}\n\n")
                    md.append("**Error:** unable to resolve diff base/head\n\n")
                    continue

                stat = diff_stat(repo_dir, base, head) or ""
                ns = diff_name_status(repo_dir, base, head) or []

                num = diff_numstat(repo_dir, base, head)
                candidate_paths = [p for p, _ in num if not is_noisy_path(p)]
                chosen_paths = candidate_paths[:max_files_in_patch]

                changed_paths = []
                for ln in ns:
                    parts = ln.split("\t")
                    if len(parts) >= 2:
                        changed_paths.append(parts[-1])
                version_paths = [p for p in changed_paths if path_is_version_signal(p)]
                version_signals = extract_version_signals_from_files(repo_dir, base, head, version_paths)

                patch = diff_patch(repo_dir, base, head, chosen_paths, max_chars=max_patch_chars)

                safe_branch = br.replace("/", "__")
                artifact_dir = artifacts_root / repo
                artifact_dir.mkdir(parents=True, exist_ok=True)

                prompt = build_prompt(repo, br, parent, stat, patch, version_signals)
                if len(prompt) > max_prompt_chars:
                    prompt = prompt[:max_prompt_chars] + "\n\n[...prompt truncated...]\n"

                (artifact_dir / f"{safe_branch}.prompt.txt").write_text(prompt, encoding="utf-8", errors="replace")
                (artifact_dir / f"{safe_branch}.patch.txt").write_text(patch, encoding="utf-8", errors="replace")

                md.append(f"### Branch: {br}\n\n")
                md.append(f"_Diff range_: `{base[:10]}..{head[:10]}`\n\n")

                if version_signals:
                    md.append("**Version/build signals**\n\n")
                    for s in version_signals[:25]:
                        md.append(f"- {s}\n")
                    md.append("\n")

                if include_ollama:
                    summary, s_err = call_ollama(ollama_url, ollama_model, prompt, timeout_s=240)
                    if s_err:
                        logging.error(f"Ollama summary error for repo {repo}, branch {br}: {s_err}")
                        md.append("**Ollama summary error**\n\n")
                        md.append(f"{s_err}\n\n")
                    else:
                        logging.info(f"Ollama summary for repo {repo}, branch {br}: {summary}")
                        md.append("**What changed (Ollama, diff-grounded)**\n\n")
                        md.append(summary + "\n\n")
                else:
                    md.append("_Ollama disabled; see artifacts for prompt/patch._\n\n")

    if use_temp:
        with tempfile.TemporaryDirectory(prefix="weekly_repo_cache_") as td:
            process_all(Path(td))
    else:
        cache_dir.mkdir(parents=True, exist_ok=True)
        process_all(cache_dir)

    out_path.write_text("".join(md), encoding="utf-8", errors="replace")

    logging.info(f"Artifacts stored in: {artifacts_root}")
    logging.info(f"Wrote summary to: {out_path}")
    return 0


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Diff-based weekly summary per branch, summarized via local Ollama.")
    p.add_argument("--remote", default=REMOTE_DEFAULT)
    p.add_argument("--days", type=int, default=DAYS_DEFAULT)
    p.add_argument("--out", default="weekly_summary.md")
    p.add_argument("--cache-dir", default="repo_cache")
    p.add_argument("--temp", action="store_true")

    p.add_argument("--no-ollama", action="store_true")
    p.add_argument("--ollama-url", default=OLLAMA_URL_DEFAULT)
    p.add_argument("--ollama-model", default=OLLAMA_MODEL_DEFAULT)

    p.add_argument("--max-patch-chars", type=int, default=MAX_PATCH_CHARS_DEFAULT)
    p.add_argument("--max-prompt-chars", type=int, default=MAX_PROMPT_CHARS_DEFAULT)
    p.add_argument("--max-files", type=int, default=MAX_FILES_IN_PATCH_DEFAULT)

    p.add_argument("--artifacts-dir", default="")
    p.add_argument("--quiet", action="store_true")
    return p.parse_args()


import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def main() -> int:
    args = parse_args()
    logging.info(f"Starting with arguments: {args}")
    artifacts_dir = Path(args.artifacts_dir) if args.artifacts_dir.strip() else None
    return generate(
        repo_urls=REPOS_DEFAULT,
        remote=args.remote,
        days=args.days,
        out_path=Path(args.out),
        cache_dir=Path(args.cache_dir),
        use_temp=args.temp,
        artifacts_dir=artifacts_dir,
        include_ollama=(not args.no_ollama),
        ollama_url=args.ollama_url,
        ollama_model=args.ollama_model,
        max_patch_chars=max(5_000, args.max_patch_chars),
        max_prompt_chars=max(10_000, args.max_prompt_chars),
        max_files_in_patch=max(1, args.max_files),
        verbose=(not args.quiet),
    )


if __name__ == "__main__":
    raise SystemExit(main())
