import subprocess
from dataclasses import dataclass
from pathlib import Path

@dataclass
class CmdResult:
    ok: bool
    cmd: list[str]
    stdout: str
    stderr: str
    returncode: int

def run_cmd(cmd: list[str], cwd: Path | None = None) -> CmdResult:
    try:
        p = subprocess.run(
            cmd,
            cwd=str(cwd) if cwd else None,
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            check=False,
        )
        return CmdResult(p.returncode == 0, cmd, p.stdout, p.stderr, p.returncode)
    except Exception as exc:
        return CmdResult(False, cmd, "", str(exc), 127)

def git(repo_dir: Path, *args: str) -> CmdResult:
    return run_cmd(["git", "-C", str(repo_dir), *args])

def repo_dir_name_from_project(project_name: str, ssh_url: str) -> str:
    pn = (project_name or "").strip()
    if "/" in pn:
        return pn.split("/")[-1].strip() or pn.replace("/", "__")
    base = (ssh_url or "").strip().split("/")[-1]
    if base.endswith(".git"):
        base = base[:-4]
    return base or pn or "repo"

def ensure_clone(repo_url: str, target_dir: Path) -> tuple[bool, str | None]:
    if (target_dir / ".git").exists():
        return True, None
    target_dir.parent.mkdir(parents=True, exist_ok=True)
    res = run_cmd(["git", "clone", "--no-tags", repo_url, str(target_dir)])
    if res.ok:
        return True, None
    return False, res.stderr.strip() or f"git clone failed (exit {res.returncode})"

def fetch_all(repo_dir: Path) -> CmdResult:
    return git(repo_dir, "fetch", "--all", "--prune")

def get_default_remote_branch(repo_dir: Path, remote: str) -> str | None:
    res = git(repo_dir, "symbolic-ref", "-q", f"refs/remotes/{remote}/HEAD")
    if not res.ok:
        return None
    ref = res.stdout.strip()
    if ref.startswith("refs/remotes/"):
        return ref.replace("refs/remotes/", "")
    return None

def list_remote_branches(repo_dir: Path, remote: str) -> tuple[list[str] | None, str | None]:
    res = git(repo_dir, "for-each-ref", "--format=%(refname:short)", f"refs/remotes/{remote}")
    if not res.ok:
        return None, res.stderr.strip() or f"git for-each-ref failed (exit {res.returncode})"
    out: list[str] = []
    for line in res.stdout.splitlines():
        line = line.strip()
        if not line or line.endswith("/HEAD") or line == remote:
            continue
        out.append(line)
    return sorted(out), None

def branch_has_recent_commits(repo_dir: Path, branch: str, since: str) -> bool:
    res = git(repo_dir, "rev-list", "--count", f"--since={since}", branch)
    if not res.ok:
        return False
    try:
        return int(res.stdout.strip() or "0") > 0
    except ValueError:
        return False

def merge_base(repo_dir: Path, a: str, b: str) -> str | None:
    res = git(repo_dir, "merge-base", a, b)
    return res.stdout.strip() if res.ok and res.stdout.strip() else None

def rev_parse(repo_dir: Path, ref: str) -> str | None:
    res = git(repo_dir, "rev-parse", ref)
    return res.stdout.strip() if res.ok and res.stdout.strip() else None

def diff_stat(repo_dir: Path, base: str, head: str) -> str:
    res = git(repo_dir, "diff", "--stat", f"{base}..{head}")
    return res.stdout.strip() if res.ok else ""

def diff_name_status(repo_dir: Path, base: str, head: str) -> list[str]:
    res = git(repo_dir, "diff", "--name-status", f"{base}..{head}")
    if not res.ok:
        return []
    return [line.strip() for line in res.stdout.splitlines() if line.strip()]

def diff_numstat(repo_dir: Path, base: str, head: str) -> list[tuple[str, int, int]]:
    res = git(repo_dir, "diff", "--numstat", f"{base}..{head}")
    if not res.ok:
        return []
    out: list[tuple[str, int, int]] = []
    for line in res.stdout.splitlines():
        parts = line.split("\t")
        if len(parts) != 3 or parts[0] == "-" or parts[1] == "-":
            continue
        try:
            out.append((parts[2], int(parts[0]), int(parts[1])))
        except ValueError:
            pass
    return sorted(out, key=lambda item: item[1] + item[2], reverse=True)

def diff_patch(repo_dir: Path, base: str, head: str, paths: list[str]) -> str:
    if not paths:
        return ""
    res = git(repo_dir, "diff", f"{base}..{head}", "--patch", "--no-color", "--minimal", "--", *paths)
    return res.stdout if res.ok else ""

def recent_merge_commits(repo_dir: Path, days: int = 10, limit: int = 40) -> list[dict]:
    fmt = "%H%x1f%an%x1f%ad%x1f%s"
    res = git(
        repo_dir,
        "log",
        "--merges",
        f"--since={days} days ago",
        f"--max-count={limit}",
        f"--pretty=format:{fmt}",
        "--date=short",
    )
    if not res.ok:
        return []
    entries: list[dict] = []
    for row in res.stdout.splitlines():
        parts = row.split("\x1f")
        if len(parts) != 4:
            continue
        entries.append({"sha": parts[0], "author": parts[1], "date": parts[2], "subject": parts[3]})
    return entries
