#!/usr/bin/env python3

import argparse
import csv
import json
import os
import re
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, List, Tuple, Optional, Set

import toml

try:
    import yaml  # for docker-compose
except Exception:
    yaml = None


MANIFEST_FILENAMES = {
    # Python
    "pyproject.toml",
    "requirements.txt",
    "requirements-dev.txt",
    "requirements-prod.txt",
    "setup.cfg",
    "Pipfile",
    # Node
    "package.json",
    "package-lock.json",
    "npm-shrinkwrap.json",
    "yarn.lock",
    "pnpm-lock.yaml",
    # Node runtime pins
    ".nvmrc",
    ".node-version",
    # uv
    "uv.lock",
    # Docker
    "Dockerfile",
    "docker-compose.yml",
    "docker-compose.yaml",
    "compose.yml",
    "compose.yaml",
}

EXCLUDE_DIRS = {
    ".git",
    ".hg",
    ".svn",
    "node_modules",
    ".venv",
    "venv",
    "__pycache__",
    ".mypy_cache",
    ".pytest_cache",
    ".ruff_cache",
    "dist",
    "build",
    ".tox",
}

PY_NAME_RE = re.compile(r"^\s*([A-Za-z0-9][A-Za-z0-9._-]*)\s*")
REQ_LINE_CONTINUATION_RE = re.compile(r"\\\s*$")


@dataclass(frozen=True)
class Usage:
    repo: str
    relpath: str
    manifest_type: str
    version_or_spec: str


def normalize_name(name: str) -> str:
    return re.sub(r"[-_.]+", "-", (name or "")).lower().strip()


def safe_read_text(path: Path) -> str:
    return path.read_text(encoding="utf-8", errors="replace")


def iter_text_lines(path: Path) -> Iterable[str]:
    with open(path, "r", encoding="utf-8", errors="replace") as f:
        yield from f


def stable_spec(spec: str) -> str:
    return (spec or "").strip()


def is_dockerfile_name(filename: str) -> bool:
    lname = (filename or "").lower()
    return lname == "dockerfile" or lname.startswith("dockerfile.") or lname.endswith(".dockerfile")


def is_compose_name(filename: str) -> bool:
    lname = (filename or "").lower()
    if lname in ("docker-compose.yml", "docker-compose.yaml", "compose.yml", "compose.yaml"):
        return True
    return lname.startswith("docker-compose.") and (lname.endswith(".yml") or lname.endswith(".yaml"))


def is_manifest_filename(filename: str) -> bool:
    if filename in MANIFEST_FILENAMES:
        return True

    lname = (filename or "").lower()
    if lname.startswith("requirements") and lname.endswith(".txt"):
        return True
    if is_dockerfile_name(filename) or is_compose_name(filename):
        return True
    return False


_IMAGE_DIGEST_SPLIT_RE = re.compile(r"@sha256:[0-9a-f]{16,}$", re.IGNORECASE)

def parse_docker_image_ref(image: str) -> Tuple[str, str]:
    s = (image or "").strip()
    if not s:
        return ("", "")

    # remove digest suffix (keep tag if present)
    s = _IMAGE_DIGEST_SPLIT_RE.sub("", s)

    # last component after '/'
    last = s.split("/")[-1]

    # handle possible "name:tag"
    # if last has multiple ':', treat the last ':' as tag separator (common case)
    if ":" in last:
        name, tag = last.rsplit(":", 1)
        return (normalize_name(name), tag.strip())
    return (normalize_name(last), "")


def runtime_key_from_image(image: str) -> Tuple[str, str]:
    name, tag = parse_docker_image_ref(image)
    if not name:
        return ("", "")
    return (f"runtime:{name}", tag)


_APK_TOKEN_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9+._-]*")
def parse_apk_pkg_token(token: str) -> Tuple[str, str]:
    t = (token or "").strip()
    if not t:
        return ("", "")
    # remove trailing punctuation
    t = t.strip(" \t\r\n\\")
    # split by first occurrence of one of (=, ~=)
    if "~=" in t:
        name, rest = t.split("~=", 1)
        name = name.strip()
        return (normalize_name(name), "~=" + rest.strip())
    if "=" in t:
        name, rest = t.split("=", 1)
        name = name.strip()
        return (normalize_name(name), "=" + rest.strip())
    # plain token
    m = _APK_TOKEN_RE.match(t)
    return (normalize_name(m.group(0)) if m else normalize_name(t), "")


def parse_apt_pkg_token(token: str) -> Tuple[str, str]:
    t = (token or "").strip()
    if not t:
        return ("", "")
    t = t.strip(" \t\r\n\\")
    if "=" in t:
        name, rest = t.split("=", 1)
        return (normalize_name(name.strip()), "=" + rest.strip())
    return (normalize_name(t), "")


def add_lib(out: Dict[str, str], raw_name: str, raw_spec: str):
    if not raw_name:
        return
    out[normalize_name(raw_name)] = stable_spec(raw_spec)


def add_lib_keep_key(out: Dict[str, str], key: str, spec: str):
    if not key:
        return
    out[key] = stable_spec(spec)


def parse_python_req_name(spec: str) -> str:
    s = (spec or "").strip()
    if not s:
        return ""
    # skip pure URL/vcs without name
    if s.startswith(("git+", "http://", "https://", "ssh://")):
        return ""
    s = s.split(";", 1)[0].strip()
    if "@" in s:
        left = s.split("@", 1)[0].strip()
        left = left.split("[", 1)[0].strip()
        m = PY_NAME_RE.match(left)
        return m.group(1) if m else ""
    base = s.split("[", 1)[0].strip()
    m = PY_NAME_RE.match(base)
    return m.group(1) if m else ""


def extract_python_from_pyproject(pyproject_path: Path) -> Dict[str, str]:
    libs: Dict[str, str] = {}
    try:
        pyproject = toml.load(str(pyproject_path))
    except Exception:
        return libs

    project = pyproject.get("project", {}) or {}
    for dep in project.get("dependencies", []) or []:
        add_lib(libs, parse_python_req_name(dep), dep)

    opt = project.get("optional-dependencies", {}) or {}
    if isinstance(opt, dict):
        for _, deps in opt.items():
            if isinstance(deps, list):
                for dep in deps:
                    add_lib(libs, parse_python_req_name(dep), dep)

    # best-effort: poetry
    poetry = (pyproject.get("tool", {}) or {}).get("poetry", {}) or {}
    deps = poetry.get("dependencies", {}) or {}
    if isinstance(deps, dict):
        for name, spec in deps.items():
            if name.lower() == "python":
                continue
            add_lib(libs, name, f"{name} {spec}")

    dev = poetry.get("dev-dependencies", {}) or {}
    if isinstance(dev, dict):
        for name, spec in dev.items():
            add_lib(libs, name, f"{name} {spec}")

    return libs


def extract_requirements_txt(req_path: Path) -> Dict[str, str]:
    libs: Dict[str, str] = {}
    buf = ""
    for raw in iter_text_lines(req_path):
        line = raw.strip()
        if not line or line.startswith("#"):
            continue

        # handle line continuations with trailing '\'
        if REQ_LINE_CONTINUATION_RE.search(line):
            buf += REQ_LINE_CONTINUATION_RE.sub("", line).strip() + " "
            continue
        if buf:
            line = (buf + line).strip()
            buf = ""

        name = parse_python_req_name(line)
        add_lib(libs, name, line)
    return libs


def extract_setup_cfg(setup_cfg_path: Path) -> Dict[str, str]:
    libs: Dict[str, str] = {}
    text = safe_read_text(setup_cfg_path)

    # install_requires under [options]
    m = re.search(
        r"(?is)^\[options\].*?^install_requires\s*=\s*(.*?)(^\[|\Z)",
        text,
        re.MULTILINE,
    )
    if m:
        body = m.group(1)
        for line in body.splitlines():
            s = line.strip()
            if not s or s.startswith("#"):
                continue
            add_lib(libs, parse_python_req_name(s), s)

    # extras under [options.extras_require]
    m2 = re.search(r"(?is)^\[options\.extras_require\].*?(?=^\[|\Z)", text, re.MULTILINE)
    if m2:
        body = m2.group(0)
        for line in body.splitlines():
            s = line.strip()
            if (
                not s
                or s.startswith("#")
                or s.startswith("[")
                or s.endswith("=")
                or " =" in s
            ):
                continue
            add_lib(libs, parse_python_req_name(s), s)

    return libs


def extract_pipfile(pipfile_path: Path) -> Dict[str, str]:
    libs: Dict[str, str] = {}
    try:
        data = toml.load(str(pipfile_path))
    except Exception:
        return libs
    for section in ("packages", "dev-packages"):
        pkgs = data.get(section, {}) or {}
        if isinstance(pkgs, dict):
            for name, spec in pkgs.items():
                add_lib(libs, name, f"{name} {spec}")
    return libs


def extract_package_json(pkg_path: Path) -> Dict[str, str]:
    libs: Dict[str, str] = {}
    try:
        data = json.loads(safe_read_text(pkg_path))
    except Exception:
        return libs

    for section in ("dependencies", "devDependencies", "peerDependencies", "optionalDependencies"):
        deps = data.get(section, {}) or {}
        if isinstance(deps, dict):
            for name, ver in deps.items():
                libs[normalize_name(name)] = str(ver).strip()

    # Runtime: engines.node (captures node policy even when docker images differ)
    engines = data.get("engines") or {}
    if isinstance(engines, dict):
        node_spec = engines.get("node")
        if node_spec:
            add_lib_keep_key(libs, "runtime:node", str(node_spec).strip())

    return libs


def extract_package_lock(lock_path: Path) -> Dict[str, str]:
    libs: Dict[str, str] = {}
    try:
        data = json.loads(safe_read_text(lock_path))
    except Exception:
        return libs

    deps = data.get("dependencies")
    if isinstance(deps, dict):
        for name, meta in deps.items():
            if isinstance(meta, dict):
                ver = meta.get("version")
                if ver:
                    libs[normalize_name(name)] = str(ver).strip()
    return libs


def extract_node_version_file(path: Path) -> Dict[str, str]:
    libs: Dict[str, str] = {}
    raw = safe_read_text(path).strip()
    if not raw:
        return libs
    v = raw.splitlines()[0].strip()
    v = v.lstrip("v").strip()
    if v:
        add_lib_keep_key(libs, "runtime:node", v)
    return libs


def extract_uv_lock(uv_lock_path: Path) -> Dict[str, str]:
    libs: Dict[str, str] = {}
    text = safe_read_text(uv_lock_path)

    name: Optional[str] = None
    for line in text.splitlines():
        line = line.strip()
        m1 = re.match(r'^name\s*=\s*"([^"]+)"', line)
        if m1:
            name = m1.group(1)
            continue
        m2 = re.match(r'^version\s*=\s*"([^"]+)"', line)
        if m2 and name:
            add_lib(libs, name, f"{name}=={m2.group(1)}")
            name = None
    return libs


_APT_INSTALL_RE = re.compile(r"(?i)\bapt-get\s+install\b([^&;]+)")
_APK_ADD_RE = re.compile(r"(?i)\bapk\s+add\b([^&;]+)")

def extract_docker_signals(dockerfile_path: Path) -> Dict[str, str]:
    libs: Dict[str, str] = {}
    text = safe_read_text(dockerfile_path)

    for line in text.splitlines():
        s = line.strip()
        if not s or s.startswith("#"):
            continue

        m = re.match(r"(?i)^FROM\s+([^\s]+)", s)
        if m:
            image = m.group(1).strip()
            key, tag = runtime_key_from_image(image)
            if key:
                add_lib_keep_key(libs, key, tag or image)
            else:
                add_lib(libs, f"docker-image:{image}", f"FROM {image}")
            continue

        # apt-get install -y foo bar foo=1.2
        m = _APT_INSTALL_RE.search(s)
        if m:
            chunk = m.group(1)
            chunk = re.sub(r"(?i)\s+-y\s+", " ", chunk)
            pkgs = chunk.strip().split()
            for p in pkgs:
                if not p or p.startswith("-"):
                    continue
                name, vspec = parse_apt_pkg_token(p)
                if name:
                    add_lib_keep_key(libs, f"apt:{name}", vspec or "")

        # apk add --no-cache foo bar foo=1.2 foo~=1.2
        m = _APK_ADD_RE.search(s)
        if m:
            chunk = m.group(1)
            chunk = re.sub(r"(?i)--no-cache", "", chunk)
            pkgs = chunk.strip().split()
            for p in pkgs:
                if not p or p.startswith("-"):
                    continue
                name, vspec = parse_apk_pkg_token(p)
                if name:
                    add_lib_keep_key(libs, f"apk:{name}", vspec or "")

        # pip install
        if re.search(r"(?i)\bpip\s+install\b", s):
            add_lib_keep_key(libs, "pip-install", s)

        # npm ci / npm install / pnpm / yarn
        if re.search(r"(?i)\bnpm\s+(ci|install)\b", s):
            add_lib_keep_key(libs, "npm-install", s)
        if re.search(r"(?i)\bpnpm\s+install\b", s):
            add_lib_keep_key(libs, "pnpm-install", s)
        if re.search(r"(?i)\byarn\s+install\b", s):
            add_lib_keep_key(libs, "yarn-install", s)

        # uv sync / uv pip install
        if re.search(r"(?i)\buv\s+sync\b", s) or re.search(r"(?i)\buv\s+pip\s+install\b", s):
            add_lib_keep_key(libs, "uv", s)

    return libs


def extract_compose_signals(compose_path: Path) -> Dict[str, str]:
    libs: Dict[str, str] = {}
    if yaml is None:
        return libs
    try:
        data = yaml.safe_load(safe_read_text(compose_path))
    except Exception:
        return libs

    services = (data or {}).get("services", {}) or {}
    if isinstance(services, dict):
        for svc_name, svc in services.items():
            if not isinstance(svc, dict):
                continue
            image = svc.get("image")
            if image:
                key, tag = runtime_key_from_image(str(image))
                if key:
                    add_lib_keep_key(libs, key, tag or str(image))
                else:
                    add_lib(libs, f"docker-image:{image}", f"{svc_name}: image {image}")

            build = svc.get("build")
            if isinstance(build, dict):
                dockerfile = build.get("dockerfile")
                if dockerfile:
                    add_lib_keep_key(libs, f"dockerfile:{dockerfile}", f"{svc_name}: dockerfile {dockerfile}")

    return libs


def find_manifests(repo_root: Path) -> List[Path]:
    manifests: List[Path] = []
    for root, dirs, files in os.walk(repo_root):
        root_path = Path(root)
        dirs[:] = [d for d in dirs if d not in EXCLUDE_DIRS]
        for fn in files:
            if is_manifest_filename(fn):
                manifests.append(root_path / fn)
    return manifests


def parse_manifest(path: Path) -> Tuple[str, Dict[str, str]]:
    name = path.name
    lname = name.lower()

    if name == "pyproject.toml":
        return ("pyproject.toml", extract_python_from_pyproject(path))
    if lname.startswith("requirements") and lname.endswith(".txt"):
        return ("requirements.txt", extract_requirements_txt(path))
    if name == "setup.cfg":
        return ("setup.cfg", extract_setup_cfg(path))
    if name == "Pipfile":
        return ("Pipfile", extract_pipfile(path))

    if name == "package.json":
        return ("package.json", extract_package_json(path))
    if name in ("package-lock.json", "npm-shrinkwrap.json"):
        return (name, extract_package_lock(path))

    if name in (".nvmrc", ".node-version"):
        return (name, extract_node_version_file(path))

    if lname == "uv.lock":
        return ("uv.lock", extract_uv_lock(path))

    if is_dockerfile_name(name):
        return ("Dockerfile", extract_docker_signals(path))
    if is_compose_name(name):
        return (name, extract_compose_signals(path))

    # locks we aren't parsing fully yet; still signal presence
    if name in ("yarn.lock", "pnpm-lock.yaml"):
        return (name, {"lockfile-present": name})

    return ("unknown", {})


def scan_repo_cache(repo_cache_path: Path) -> Tuple[Dict[str, List[Usage]], List[str]]:
    library_usage: Dict[str, List[Usage]] = defaultdict(list)
    repos: List[str] = []

    if not repo_cache_path.exists():
        raise FileNotFoundError(f"repo_cache_path does not exist: {repo_cache_path}")

    for repo_name in sorted(os.listdir(repo_cache_path)):
        repo_path = repo_cache_path / repo_name
        if not repo_path.is_dir():
            continue
        repos.append(repo_name)

        manifests = find_manifests(repo_path)
        for mf in manifests:
            kind, libs = parse_manifest(mf)
            rel = mf.relative_to(repo_path).as_posix()

            for lib, spec in libs.items():
                library_usage[lib].append(
                    Usage(
                        repo=repo_name,
                        relpath=rel,
                        manifest_type=kind,
                        version_or_spec=str(spec).strip(),
                    )
                )

    return library_usage, repos


def write_usage_csv(library_usage: Dict[str, List[Usage]], output_csv_path: Path) -> None:
    output_csv_path.parent.mkdir(parents=True, exist_ok=True)

    with open(output_csv_path, "w", newline="", encoding="utf-8") as csvfile:
        fieldnames = ["Library", "Repo", "Path", "ManifestType", "VersionOrSpec"]
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()

        for lib in sorted(library_usage.keys()):
            for u in library_usage[lib]:
                writer.writerow(
                    {
                        "Library": lib,
                        "Repo": u.repo,
                        "Path": u.relpath,
                        "ManifestType": u.manifest_type,
                        "VersionOrSpec": u.version_or_spec,
                    }
                )


def build_matrix(
    library_usage: Dict[str, List[Usage]],
    repos: List[str],
    *,
    min_repos: int = 1,
    only_mismatches: bool = False,
) -> Tuple[List[str], List[Dict[str, str]]]:
    # lib -> repo -> spec (if multiple, join with " | " deterministically)
    lib_repo_specs: Dict[str, Dict[str, Set[str]]] = defaultdict(lambda: defaultdict(set))

    for lib, usages in library_usage.items():
        for u in usages:
            lib_repo_specs[lib][u.repo].add(stable_spec(u.version_or_spec))

    header = ["Library"] + repos
    out_rows: List[Dict[str, str]] = []

    for lib in sorted(lib_repo_specs.keys()):
        repo_map = lib_repo_specs[lib]

        repo_count = len(repo_map)
        if repo_count < min_repos:
            continue

        # compute distinct specs across repos
        distinct: Set[str] = set()
        for r in repo_map:
            distinct.update(repo_map[r])

        is_mismatch = len(distinct) > 1
        if only_mismatches and not is_mismatch:
            continue

        row: Dict[str, str] = {"Library": lib}
        for r in repos:
            if r not in repo_map:
                row[r] = ""
            else:
                specs = sorted(s for s in repo_map[r] if s)
                row[r] = " | ".join(specs)

        out_rows.append(row)

    return header, out_rows


def write_matrix_csv(header: List[str], rows: List[Dict[str, str]], out_path: Path) -> None:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with open(out_path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=header)
        w.writeheader()
        for row in rows:
            w.writerow(row)


def build_mismatch_report(
    library_usage: Dict[str, List[Usage]],
    *,
    min_repos: int = 2,
) -> List[Dict[str, str]]:
    lib_repo_specs: Dict[str, Dict[str, Set[str]]] = defaultdict(lambda: defaultdict(set))

    for lib, usages in library_usage.items():
        for u in usages:
            lib_repo_specs[lib][u.repo].add(stable_spec(u.version_or_spec))

    report: List[Dict[str, str]] = []
    for lib, repo_map in lib_repo_specs.items():
        repo_count = len(repo_map)
        if repo_count < min_repos:
            continue

        distinct: Set[str] = set()
        for specs in repo_map.values():
            distinct.update(specs)

        if len(distinct) <= 1:
            continue

        report.append(
            {
                "Library": lib,
                "RepoCount": str(repo_count),
                "DistinctSpecCount": str(len(distinct)),
                "DistinctSpecs": " || ".join(sorted(distinct)),
                "Repos": ", ".join(sorted(repo_map.keys())),
            }
        )

    # rank: most repos affected first, then most distinct specs
    report.sort(key=lambda x: (int(x["RepoCount"]), int(x["DistinctSpecCount"])), reverse=True)
    return report


def write_mismatch_csv(rows: List[Dict[str, str]], out_path: Path) -> None:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = ["Library", "RepoCount", "DistinctSpecCount", "DistinctSpecs", "Repos"]
    with open(out_path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for r in rows:
            w.writerow(r)


def main() -> int:
    ap = argparse.ArgumentParser(description="Analyze dependency versions across repos and emit a matrix + mismatch report.")
    ap.add_argument("--repo-cache", default="./repo_cache", help="Path to repo cache directory (folders = apps).")
    ap.add_argument("--usage-csv", default="./library_usage.csv", help="Output CSV for row-per-occurrence usage.")
    ap.add_argument("--matrix-csv", default="./library_matrix.csv", help="Output CSV for library x repo matrix.")
    ap.add_argument("--mismatch-csv", default="./library_mismatches.csv", help="Output CSV for mismatch report.")
    ap.add_argument("--min-repos", type=int, default=2, help="Only include libraries used by at least N repos.")
    ap.add_argument("--only-mismatches", action="store_true", help="Only include libraries with mismatched specs across repos in the matrix.")
    ap.add_argument("--top", type=int, default=25, help="How many top 'start here' mismatch libs to print.")
    args = ap.parse_args()

    repo_cache_path = Path(args.repo_cache)
    usage_csv_path = Path(args.usage_csv)
    matrix_csv_path = Path(args.matrix_csv)
    mismatch_csv_path = Path(args.mismatch_csv)

    library_usage, repos = scan_repo_cache(repo_cache_path)

    # 1) Ground-truth usage list
    write_usage_csv(library_usage, usage_csv_path)

    # 2) Matrix
    header, rows = build_matrix(
        library_usage,
        repos,
        min_repos=args.min_repos,
        only_mismatches=args.only_mismatches,
    )
    write_matrix_csv(header, rows, matrix_csv_path)

    # 3) Mismatch report (always mismatch-only)
    mismatch_rows = build_mismatch_report(library_usage, min_repos=args.min_repos)
    write_mismatch_csv(mismatch_rows, mismatch_csv_path)

    # Console summary: what to start with
    print("\n=== What to start with (most common libs with version/spec mismatches) ===")
    for r in mismatch_rows[: max(0, args.top)]:
        print(f"- {r['Library']}  (repos={r['RepoCount']}, distinct_specs={r['DistinctSpecCount']})")

    # Also show top libs by occurrences
    top_occ = sorted(library_usage.items(), key=lambda x: len(x[1]), reverse=True)[:15]
    print("\n=== Top Libraries (by occurrences across manifests) ===")
    for lib, usages in top_occ:
        repo_set = {u.repo for u in usages}
        print(f"- {lib}: occurrences={len(usages)}, repos={len(repo_set)}")

    print("\nWrote:")
    print(f"- usage CSV:     {usage_csv_path.resolve()}")
    print(f"- matrix CSV:    {matrix_csv_path.resolve()}")
    print(f"- mismatch CSV:  {mismatch_csv_path.resolve()}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
