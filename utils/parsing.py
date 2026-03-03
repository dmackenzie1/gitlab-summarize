from __future__ import annotations

import hashlib
import json
import re
from pathlib import Path
from typing import Any, Iterable, List, Sequence

NOISY_FILES = {
    "package-lock.json",
    "pnpm-lock.yaml",
    "yarn.lock",
    "uv.lock",
    "poetry.lock",
}
NOISY_SUFFIXES = (".lock", ".min.js", ".map")
NOISY_PATH_CONTAINS = ("/node_modules/", "/dist/", "/build/", "/.venv/")

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


def coerce_text(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="replace")
    return str(value)


def sanitize_prompt(text: str | None) -> str:
    text = coerce_text(text).replace("\t", "    ")
    return re.sub(r"[\x00-\x08\x0b-\x1f\x7f]", "", text)


def unique_preserve_order(items: Iterable[str]) -> List[str]:
    seen = set()
    out: List[str] = []
    for item in items:
        token = item.strip()
        if not token or token in seen:
            continue
        seen.add(token)
        out.append(token)
    return out


def is_noisy_path(path: str) -> bool:
    normalized = path.replace("\\", "/")
    if normalized.split("/")[-1] in NOISY_FILES:
        return True
    if normalized.endswith(NOISY_SUFFIXES):
        return True
    return any(fragment in normalized for fragment in NOISY_PATH_CONTAINS)


def path_is_version_signal(path: str) -> bool:
    normalized = path.replace("\\", "/").lstrip("./")
    for sig in VERSION_SIGNAL_PATHS:
        if sig.endswith("/") and normalized.startswith(sig):
            return True
        if normalized == sig or normalized.endswith("/" + sig):
            return True
    return False


def truncate(text: str | None, max_chars: int, suffix: str = "\n\n[...truncated...]\n") -> str:
    text = coerce_text(text)
    if len(text) <= max_chars:
        return text
    return text[:max_chars] + suffix


def chunk_text(text: str, max_chars: int, overlap: int = 1000) -> Sequence[str]:
    if max_chars <= 0:
        return [text]
    if len(text) <= max_chars:
        return [text]
    chunks: list[str] = []
    start = 0
    while start < len(text):
        end = min(len(text), start + max_chars)
        chunks.append(text[start:end])
        if end >= len(text):
            break
        start = max(0, end - overlap)
    return chunks


def stable_json_hash(payload: dict) -> str:
    raw = json.dumps(payload, sort_keys=True, ensure_ascii=False)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        while True:
            data = f.read(65536)
            if not data:
                break
            h.update(data)
    return h.hexdigest()
