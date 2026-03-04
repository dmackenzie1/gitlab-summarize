import datetime as dt
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from utils.ollama import OllamaClient

@dataclass
class OllamaError:
    message: str
    status_code: int | None = None
    details: str | None = None

@dataclass
class OllamaResult:
    text: str | None
    error: OllamaError | None

@dataclass
class ActivitySummaryResult:
    rollups_by_project_name: dict[str, str]
    highlights_for_master: list[tuple[str, str]]


@dataclass
class RunResult:
    exit_code: int
    projects_processed: int
    branches_analyzed: int
    artifacts_root: Path
    errors: list[str]


@dataclass
class Context:
    projects: list[dict]
    remote: str
    days: int
    out_dir: Path
    cache_dir: Path
    use_temp: bool
    offline: bool
    include_ollama: bool
    ollama_client: "OllamaClient | None"
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
    repo_items: list["RepoWorkItem"] = field(default_factory=list)
    branches_analyzed: int = 0
    errors: list[str] = field(default_factory=list)
    started_at: dt.datetime = field(default_factory=lambda: dt.datetime.now(dt.timezone.utc))
