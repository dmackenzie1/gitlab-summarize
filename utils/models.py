from dataclasses import dataclass

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
