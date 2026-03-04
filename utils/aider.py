import subprocess
from dataclasses import dataclass
from pathlib import Path


@dataclass
class AiderResult:
    text: str | None
    error: str | None


class AiderClient:
    def __init__(self, cmd: str = "aider", model: str = "qwen3.5:27b", timeout_s: int = 300) -> None:
        self.cmd = cmd
        self.model = model
        self.timeout_s = timeout_s

    def summarize_branch_diff(
        self,
        *,
        repo_dir: Path,
        repo_display: str,
        branch: str,
        parent_branch: str,
        stat: str,
        patch: str,
    ) -> AiderResult:
        prompt = (
            f"Summarize branch changes for {repo_display}.\\n"
            f"Compare {parent_branch} -> {branch}.\\n"
            "Return a short engineering summary as bullet points.\\n\\n"
            f"Diffstat:\\n{stat or 'N/A'}\\n\\n"
            f"Patch excerpt:\\n{patch[:120000]}"
        )
        cmd = [
            self.cmd,
            "--model",
            self.model,
            "--yes",
            "--message",
            prompt,
        ]
        try:
            proc = subprocess.run(
                cmd,
                cwd=str(repo_dir),
                capture_output=True,
                text=True,
                encoding="utf-8",
                errors="replace",
                timeout=self.timeout_s,
                check=False,
            )
        except Exception as exc:  # noqa: BLE001
            return AiderResult(text=None, error=str(exc))

        output = (proc.stdout or "").strip()
        if proc.returncode != 0:
            err = (proc.stderr or output or f"aider failed with exit {proc.returncode}").strip()
            return AiderResult(text=None, error=err)
        if not output:
            return AiderResult(text=None, error="aider returned empty response")
        return AiderResult(text=output, error=None)
