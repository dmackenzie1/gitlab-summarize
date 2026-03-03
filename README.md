# gitlab-summarize

Diff-grounded weekly summarization tooling.

## Quick start

### 1) Configure projects

Put project metadata in `data/projects.json`.

```json
[
  {
    "project_name": "emss/coda",
    "project_id": 670,
    "ssh_url": "git@jsfitpeegitlab.ndc.nasa.gov:emss/coda.git",
    "is_default": true
  }
]
```

### 2) Add activity exports

Place one or more exported activity files in `project_activity/`.

- Auto-detected files: `project_activity/*.json` and `project_activity/*.csv`
- Recommended filename: `events-670.json`
- Required fields per row/event:
  - `timestamp`
  - `actor`
  - `action_type`
  - `project_id`
  - optional `project_name`
  - `description` (or summary text)

For GitLab native events JSON, the pipeline also accepts `created_at`, author/action variants, and embedded event payload fields.

### 3) Environment configuration

Copy `.env.example` to `.env` and update values as needed.

```bash
cp .env.example .env
```

Supported settings include Ollama model/timeout, summary sizing caps, and SMTP delivery config (`SMTP_TO` accepts a comma-separated list).

### 4) Run the pipeline

```bash
python main.py --out-dir out
```

Default behavior:
- `days=10`
- `projects=data/projects.json`
- verbose INFO logging enabled
- Ollama keep-alive set to `5m`

## Outputs

- `out/weeklySummary.markup`
- `out/weeklySummary.email.markup`
- `out/artifacts/` (per-repo patches, prompts, branch summaries, repo rollups, project summaries, cache, and errors)
- `out/artifacts/project_summaries/*.summary.markup` (one project summary file per configured project)

## Important flags

- `--projects <path>`
- `--days <n>`
- `--out-dir <dir>`
- `--ollama-model <model>`
- `--ollama-timeout <seconds>`
- `--ollama-retries <n>`
- `--ollama-keep-alive 5m`
- `--max-files <n>` / `--max-patch-chars <n>` / `--max-prompt-chars <n>`
- `--resummarize` (reuse saved patch artifacts, regenerate prompts, and resave summaries)

## Completion email

On both success and failure, the pipeline sends a completion email with start/end/duration, project and branch counts, artifact location, and error details when present.

SMTP settings are read from `.env` (`SMTP_HOST`, `SMTP_PORT`, `EMAIL_FROM`, `SMTP_TO`).

If SMTP send fails, the pipeline logs a warning and does not crash.

See `architecture.md` for module responsibilities and data flow.
