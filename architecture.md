# Architecture

## Module layout

- `main.py`: thin CLI orchestrator. Parses flags/env-like options and invokes the summary pipeline.
- `utils/summary.py`: end-to-end weekly pipeline orchestration.
  - per-project branch summarization
  - per-project rollup summarization
  - master (all projects) summarization
  - deterministic artifact/output writes
- `utils/ollama.py`: Ollama HTTP client with:
  - request retries + exponential backoff
  - timeout handling
  - structured error return objects
  - persistent `requests.Session` reuse and optional `keep_alive`
- `utils/git.py`: git helpers:
  - clone/fetch
  - branch listing and activity filtering
  - merge-base/diff helpers
  - recent merge commit extraction (`recent_merge_commits`, default 10 days)
- `utils/parsing.py`: text/prompt helpers, chunking, sanitization, stable hashing for cache keys.
- `utils/email_markup.py`: reusable email-oriented markup rendering.

## Data flow

1. Read monitored repositories from `data/monitored.json`.
2. Clone/fetch each repository into repo cache (or temporary workspace).
3. Detect active branches based on `--days` window.
4. For each branch:
   - compute diff range using `merge-base(parent, branch)..branch`
   - capture curated patch + version/build signals
   - summarize via Ollama (with caching)
   - persist branch artifacts (`*.patch.txt`, `*.prompt.txt`, `*.summary.txt`)
5. Build repo rollup summaries from branch summaries.
6. Build a master summary from repo rollups.
7. Render outputs:
   - `weeklySummary.markup`
   - `weeklySummary.email.markup`
   - artifact prompts/summaries under `artifacts/`

## Merge detection (recent merges)

Implementation is best-effort and repo-history dependent:
- uses `git log --merges --since="<N> days ago"`
- captures SHA, author, date, subject

Limitations:
- Squash/rebase workflows may produce no merge commits.
- Some teams merge without merge commits, so history can under-report merged work.

Fallback commands:

```bash
git log --since="10 days ago" --first-parent --oneline
git log --since="10 days ago" --pretty=format:'%h %ad %an %s' --date=short
```

## Ollama strategy

- Request timeout configurable (`--ollama-timeout`)
- Retries + exponential backoff (`--ollama-retries`, `--ollama-backoff`)
- Session reuse via `requests.Session`
- Optional `keep_alive` payload (`--ollama-keep-alive`)
- Caching to avoid re-summarizing unchanged payloads:
  - cache key includes prompt text, model, prompt version, timeout, retries, and scope metadata
  - stored under `artifacts/cache/*.txt`
- Context safety:
  - patch truncation and prompt truncation caps
  - chunked branch summarization for oversized patches
  - chunk rollup pass before branch final summary

## Output and error locations

Given `--out-dir <DIR>`:

- `<DIR>/weeklySummary.markup`
- `<DIR>/weeklySummary.email.markup`
- `<DIR>/artifacts/<repo>/...`
- `<DIR>/artifacts/master_summary.prompt.txt`
- `<DIR>/artifacts/master_summary.summary.txt`
- `<DIR>/artifacts/errors/*.log`
- `<DIR>/artifacts/cache/*.txt`

## Run locally / CI

### Local run

```bash
python main.py --monitored data/monitored.json --days 10 --out-dir out
```

### Without Ollama (dry run for git artifacts)

```bash
python main.py --no-ollama --monitored data/monitored.json --out-dir out
```

### CI-friendly example

```bash
python main.py \
  --monitored data/monitored.json \
  --days 10 \
  --out-dir out \
  --ollama-url http://ollama:11434/api/generate \
  --ollama-model qwen2.5-coder:32b \
  --ollama-timeout 300 \
  --ollama-retries 4
```

## Email markup generation

Email output is generated automatically during the run:
- `weeklySummary.markup` is rendered first.
- `utils/email_markup.py` converts it to HTML-email-friendly markup and writes `weeklySummary.email.markup`.
