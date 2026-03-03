# gitlab-summarize

Diff-grounded weekly summarization tooling.

## Quick start

```bash
python main.py --monitored data/monitored.json --days 10 --out-dir out
```

## Outputs

- `out/weeklySummary.markup`
- `out/weeklySummary.email.markup`
- `out/artifacts/` (per-repo prompts, patches, summaries, cache, and error logs)

## Useful flags

- `--no-ollama` (generate git artifacts only)
- `--ollama-model <model>`
- `--ollama-timeout <seconds>`
- `--ollama-retries <n>`
- `--ollama-keep-alive 20m`
- `--fail-fast` (stop on unrecoverable summary errors)

See `architecture.md` for module responsibilities and data flow.

`main.py` is the single supported entrypoint for summary generation.
