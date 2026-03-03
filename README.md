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

## Troubleshooting

- **`TypeError: object of NoneType has no len()` around `truncate(...)`**  
  The parser now coerces `None` and bytes to safe strings before truncation/sanitization. If you still see this, check custom callers that bypass `utils.parsing`.
- **Unicode decode errors from git output (for example `charmap` / byte decode failures)**  
  Git subprocess output is decoded as UTF-8 with replacement (`errors="replace"`) to prevent crashes on mixed encodings.
- **LLM prompt/response sanitation**  
  Prompts and model responses are normalized to text and cleaned of control characters before cache/write steps.

