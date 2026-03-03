from utils.parsing import sanitize_prompt

BRANCH_SUMMARY_INTRO = (
    "You are summarizing code changes for a weekly engineering report.\n"
    "Use only the diff evidence provided; do NOT rely on commit messages/authors."
)

BRANCH_SUMMARY_RETURN_SPEC = (
    "Return:\n"
    "- 3-8 bullet points: behavior/capability changes\n"
    "- 0-3 bullet points: risks/breaking changes\n"
    "- 0-3 bullet points: significant version/build changes\n"
)

REPO_ROLLUP_INTRO = [
    "You are producing a repo-level weekly rollup from branch summaries.",
    "Do not invent changes.",
    "Prioritize leadership signal: architecture/runtime/CI/cross-repo consistency over small refactors.",
    "Remove repetition across sections and collapse duplicate upgrades into one shared bullet.",
]

REPO_ROLLUP_RETURN_SPEC = (
    "\nReturn using this exact structure:\n"
    "- Management Summary: 1-3 sentences (sentence 1 = main delivery, sentence 2 = leadership impact, sentence 3 optional = near-term awareness).\n"
    "- Key Changes: 3-5 bullets max, only high-signal items.\n"
    "- Action Needed: include only future-facing actions requiring coordination/blocker awareness; format each as 'What / When / Consequence'. Omit section if none.\n"
    "- Shared Updates Note: if this repo matches an org-wide update (for example template/version bumps), use one short reference instead of repeating details.\n"
)

ACTIVITY_CHUNK_INTRO = "Summarize GitLab project activity. Focus on high-signal changes."
ACTIVITY_ROLLUP_INTRO = "Create a concise project activity summary from chunk summaries."
ACTIVITY_ROLLUP_RETURN_SPEC = "Return sections: Activity highlights, MRs / merges, Notable discussions/comments, Other activity."

def build_branch_prompt(repo: str, branch: str, parent: str, diffstat: str, patch: str, version_signals: list[str]) -> str:
    vs = "\n".join(f"- {entry}" for entry in version_signals[:40]) or "(none detected)"
    return sanitize_prompt(
        f"{BRANCH_SUMMARY_INTRO}\n\n"
        f"Repo: {repo}\nBranch: {branch}\nParent baseline: {parent}\n\n"
        f"DIFFSTAT:\n{diffstat}\n\n"
        f"VERSION/BUILD SIGNALS:\n{vs}\n\n"
        f"PATCH (curated; may be truncated):\n{patch}\n\n"
        f"{BRANCH_SUMMARY_RETURN_SPEC}"
    )

def build_repo_rollup_prompt(repo: str, branch_summaries: list[tuple[str, str]]) -> str:
    parts = [*REPO_ROLLUP_INTRO, f"Repo: {repo}", "", "BRANCH SUMMARIES:"]
    for branch, summary in branch_summaries:
        parts.append(f"\n### {branch}\n{summary.strip()}\n")
    parts.append(REPO_ROLLUP_RETURN_SPEC)
    return sanitize_prompt("\n".join(parts))

def build_activity_chunk_prompt(project_name: str, source_name: str, chunk_number: int, rows: list[dict]) -> str:
    body = [
        ACTIVITY_CHUNK_INTRO,
        f"Project: {project_name}",
        f"Source: {source_name}",
        f"Chunk: {chunk_number}",
        "",
    ]
    for event in rows:
        body.append(
            f"- {event['created_at'] or 'unknown_date'} | {event['author']} | {event['action']} | "
            f"{event['title_or_text']} | {event['url']}"
        )
    return "\n".join(body)

def build_activity_rollup_prompt(project_name: str, source_name: str, chunk_summaries: list[str]) -> str:
    prompt = [
        ACTIVITY_ROLLUP_INTRO,
        f"Project: {project_name}",
        f"Source: {source_name}",
        ACTIVITY_ROLLUP_RETURN_SPEC,
    ]
    for i, summary in enumerate(chunk_summaries, 1):
        prompt.append(f"\n### Chunk {i}\n{summary}")
    return "\n".join(prompt)
