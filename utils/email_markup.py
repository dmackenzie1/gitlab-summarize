import html
from typing import Iterable

def _to_lines(items: Iterable[str]) -> str:
    return "".join(f"<li>{html.escape(item)}</li>" for item in items if item.strip())

def render_email_markup(markup: str, title: str = "Weekly Summary") -> str:
    lines = [line.rstrip() for line in markup.splitlines()]
    html_lines: list[str] = [
        "<html><body style=\"font-family:Arial,Helvetica,sans-serif;color:#222\">",
        f"<h2 style=\"margin-bottom:8px\">{html.escape(title)}</h2>",
    ]
    bullets: list[str] = []
    for line in lines:
        if line.startswith("#"):
            if bullets:
                html_lines.append(f"<ul>{_to_lines(bullets)}</ul>")
                bullets.clear()
            lvl = min(4, max(2, line.count("#")))
            html_lines.append(f"<h{lvl}>{html.escape(line.lstrip('# ').strip())}</h{lvl}>")
        elif line.startswith("- "):
            bullets.append(line[2:].strip())
        elif line.strip():
            if bullets:
                html_lines.append(f"<ul>{_to_lines(bullets)}</ul>")
                bullets.clear()
            html_lines.append(f"<p>{html.escape(line.strip())}</p>")
    if bullets:
        html_lines.append(f"<ul>{_to_lines(bullets)}</ul>")
    html_lines.append("</body></html>")
    return "\n".join(html_lines) + "\n"
