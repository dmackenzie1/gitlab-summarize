from typing import Optional, Tuple
import logging
import requests


def call_ollama(
    prompt: str,
    ollama_url: str = "http://localhost:11434/api/generate",
    model: str = "llama3.1:latest",
    num_ctx: int = 16384,
    num_predict: int = 800,
    timeout_s: int = 600,
    debug: bool = False,
) -> Tuple[Optional[str], Optional[str]]:
    """
    Call Ollama /api/generate.

    Returns:
      (response_text, None) on success
      (None, error_message) on failure
    """
    headers = {"Content-Type": "application/json"}

    # Keep this short; avoid triggering “explain your reasoning” behaviors.
    instruction = (
        "Return only the final answer. "
        "No chain-of-thought, no 'Thinking', no preamble."
    )

    data = {
        "model": model,
        "prompt": f"{instruction}\n\n{prompt}",
        "stream": False,
        "options": {
            "num_ctx": num_ctx,
            "num_predict": num_predict,
        },
    }

    try:
        r = requests.post(ollama_url, headers=headers, json=data, timeout=timeout_s)
        r.raise_for_status()

        obj = r.json()
        if debug:
            logging.info(f"Ollama raw keys: {list(obj.keys())}")

        resp = obj.get("response", "")
        if isinstance(resp, str) and resp.strip():
            return resp.strip(), None

        # If the server ever returns something unexpected, make it debuggable
        if debug:
            logging.error(f"Ollama returned empty response. Full payload: {obj!r}")

        return None, "Ollama returned an empty 'response' field."

    except requests.exceptions.HTTPError as e:
        # include response body if present (helps a ton)
        body = ""
        try:
            body = f" | body={r.text[:500]!r}"
        except Exception:
            pass
        return None, f"HTTP error occurred: {e}{body}"

    except ValueError as e:
        # JSON parse error
        snippet = ""
        try:
            snippet = f" | raw={r.text[:500]!r}"
        except Exception:
            pass
        return None, f"JSON parse error: {e}{snippet}"

    except Exception as e:
        return None, f"An error occurred: {e}"