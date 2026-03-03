import json
import time
import requests

from utils.models import OllamaError, OllamaResult

class OllamaClient:
    def __init__(
        self,
        url: str,
        model: str,
        timeout_s: int = 240,
        retries: int = 3,
        backoff_s: float = 0.4,
        keep_alive: str | None = None,
    ) -> None:
        self.url = url
        self.model = model
        self.timeout_s = timeout_s
        self.retries = retries
        self.backoff_s = backoff_s
        self.keep_alive = keep_alive
        self.session = requests.Session()

    def generate(self, prompt: str) -> OllamaResult:
        payload = {"model": self.model, "stream": False, "prompt": prompt}
        if self.keep_alive:
            payload["keep_alive"] = self.keep_alive

        last_error: OllamaError | None = None
        for attempt in range(self.retries):
            try:
                response = self.session.post(self.url, json=payload, timeout=self.timeout_s)
                response.raise_for_status()
                data = response.json()
                text = data.get("response", "")
                if isinstance(text, str) and text.strip():
                    return OllamaResult(text=text.strip(), error=None)
                last_error = OllamaError("Ollama returned empty response", details=json.dumps(data)[:1000])
            except requests.RequestException as exc:
                status = exc.response.status_code if getattr(exc, "response", None) is not None else None
                body = None
                if getattr(exc, "response", None) is not None:
                    body = exc.response.text[:1500]
                last_error = OllamaError(message=str(exc), status_code=status, details=body)
            if attempt < self.retries - 1:
                time.sleep(self.backoff_s * (2 ** attempt))

        return OllamaResult(text=None, error=last_error or OllamaError("unknown ollama error"))
