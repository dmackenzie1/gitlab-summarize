import requests

def call_ollama(prompt: str, ollama_url: str = "http://localhost:11434/v1/completions", model: str = "qwen3.5:9b") -> str:
    headers = {"Content-Type": "application/json"}
    data = {
        "prompt": prompt,
        "max_tokens": 150
    }
    response = requests.post(ollama_url, headers=headers, json=data)
    response.raise_for_status()
    return response.json().get("choices", [{}])[0].get("text", "")
