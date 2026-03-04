import unittest

from utils.ollama import OllamaClient


class OllamaClientExtractTests(unittest.TestCase):
    def test_extract_response_field(self):
        self.assertEqual(OllamaClient._extract_text({"response": "hello"}), "hello")

    def test_extract_message_content_for_chat_style_payload(self):
        data = {"message": {"role": "assistant", "content": "chat response"}}
        self.assertEqual(OllamaClient._extract_text(data), "chat response")


if __name__ == "__main__":
    unittest.main()
