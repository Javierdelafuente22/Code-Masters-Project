import json
import os
from google import genai
from dotenv import load_dotenv

load_dotenv()


class EnergyChatbot:
    def __init__(self, prompt_path=None):
        """Sets up the API client and loads the system prompt."""
        api_key = os.getenv("GEMINI_API_KEY")
        if not api_key:
            raise ValueError("API Key missing from .env")

        self.client = genai.Client(api_key=api_key)

        # The model to use
        self.model_name = 'gemini-2.5-flash-lite'

        if prompt_path is None:
            prompt_path = os.path.join(
                os.path.dirname(os.path.abspath(__file__)),
                "system_prompt_chatbot.txt",
            )
        with open(prompt_path, "r", encoding="utf-8") as f:
            self.system_instructions = f.read()

    def get_chat_response(self, user_input=None, audio_path=None):
        """Sends text or an audio file to the model and returns its JSON answer."""
        try:
            contents = [self.system_instructions]

            if audio_path:
                audio_file = self.client.files.upload(
                    file=audio_path,
                    config={'mime_type': 'audio/wav'}
                )

                contents.append("Input Audio:")
                contents.append(audio_file)
            elif user_input:
                contents.append(f"Input Text: \"{user_input}\"")
            else:
                return {"category": "Error", "mission_check": "No input provided."}

            response = self.client.models.generate_content(
                model=self.model_name,
                contents=contents
            )

            clean_text = response.text.replace('```json', '').replace('```', '').strip()
            return json.loads(clean_text)

        except Exception as e:
            print(f"\n[DEBUG] Full Error: {e}")
            return {"category": "Error", "mission_check": "Technical error during processing."}
