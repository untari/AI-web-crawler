from openai import OpenAI
import logging

class Translator:
    def __init__(self, api_key):
        self.openai_client = OpenAI(api_key=api_key)

    def translate_text(self, text_to_translate, target_language='English'):
        try:
            chat_completion = self.openai_client.chat.completions.create(
                messages=[
                    {
                        "role": "user",
                        "content": f"Translate this to {target_language}: {text_to_translate}",
                    }
                ],
                model="gpt-4-0125-preview",
            )
            # Extract the translated text
            translated_text = chat_completion.choices[0].message.content
            return translated_text
        except Exception as e:
            logging.error(f'Error translating text: {e}')
            return None