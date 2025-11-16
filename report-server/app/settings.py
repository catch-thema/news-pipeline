# settings.py
from pydantic_settings import BaseSettings

class OpenAISettings(BaseSettings):
    OPENAI_API_KEY: str

    model_config = {
        "env_file": ".env",
        "case_sensitive": False,
        "extra": "ignore"
    }


settings = OpenAISettings()