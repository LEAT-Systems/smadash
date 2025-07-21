import os
from typing import List
from dotenv import load_dotenv

load_dotenv()


class Settings:
    """Application settings."""

    def __init__(self):
        # API configuration
        self.API_V1_STR: str = "/api/v1"

        # Project metadata
        self.PROJECT_NAME: str = "Modular FastAPI Project"
        self.PROJECT_DESCRIPTION: str = "A modular FastAPI application with clean separation of concerns."
        self.VERSION: str = "0.1.0"

        # CORS configuration
        self.CORS_ORIGINS: List[str] = [
            "http://localhost:3000",  # React frontend default
            "http://localhost:8080",  # Vue.js frontend default
        ]

        # Database configuration
        self.DATABASE_URL: str = os.getenv(
            "DATABASE_URL"
        )

        # Security
        self.SECRET_KEY: str = os.getenv("SECRET_KEY", "development_secret_key")
        self.ALGORITHM: str = "HS256"
        self.ACCESS_TOKEN_EXPIRE_MINUTES: int = int(os.getenv("ACCESS_TOKEN_EXPIRE_MINUTES", "30"))

        # # Load environment variables from .env file if it exists
        # self._load_env_file()

    def _load_env_file(self):
        """Load environment variables from .env file if it exists."""
        env_file_path = ".env"
        if os.path.exists(env_file_path):
            with open(env_file_path, 'r') as f:
                for line in f:
                    line = line.strip()
                    if line and not line.startswith('#') and '=' in line:
                        key, value = line.split('=', 1)
                        key = key.strip()
                        value = value.strip().strip('"').strip("'")

                        # Update settings if the environment variable exists
                        if key == "DATABASE_URL":
                            self.DATABASE_URL = value
                        elif key == "SECRET_KEY":
                            self.SECRET_KEY = value
                        elif key == "ACCESS_TOKEN_EXPIRE_MINUTES":
                            try:
                                self.ACCESS_TOKEN_EXPIRE_MINUTES = int(value)
                            except ValueError:
                                pass  # Keep default value if conversion fails


settings = Settings()