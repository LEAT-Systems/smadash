import os
from typing import List

from pydantic import AnyHttpUrl, BaseSettings


class Settings(BaseSettings):
    """Application settings."""

    # API configuration
    API_V1_STR: str = "/api/v1"

    # Project metadata
    PROJECT_NAME: str = "Modular FastAPI Project"
    PROJECT_DESCRIPTION: str = "A modular FastAPI application with clean separation of concerns."
    VERSION: str = "0.1.0"

    # CORS configuration
    CORS_ORIGINS: List[AnyHttpUrl] = [
        "http://localhost:3000",  # React frontend default
        "http://localhost:8080",  # Vue.js frontend default
    ]

    # Database configuration
    DATABASE_URL: str = os.getenv(
        "DATABASE_URL", "sqlite:///./app.db"
    )

    # Security
    SECRET_KEY: str = os.getenv("SECRET_KEY", "development_secret_key")
    ALGORITHM: str = "HS256"
    ACCESS_TOKEN_EXPIRE_MINUTES: int = 30

    class Config:
        """Pydantic config."""
        case_sensitive = True
        env_file = ".env"


settings = Settings()
