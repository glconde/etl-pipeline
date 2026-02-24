from __future__ import annotations

from dataclasses import dataclass
import os

from dotenv import load_dotenv


@dataclass(frozen=True)
class Settings:
    omdb_api_key: str
    db_url: str
    omdb_base_url: str = "https://www.omdbapi.com/"
    request_sleep_seconds: float = 0.3
    request_timeout_seconds: float = 10.0
    max_retries: int = 2


def _require(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise RuntimeError(f"Missing required env var: {name}")
    return value


def get_settings() -> Settings:
    """
    Loads .env (if present) and returns validated settings.
    """
    load_dotenv()  # loads .env from project root by default

    omdb_api_key = _require("OMDB_API_KEY")
    db_url = _require("DB_URL")

    base_url = os.getenv("OMDB_BASE_URL", "https://www.omdbapi.com/").strip() or "https://www.omdbapi.com/"
    sleep_s = float(os.getenv("REQUEST_SLEEP_SECONDS", "0.3"))
    timeout_s = float(os.getenv("REQUEST_TIMEOUT_SECONDS", "10.0"))
    max_retries = int(os.getenv("MAX_RETRIES", "2"))

    return Settings(
        omdb_api_key=omdb_api_key,
        db_url=db_url,
        omdb_base_url=base_url,
        request_sleep_seconds=sleep_s,
        request_timeout_seconds=timeout_s,
        max_retries=max_retries,
    )