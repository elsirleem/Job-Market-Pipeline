"""Central runtime configuration, read from environment variables.

Kept deliberately small and dependency-free so it imports cleanly in tests.
"""
from __future__ import annotations

import os
from dataclasses import dataclass, field

from dotenv import load_dotenv

load_dotenv()  # no-op in the container (env_file already populates os.environ)


def _csv(name: str, default: str) -> list[str]:
    return [v.strip() for v in os.getenv(name, default).split(",") if v.strip()]


@dataclass(frozen=True)
class Settings:
    adzuna_app_id: str = os.getenv("ADZUNA_APP_ID", "")
    adzuna_app_key: str = os.getenv("ADZUNA_APP_KEY", "")
    countries: list[str] = field(default_factory=lambda: _csv("JOB_COUNTRIES", "gb,de,nl,fr"))
    query: str = os.getenv("JOB_QUERY", "data engineer")
    max_pages: int = int(os.getenv("JOB_MAX_PAGES", "3"))
    lake_root: str = os.getenv("LAKE_ROOT", "/app/data/lake")

    def require_adzuna_creds(self) -> None:
        """Fail fast with a helpful message instead of an opaque 401 later."""
        if not self.adzuna_app_id or not self.adzuna_app_key:
            raise RuntimeError(
                "ADZUNA_APP_ID / ADZUNA_APP_KEY are not set. Copy .env.example to "
                ".env and add your free credentials from https://developer.adzuna.com/"
            )


settings = Settings()
