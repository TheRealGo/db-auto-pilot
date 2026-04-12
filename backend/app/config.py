from __future__ import annotations

from functools import lru_cache
from pathlib import Path

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    openai_api_key: str | None = Field(default=None, alias="OPENAI_API_KEY")
    openai_model: str = Field(default="gpt-4.1-mini", alias="OPENAI_MODEL")
    data_dir: Path = Field(default=Path("./data"), alias="DB_AUTO_PILOT_DATA_DIR")
    backend_cors_origins: str = Field(
        default="http://localhost:5173",
        alias="BACKEND_CORS_ORIGINS",
    )
    frontend_dist_dir: Path = Field(default=Path("../frontend/dist"), alias="FRONTEND_DIST_DIR")

    @property
    def uploads_dir(self) -> Path:
        return self.data_dir / "uploads"

    @property
    def sqlite_path(self) -> Path:
        return self.data_dir / "db_auto_pilot.db"

    @property
    def resolved_frontend_dist_dir(self) -> Path:
        return self.frontend_dist_dir.resolve()

    @property
    def cors_origins(self) -> list[str]:
        return [origin.strip() for origin in self.backend_cors_origins.split(",") if origin.strip()]


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    settings = Settings()
    settings.data_dir.mkdir(parents=True, exist_ok=True)
    settings.uploads_dir.mkdir(parents=True, exist_ok=True)
    return settings
