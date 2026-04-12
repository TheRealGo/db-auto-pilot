from __future__ import annotations

from functools import lru_cache
from pathlib import Path

from pydantic import BaseModel, Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class AppSettingsPayload(BaseModel):
    api_key: str | None = None
    endpoint: str | None = None
    model: str | None = None


class EffectiveOpenAISettings(BaseModel):
    api_key: str | None = None
    endpoint: str | None = None
    model: str


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    openai_api_key: str | None = Field(default=None, alias="OPENAI_API_KEY")
    openai_endpoint: str | None = Field(default=None, alias="OPENAI_ENDPOINT")
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
    def app_settings_path(self) -> Path:
        return self.data_dir / "app_settings.json"

    @property
    def cors_origins(self) -> list[str]:
        return [origin.strip() for origin in self.backend_cors_origins.split(",") if origin.strip()]


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    settings = Settings()
    settings.data_dir.mkdir(parents=True, exist_ok=True)
    settings.uploads_dir.mkdir(parents=True, exist_ok=True)
    return settings


def load_app_settings(settings: Settings) -> AppSettingsPayload:
    path = settings.app_settings_path
    if not path.exists():
        return AppSettingsPayload()
    return AppSettingsPayload.model_validate_json(path.read_text(encoding="utf-8"))


def save_app_settings(settings: Settings, payload: AppSettingsPayload) -> AppSettingsPayload:
    settings.app_settings_path.write_text(
        payload.model_dump_json(exclude_none=False, indent=2),
        encoding="utf-8",
    )
    return payload


def effective_openai_settings(settings: Settings) -> EffectiveOpenAISettings:
    stored = load_app_settings(settings)
    return EffectiveOpenAISettings(
        api_key=stored.api_key or settings.openai_api_key,
        endpoint=stored.endpoint or settings.openai_endpoint,
        model=stored.model or settings.openai_model,
    )
