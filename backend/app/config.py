from __future__ import annotations

import json
import os
from functools import lru_cache
from pathlib import Path
from typing import Any

from pydantic import BaseModel, Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class AppSettingsPayload(BaseModel):
    openai_api_key: str | None = None
    openai_model: str | None = None
    llm_enabled: bool | None = None
    clear_openai_api_key: bool | None = None


class EffectiveOpenAISettings(BaseModel):
    api_key: str | None
    model: str
    llm_enabled: bool


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="DB_AUTO_PILOT_", env_file=".env", extra="ignore")

    data_dir: Path = Field(default=Path("data"))
    database_path: Path = Field(default=Path("data/db_auto_pilot.db"))
    uploads_dir: Path = Field(default=Path("data/uploads"))
    app_settings_path: Path = Field(default=Path("data/settings.json"))
    openai_api_key: str | None = None
    openai_model: str = "gpt-4.1-mini"
    llm_enabled: bool = False
    llm_data_policy: str = "metadata_only"
    max_upload_mb: int = 50
    max_materialization_rows: int = 100_000
    max_materialization_columns: int = 200
    query_row_limit: int = 500
    cors_allow_origins: str = "http://127.0.0.1:5173,http://localhost:5173"

    def ensure_dirs(self) -> None:
        self.data_dir.mkdir(parents=True, exist_ok=True)
        self.uploads_dir.mkdir(parents=True, exist_ok=True)


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    settings = Settings()
    settings.ensure_dirs()
    return settings


def load_app_settings(settings: Settings | None = None) -> AppSettingsPayload:
    settings = settings or get_settings()
    if not settings.app_settings_path.exists():
        return AppSettingsPayload()
    try:
        return AppSettingsPayload.model_validate_json(settings.app_settings_path.read_text(encoding="utf-8"))
    except Exception:
        return AppSettingsPayload()


def save_app_settings(payload: AppSettingsPayload, settings: Settings | None = None) -> AppSettingsPayload:
    settings = settings or get_settings()
    settings.ensure_dirs()
    existing = load_app_settings(settings).model_dump()
    incoming = payload.model_dump(exclude_unset=True)
    if incoming.pop("clear_openai_api_key", False):
        incoming["openai_api_key"] = ""
    existing.update({k: v for k, v in incoming.items() if v is not None})
    merged = AppSettingsPayload(**existing)
    settings.app_settings_path.write_text(json.dumps(merged.model_dump(), ensure_ascii=False, indent=2), encoding="utf-8")
    os.chmod(settings.app_settings_path, 0o600)
    return merged


def effective_openai_settings(settings: Settings | None = None) -> EffectiveOpenAISettings:
    settings = settings or get_settings()
    persisted = load_app_settings(settings)
    api_key = persisted.openai_api_key or settings.openai_api_key
    model = persisted.openai_model or settings.openai_model
    llm_enabled = settings.llm_enabled if persisted.llm_enabled is None else persisted.llm_enabled
    return EffectiveOpenAISettings(api_key=api_key, model=model, llm_enabled=bool(llm_enabled and api_key))


def public_settings_payload(settings: Settings | None = None) -> dict[str, Any]:
    effective = effective_openai_settings(settings)
    return {
        "openai_model": effective.model,
        "llm_enabled": effective.llm_enabled,
        "llm_data_policy": settings.llm_data_policy,
        "openai_api_key_configured": bool(effective.api_key),
        "max_upload_mb": settings.max_upload_mb,
        "max_materialization_rows": settings.max_materialization_rows,
        "max_materialization_columns": settings.max_materialization_columns,
        "query_row_limit": settings.query_row_limit,
        "cors_allow_origins": cors_allow_origins(settings),
    }


def cors_allow_origins(settings: Settings | None = None) -> list[str]:
    settings = settings or get_settings()
    return [origin.strip() for origin in settings.cors_allow_origins.split(",") if origin.strip()]
