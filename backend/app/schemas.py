from __future__ import annotations

from datetime import datetime
from typing import Any, Literal

from pydantic import BaseModel, Field


class SourceFileInfo(BaseModel):
    id: str
    filename: str
    sheet_name: str | None = None
    table_name: str
    rows: int
    columns: list[str]
    profile: dict[str, Any] = Field(default_factory=dict)


class DatasetSummary(BaseModel):
    id: str
    name: str
    status: str
    created_at: datetime
    updated_at: datetime
    source_count: int
    materialized_table: str | None = None


class MaterializationRunEntry(BaseModel):
    id: str
    dataset_id: str
    table_name: str
    status: str
    row_count: int = 0
    column_count: int = 0
    plan: dict[str, Any] = Field(default_factory=dict)
    error: str | None = None
    created_at: datetime


class DatasetDetail(DatasetSummary):
    sources: list[SourceFileInfo] = Field(default_factory=list)
    proposal: dict[str, Any] | None = None
    proposal_id: str | None = None
    proposal_version: int | None = None
    materialization: dict[str, Any] | None = None
    materialization_runs: list[MaterializationRunEntry] = Field(default_factory=list)


class ProposalRevisionRequest(BaseModel):
    feedback: str = ""


class ProposalResponse(BaseModel):
    dataset_id: str
    proposal_id: str
    version: int
    proposal: dict[str, Any]


class ProposalHistoryEntry(BaseModel):
    id: str
    dataset_id: str
    version: int
    feedback: str = ""
    proposal: dict[str, Any]
    created_at: datetime


class ApproveRequest(BaseModel):
    proposal: dict[str, Any] | None = None
    proposal_id: str | None = None
    proposal_version: int | None = None
    notes: str = ""


class ApproveResponse(BaseModel):
    dataset_id: str
    status: str
    materialized_table: str
    rows: int
    columns: list[str]


class MaterializationProposalRequest(BaseModel):
    feedback: str = ""


class MaterializationProposalResponse(BaseModel):
    dataset_id: str
    materialization_proposal_id: str
    version: int
    plan: dict[str, Any]


class MaterializationProposalHistoryEntry(BaseModel):
    id: str
    dataset_id: str
    version: int
    feedback: str = ""
    plan: dict[str, Any]
    created_at: datetime


class MaterializationProposalApproveRequest(BaseModel):
    plan: dict[str, Any] | None = None
    materialization_proposal_id: str | None = None
    materialization_proposal_version: int | None = None
    notes: str = ""


class QueryRequest(BaseModel):
    question: str
    dataset_id: str | None = None
    sql: str | None = None
    limit: int = Field(default=200, ge=1, le=1000)


class QueryResponse(BaseModel):
    sql: str
    rows: list[dict[str, Any]]
    columns: list[str]
    explanation: str
    history_id: str


class QueryHistoryEntry(BaseModel):
    id: str
    dataset_id: str | None = None
    question: str
    sql: str
    row_count: int
    created_at: datetime
    explanation: str = ""
    columns: list[str] = Field(default_factory=list)
    result_preview: list[dict[str, Any]] = Field(default_factory=list)
    materialized_table: str | None = None
    materialization_run_id: str | None = None
    target_mode: str = "merged"
    proposal_id: str | None = None
    proposal_version: int | None = None
    materialization_proposal_id: str | None = None
    materialization_proposal_version: int | None = None


class ApprovalTimelineEntry(BaseModel):
    id: str
    dataset_id: str
    proposal_id: str | None = None
    decision_type: str
    notes: str = ""
    payload: dict[str, Any] = Field(default_factory=dict)
    created_at: datetime


class TableAnalytics(BaseModel):
    table_name: str
    row_count: int
    column_count: int
    numeric_summaries: dict[str, dict[str, float | None]] = Field(default_factory=dict)
    categorical_top_values: dict[str, list[dict[str, Any]]] = Field(default_factory=dict)
    recommended_charts: list[dict[str, Any]] = Field(default_factory=list)


class AnalyticsResponse(BaseModel):
    dataset_id: str
    materialized_table: str | None = None
    materialization_run_id: str | None = None
    tables: list[TableAnalytics]


class AppSettingsRequest(BaseModel):
    openai_api_key: str | None = None
    openai_model: str | None = None
    llm_enabled: bool | None = None
    clear_openai_api_key: bool | None = None


class AppSettingsResponse(BaseModel):
    openai_model: str
    llm_enabled: bool
    llm_data_policy: str
    openai_api_key_configured: bool
    max_upload_mb: int
    max_materialization_rows: int
    max_materialization_columns: int
    query_row_limit: int
    cors_allow_origins: list[str] = Field(default_factory=list)


class DiagnosticsResponse(BaseModel):
    status: str
    app_version: str
    schema_version: int
    database_user_version: int
    database_ready: bool
    uploads_dir_ready: bool
    database_integrity: str = "unknown"
    foreign_key_violations: int = 0
    migration_backup_count: int = 0
    latest_migration_backup: str | None = None
    counts: dict[str, int] = Field(default_factory=dict)
    settings: AppSettingsResponse


class DatasetExportResponse(BaseModel):
    exported_at: datetime
    runtime_provenance: DiagnosticsResponse
    dataset: DatasetDetail
    proposal_history: list[ProposalHistoryEntry] = Field(default_factory=list)
    materialization_proposal_history: list[MaterializationProposalHistoryEntry] = Field(default_factory=list)
    approvals: list[ApprovalTimelineEntry] = Field(default_factory=list)
    query_history_previews_included: bool = False
    query_history: list[QueryHistoryEntry] = Field(default_factory=list)
    analytics: AnalyticsResponse | None = None


DatasetStatus = Literal["uploaded", "proposed", "approved", "materialized", "failed"]
