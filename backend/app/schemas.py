from __future__ import annotations

from datetime import datetime
from typing import Any, Literal

from pydantic import BaseModel, Field


class DatasetSummary(BaseModel):
    id: str
    name: str
    status: str
    created_at: datetime
    updated_at: datetime
    approved_proposal_id: str | None = None


class SourceFileInfo(BaseModel):
    id: str
    dataset_id: str
    filename: str
    stored_path: str
    file_type: str
    sheet_count: int
    created_at: datetime


class ProposalRequest(BaseModel):
    dataset_name: str | None = None


class ProposalRevisionRequest(BaseModel):
    feedback: str = Field(min_length=1)


class ProposalResponse(BaseModel):
    id: str
    dataset_id: str
    version: int
    status: str
    feedback: str | None = None
    proposal: dict[str, Any]
    created_at: datetime


class ApproveRequest(BaseModel):
    approved_proposal_id: str


class ApproveResponse(BaseModel):
    dataset_id: str
    approved_proposal_id: str
    created_tables: list[dict[str, str]]


class DatasetDetail(BaseModel):
    dataset: DatasetSummary
    source_files: list[SourceFileInfo]
    latest_proposal: ProposalResponse | None = None
    tables: list[dict[str, Any]]
    approval_decisions: list[dict[str, Any]] = []
    column_lineage: list[dict[str, Any]] = []


class QueryRequest(BaseModel):
    target_mode: Literal["raw", "merged"]
    question: str = Field(min_length=1)


class QueryResponse(BaseModel):
    sql: str
    explanation: str
    generator: str
    warning: str | None = None
    columns: list[str]
    rows: list[list[Any]]


class QueryHistoryEntry(BaseModel):
    id: str
    dataset_id: str
    target_mode: Literal["raw", "merged"]
    question: str
    sql: str
    explanation: str
    result: dict[str, Any]
    created_at: datetime


class AppSettingsRequest(BaseModel):
    api_key: str | None = None
    endpoint: str | None = None
    model: str | None = None


class AppSettingsResponse(BaseModel):
    api_key: str | None = None
    endpoint: str | None = None
    model: str | None = None
