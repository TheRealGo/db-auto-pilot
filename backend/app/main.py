from __future__ import annotations

import logging
import sqlite3
import shutil
import time
import uuid
from copy import deepcopy
from pathlib import Path
from typing import Any

import pandas as pd
from fastapi import FastAPI, File, HTTPException, Query, Request, UploadFile
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

from app.config import AppSettingsPayload, cors_allow_origins, get_settings, public_settings_payload, save_app_settings
from app.repository import MetadataRepository, SCHEMA_VERSION, utc_now
from app.version import APP_VERSION
from app.schemas import (
    AnalyticsResponse,
    AppSettingsRequest,
    AppSettingsResponse,
    ApprovalTimelineEntry,
    ApproveRequest,
    ApproveResponse,
    DatasetDetail,
    DatasetExportResponse,
    DatasetSummary,
    DiagnosticsResponse,
    MaterializationProposalApproveRequest,
    MaterializationProposalHistoryEntry,
    MaterializationProposalRequest,
    MaterializationProposalResponse,
    MaterializationRunEntry,
    ProposalHistoryEntry,
    ProposalResponse,
    ProposalRevisionRequest,
    QueryHistoryEntry,
    QueryRequest,
    QueryResponse,
    SourceFileInfo,
    TableAnalytics,
)
from app.services.ingestion import file_profile, load_dataframe_map, read_tabular_file, save_upload, slugify
from app.services.materialization import (
    MaterializationError,
    MaterializationGuardError,
    build_materialization_retry_guidance,
    generate_materialization_proposal,
    materialize_frames,
    summarize_materialization_plan_changes,
    validate_materialization_plan_references,
)
from app.services.proposals import ProposalGenerationError, generate_proposal, summarize_proposal_changes, validate_proposal_references
from app.services.querying import QueryGenerationError, generate_query, run_query_with_columns, validate_select_sql, validate_table_scope


logger = logging.getLogger("db_auto_pilot.api")


def _repository() -> MetadataRepository:
    return MetadataRepository(get_settings().database_path)


def _dataset_or_404(repo: MetadataRepository, dataset_id: str) -> dict[str, Any]:
    dataset = repo.get_dataset(dataset_id)
    if dataset is None:
        raise HTTPException(status_code=404, detail="Dataset not found")
    return dataset


def _summary(row: dict[str, Any]) -> DatasetSummary:
    return DatasetSummary.model_validate(row)


def _detail(row: dict[str, Any]) -> DatasetDetail:
    payload = dict(row)
    payload["sources"] = [SourceFileInfo.model_validate(source) for source in row.get("sources", [])]
    payload["materialization_runs"] = [
        MaterializationRunEntry.model_validate(run)
        for run in row.get("materialization_runs", [])
    ]
    return DatasetDetail.model_validate(payload)


def _export_detail(row: dict[str, Any]) -> DatasetDetail:
    payload = deepcopy(row)
    for source in payload.get("sources", []):
        profile = source.get("profile")
        if not isinstance(profile, dict):
            continue
        columns = profile.get("columns")
        if not isinstance(columns, dict):
            continue
        for column_profile in columns.values():
            if not isinstance(column_profile, dict):
                continue
            column_profile.pop("sample_values", None)
            column_profile.pop("top_values", None)
            column_profile["cell_values_redacted"] = True
    return _detail(payload)


def _export_query_history(repo: MetadataRepository, dataset_id: str, include_result_previews: bool = False) -> list[QueryHistoryEntry]:
    entries = []
    for row in repo.list_query_history(100, dataset_id):
        payload = dict(row)
        preview = payload.get("result_preview")
        if not include_result_previews:
            payload["result_preview"] = []
        elif isinstance(preview, list):
            payload["result_preview"] = preview[:5]
        entries.append(QueryHistoryEntry.model_validate(payload))
    return entries


def _materialized_table_name(dataset_id: str) -> str:
    return f"dataset_{slugify(dataset_id.replace('-', '_'))}"


def _proposal_requires_review_acknowledgement(proposal: dict[str, Any]) -> bool:
    return any(
        isinstance(group, dict) and group.get("review_required") is True
        for group in proposal.get("canonical_columns", [])
    )


def _approve_proposal_or_400(repo: MetadataRepository, dataset_id: str, request: ApproveRequest) -> tuple[str, int, dict[str, Any]]:
    latest = repo.latest_proposal(dataset_id)
    if latest is None:
        raise HTTPException(status_code=400, detail="Create a proposal before approval")
    if not request.proposal_id or request.proposal_version is None:
        raise HTTPException(status_code=400, detail="proposal_id and proposal_version are required for approval")
    if request.proposal_id != latest["id"] or int(request.proposal_version) != int(latest["version"]):
        raise HTTPException(status_code=400, detail="Proposal is stale; regenerate or approve the latest proposal")
    proposal = request.proposal or latest["proposal"]
    if proposal != latest["proposal"]:
        raise HTTPException(status_code=400, detail="Approval proposal payload must match the latest saved proposal")
    if _proposal_requires_review_acknowledgement(proposal) and not request.notes.strip():
        raise HTTPException(status_code=400, detail="Approval notes are required when proposal items are marked review_required")
    return latest["id"], int(latest["version"]), proposal


def _materialization_plan_or_400(
    repo: MetadataRepository,
    dataset_id: str,
    request: MaterializationProposalApproveRequest,
    materialization_proposal_id: str | None = None,
) -> tuple[str, int, dict[str, Any]]:
    expected_id = materialization_proposal_id or request.materialization_proposal_id
    latest = repo.latest_materialization_proposal(dataset_id)
    if latest is None:
        raise HTTPException(status_code=400, detail="Create a materialization proposal before approval")
    if not expected_id or request.materialization_proposal_version is None:
        raise HTTPException(
            status_code=400,
            detail="materialization_proposal_id and materialization_proposal_version are required for approval",
        )
    if expected_id != latest["id"] or int(request.materialization_proposal_version) != int(latest["version"]):
        raise HTTPException(status_code=400, detail="Materialization proposal is stale; approve the latest plan")
    plan = request.plan or latest["plan"]
    if plan != latest["plan"]:
        raise HTTPException(status_code=400, detail="Approval plan payload must match the latest saved materialization proposal")
    return latest["id"], int(latest["version"]), plan


def build_table_analytics(conn: sqlite3.Connection, table_name: str) -> TableAnalytics:
    quoted = '"' + table_name.replace('"', '""') + '"'
    row_count = conn.execute(f"select count(*) from {quoted}").fetchone()[0]
    columns = conn.execute(f"pragma table_info({quoted})").fetchall()
    df = pd.read_sql_query(f"select * from {quoted} limit 1000", conn)
    numeric_summaries: dict[str, dict[str, float | None]] = {}
    categorical_top_values: dict[str, list[dict[str, Any]]] = {}
    for column in df.columns:
        numeric = pd.to_numeric(df[column], errors="coerce")
        if numeric.notna().mean() >= 0.8 and numeric.notna().any():
            numeric_summaries[column] = {
                "min": float(numeric.min()),
                "max": float(numeric.max()),
                "mean": float(numeric.mean()),
                "sum": float(numeric.sum()),
            }
        else:
            categorical_top_values[column] = (
                df[column].dropna().astype(str).value_counts().head(10).rename_axis("value").reset_index(name="count").to_dict(orient="records")
            )
    recommended_charts = []
    if numeric_summaries and categorical_top_values:
        recommended_charts.append(
            {
                "type": "bar",
                "dimension": next(iter(categorical_top_values)),
                "measure": next(iter(numeric_summaries)),
                "reason": "Compare a numeric measure across top categories.",
            }
        )
    return TableAnalytics(
        table_name=table_name,
        row_count=int(row_count),
        column_count=len(columns),
        numeric_summaries=numeric_summaries,
        categorical_top_values=categorical_top_values,
        recommended_charts=recommended_charts,
    )


def build_diagnostics(settings: Any) -> DiagnosticsResponse:
    database_ready = settings.database_path.exists()
    counts: dict[str, int] = {}
    database_user_version = 0
    database_integrity = "missing"
    foreign_key_violations = 0
    migration_backups = sorted(settings.database_path.parent.glob(f"{settings.database_path.stem}.pre-migration-*.bak"))
    if database_ready:
        with sqlite3.connect(settings.database_path) as conn:
            database_user_version = int(conn.execute("pragma user_version").fetchone()[0])
            integrity_row = conn.execute("pragma integrity_check").fetchone()
            database_integrity = str(integrity_row[0]) if integrity_row else "unknown"
            foreign_key_violations = len(conn.execute("pragma foreign_key_check").fetchall())
            for table in [
                "datasets",
                "source_files",
                "proposals",
                "materialization_proposals",
                "materialization_runs",
                "query_history",
            ]:
                exists = conn.execute(
                    "select 1 from sqlite_master where type = 'table' and name = ?",
                    (table,),
                ).fetchone()
                if exists:
                    counts[table] = int(conn.execute(f"select count(*) from {table}").fetchone()[0])
                else:
                    counts[table] = 0
    status = "ok" if (
        database_ready
        and database_user_version == SCHEMA_VERSION
        and database_integrity == "ok"
        and foreign_key_violations == 0
    ) else "degraded"
    return DiagnosticsResponse(
        status=status,
        app_version=APP_VERSION,
        schema_version=SCHEMA_VERSION,
        database_user_version=database_user_version,
        database_ready=database_ready,
        uploads_dir_ready=settings.uploads_dir.exists(),
        database_integrity=database_integrity,
        foreign_key_violations=foreign_key_violations,
        migration_backup_count=len(migration_backups),
        latest_migration_backup=migration_backups[-1].name if migration_backups else None,
        counts=counts,
        settings=AppSettingsResponse(**public_settings_payload(settings)),
    )


def create_app() -> FastAPI:
    settings = get_settings()
    repo = MetadataRepository(settings.database_path)
    app = FastAPI(title="db-auto-pilot", version=APP_VERSION)
    app.add_middleware(
        CORSMiddleware,
        allow_origins=cors_allow_origins(settings),
        allow_credentials=False,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    @app.middleware("http")
    async def request_id_logging(request: Request, call_next: Any) -> Any:
        request_id = request.headers.get("x-request-id") or str(uuid.uuid4())
        started = time.perf_counter()
        try:
            response = await call_next(request)
        except Exception:
            elapsed_ms = (time.perf_counter() - started) * 1000
            logger.exception(
                "request failed request_id=%s method=%s path=%s duration_ms=%.2f",
                request_id,
                request.method,
                request.url.path,
                elapsed_ms,
            )
            raise
        elapsed_ms = (time.perf_counter() - started) * 1000
        response.headers["x-request-id"] = request_id
        logger.info(
            "request complete request_id=%s method=%s path=%s status=%s duration_ms=%.2f",
            request_id,
            request.method,
            request.url.path,
            response.status_code,
            elapsed_ms,
        )
        return response

    @app.get("/health")
    def health() -> dict[str, str]:
        return {"status": "ok"}

    @app.get("/diagnostics", response_model=DiagnosticsResponse)
    def diagnostics() -> DiagnosticsResponse:
        return build_diagnostics(settings)

    @app.post("/datasets", response_model=DatasetDetail)
    @app.post("/datasets/upload", response_model=DatasetDetail)
    async def upload_dataset(files: list[UploadFile] = File(...), name: str | None = None) -> DatasetDetail:
        if not files:
            raise HTTPException(status_code=400, detail="At least one CSV or Excel file is required")
        pending_dir = settings.uploads_dir / f"_pending_{uuid.uuid4().hex}"
        pending_dir.mkdir(parents=True, exist_ok=True)
        staged: list[tuple[int, UploadFile, Path, dict[str, pd.DataFrame]]] = []
        try:
            for index, upload in enumerate(files):
                suffix = Path(upload.filename or "upload.csv").suffix
                pending_path = pending_dir / f"{index}_{slugify(Path(upload.filename or 'upload').stem)}{suffix}"
                with pending_path.open("wb") as fh:
                    shutil.copyfileobj(upload.file, fh)
                if pending_path.stat().st_size > settings.max_upload_mb * 1024 * 1024:
                    raise ValueError(f"Uploaded file exceeds {settings.max_upload_mb} MB limit: {upload.filename or pending_path.name}")
                frames = read_tabular_file(pending_path)
                staged.append((index, upload, pending_path, frames))
        except ValueError as exc:
            shutil.rmtree(pending_dir, ignore_errors=True)
            raise HTTPException(status_code=400, detail=str(exc)) from exc

        dataset_id = repo.create_dataset(name or Path(files[0].filename or "dataset").stem)
        dataset_upload_dir = settings.uploads_dir / dataset_id
        dataset_upload_dir.mkdir(parents=True, exist_ok=True)
        for index, upload, pending_path, frames in staged:
            stored = dataset_upload_dir / pending_path.name
            pending_path.replace(stored)
            for sheet_name, frame in frames.items():
                table_name = slugify(f"{index}_{Path(upload.filename or 'upload').stem}_{sheet_name}")
                repo.add_source_file(
                    dataset_id=dataset_id,
                    filename=upload.filename or stored.name,
                    sheet_name=None if sheet_name == "default" else sheet_name,
                    table_name=table_name,
                    file_path=stored,
                    rows=len(frame),
                    columns=list(frame.columns),
                    profile=file_profile(frame),
                )
        shutil.rmtree(pending_dir, ignore_errors=True)
        return _detail(_dataset_or_404(repo, dataset_id))

    @app.get("/datasets", response_model=list[DatasetSummary])
    def list_datasets() -> list[DatasetSummary]:
        return [_summary(row) for row in repo.list_datasets()]

    @app.get("/datasets/{dataset_id}", response_model=DatasetDetail)
    def get_dataset(dataset_id: str) -> DatasetDetail:
        return _detail(_dataset_or_404(repo, dataset_id))

    @app.post("/datasets/{dataset_id}/proposal/revise", response_model=ProposalResponse)
    @app.post("/datasets/{dataset_id}/proposal", response_model=ProposalResponse)
    def create_proposal(dataset_id: str, request: ProposalRevisionRequest | None = None) -> ProposalResponse:
        dataset = _dataset_or_404(repo, dataset_id)
        frames = load_dataframe_map(dataset["sources"])
        previous = repo.latest_proposal(dataset_id)
        proposal = generate_proposal(frames, feedback=(request.feedback if request else ""), settings=settings)
        try:
            validate_proposal_references(proposal, frames)
        except ProposalGenerationError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        proposal["change_summary"] = summarize_proposal_changes((previous or {}).get("proposal"), proposal)
        proposal_id = repo.save_proposal(dataset_id, proposal, feedback=(request.feedback if request else ""))
        latest = repo.latest_proposal(dataset_id)
        version = int(latest["version"]) if latest else 1
        return ProposalResponse(dataset_id=dataset_id, proposal_id=proposal_id, version=version, proposal=proposal)

    @app.get("/datasets/{dataset_id}/proposals", response_model=list[ProposalHistoryEntry])
    def proposal_history(dataset_id: str, limit: int = Query(default=20, ge=1, le=100)) -> list[ProposalHistoryEntry]:
        _dataset_or_404(repo, dataset_id)
        return [ProposalHistoryEntry.model_validate(row) for row in repo.list_proposals(dataset_id, limit)]

    @app.post("/datasets/{dataset_id}/approve", response_model=ApproveResponse)
    def approve_dataset(dataset_id: str, request: ApproveRequest) -> ApproveResponse:
        dataset = _dataset_or_404(repo, dataset_id)
        proposal_id, proposal_version, proposal = _approve_proposal_or_400(repo, dataset_id, request)
        frames = load_dataframe_map(dataset["sources"])
        try:
            validate_proposal_references(proposal, frames)
        except ProposalGenerationError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        plan = generate_materialization_proposal(proposal, request.notes)
        try:
            validate_materialization_plan_references(plan, frames)
        except MaterializationGuardError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        try:
            result = materialize_frames(
                settings.database_path,
                dataset_id,
                frames,
                plan,
                max_rows=settings.max_materialization_rows,
                max_columns=settings.max_materialization_columns,
            )
        except MaterializationError as exc:
            guidance = build_materialization_retry_guidance(exc)
            repo.record_materialization_failure(
                dataset_id,
                _materialized_table_name(dataset_id),
                {**plan, "retry_guidance": guidance},
                str(exc),
            )
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        repo.mark_proposal_approved(dataset_id, proposal)
        repo.add_approval_decision(
            dataset_id,
            proposal_id,
            "proposal_and_materialization_approved",
            request.notes,
            {"proposal_id": proposal_id, "proposal_version": proposal_version, "proposal": proposal, "plan": plan},
        )
        repo.save_materialization(dataset_id, result["table_name"], result)
        return ApproveResponse(
            dataset_id=dataset_id,
            status="materialized",
            materialized_table=result["table_name"],
            rows=result["rows"],
            columns=result["columns"],
        )

    @app.post("/datasets/{dataset_id}/proposal/approve", response_model=DatasetDetail)
    def approve_integration_proposal(dataset_id: str, request: ApproveRequest) -> DatasetDetail:
        dataset = _dataset_or_404(repo, dataset_id)
        proposal_id, proposal_version, proposal = _approve_proposal_or_400(repo, dataset_id, request)
        frames = load_dataframe_map(dataset["sources"])
        try:
            validate_proposal_references(proposal, frames)
        except ProposalGenerationError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        repo.mark_proposal_approved(dataset_id, proposal)
        repo.add_approval_decision(
            dataset_id,
            proposal_id,
            "proposal_approved",
            request.notes,
            {"proposal_id": proposal_id, "proposal_version": proposal_version, "proposal": proposal},
        )
        return _detail(_dataset_or_404(repo, dataset_id))

    @app.post("/datasets/{dataset_id}/materialization-proposal", response_model=MaterializationProposalResponse)
    def materialization_proposal(dataset_id: str, request: MaterializationProposalRequest) -> MaterializationProposalResponse:
        dataset = _dataset_or_404(repo, dataset_id)
        if not dataset.get("proposal"):
            raise HTTPException(status_code=400, detail="Proposal required first")
        if dataset.get("status") not in {"approved", "materialized"}:
            raise HTTPException(status_code=400, detail="Approve the integration proposal before materialization proposal generation")
        previous = repo.latest_materialization_proposal(dataset_id)
        plan = generate_materialization_proposal(dataset["proposal"], request.feedback)
        plan["change_summary"] = summarize_materialization_plan_changes((previous or {}).get("plan"), plan)
        proposal_id = repo.save_materialization_proposal(dataset_id, plan, request.feedback)
        latest = repo.latest_materialization_proposal(dataset_id)
        return MaterializationProposalResponse(
            dataset_id=dataset_id,
            materialization_proposal_id=proposal_id,
            version=int(latest["version"]) if latest else 1,
            plan=plan,
        )

    @app.get("/datasets/{dataset_id}/materialization-proposals", response_model=list[MaterializationProposalHistoryEntry])
    def materialization_proposal_history(dataset_id: str, limit: int = Query(default=20, ge=1, le=100)) -> list[MaterializationProposalHistoryEntry]:
        _dataset_or_404(repo, dataset_id)
        return [
            MaterializationProposalHistoryEntry.model_validate(row)
            for row in repo.list_materialization_proposals(dataset_id, limit)
        ]

    @app.post("/datasets/{dataset_id}/materialization-proposal/{materialization_proposal_id}/retry", response_model=MaterializationProposalResponse)
    def retry_materialization_proposal(
        dataset_id: str,
        materialization_proposal_id: str,
        request: MaterializationProposalRequest,
    ) -> MaterializationProposalResponse:
        dataset = _dataset_or_404(repo, dataset_id)
        latest = repo.latest_materialization_proposal(dataset_id)
        if latest is None:
            raise HTTPException(status_code=400, detail="Create a materialization proposal before retry")
        if materialization_proposal_id != latest["id"]:
            raise HTTPException(status_code=400, detail="Materialization proposal is stale; retry the latest proposal")
        failed_run = repo.latest_materialization_run(dataset_id, "failed")
        if failed_run is None:
            raise HTTPException(status_code=400, detail="No failed materialization run is available to retry")
        feedback_parts = [
            latest.get("feedback", ""),
            request.feedback,
            failed_run.get("plan", {}).get("retry_guidance", ""),
            failed_run.get("error") or "",
        ]
        retry_feedback = "\n".join(part for part in feedback_parts if part)
        plan = generate_materialization_proposal(dataset["proposal"], retry_feedback)
        plan["retry"] = {
            "source_materialization_proposal_id": materialization_proposal_id,
            "source_materialization_proposal_version": latest["version"],
            "failed_materialization_run_id": failed_run["id"],
            "failed_error": failed_run.get("error"),
            "retry_guidance": failed_run.get("plan", {}).get("retry_guidance"),
            "user_feedback": request.feedback,
        }
        plan["change_summary"] = summarize_materialization_plan_changes(latest["plan"], plan)
        proposal_id = repo.save_materialization_proposal(dataset_id, plan, retry_feedback)
        saved = repo.latest_materialization_proposal(dataset_id)
        return MaterializationProposalResponse(
            dataset_id=dataset_id,
            materialization_proposal_id=proposal_id,
            version=int(saved["version"]) if saved else 1,
            plan=plan,
        )

    @app.post("/datasets/{dataset_id}/materialization-proposal/{materialization_proposal_id}/approve", response_model=ApproveResponse)
    @app.post("/datasets/{dataset_id}/materialization-approve", response_model=ApproveResponse)
    def materialization_approve(
        dataset_id: str,
        request: MaterializationProposalApproveRequest,
        materialization_proposal_id: str | None = None,
    ) -> ApproveResponse:
        dataset = _dataset_or_404(repo, dataset_id)
        materialization_proposal_id, materialization_proposal_version, plan = _materialization_plan_or_400(
            repo,
            dataset_id,
            request,
            materialization_proposal_id,
        )
        frames = load_dataframe_map(dataset["sources"])
        try:
            validate_materialization_plan_references(plan, frames)
        except MaterializationGuardError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        try:
            result = materialize_frames(
                settings.database_path,
                dataset_id,
                frames,
                plan,
                max_rows=settings.max_materialization_rows,
                max_columns=settings.max_materialization_columns,
            )
        except MaterializationError as exc:
            guidance = build_materialization_retry_guidance(exc)
            repo.record_materialization_failure(
                dataset_id,
                _materialized_table_name(dataset_id),
                {**plan, "retry_guidance": guidance},
                str(exc),
            )
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        repo.add_approval_decision(
            dataset_id,
            None,
            "materialization_approved",
            request.notes,
            {
                "materialization_proposal_id": materialization_proposal_id,
                "materialization_proposal_version": materialization_proposal_version,
                "plan": plan,
            },
        )
        repo.save_materialization(dataset_id, result["table_name"], result)
        return ApproveResponse(
            dataset_id=dataset_id,
            status="materialized",
            materialized_table=result["table_name"],
            rows=result["rows"],
            columns=result["columns"],
        )

    @app.post("/datasets/{dataset_id}/query", response_model=QueryResponse)
    def dataset_query(dataset_id: str, request: QueryRequest) -> QueryResponse:
        request.dataset_id = dataset_id
        return query(request)

    @app.post("/query", response_model=QueryResponse)
    def query(request: QueryRequest) -> QueryResponse:
        if not request.dataset_id:
            raise HTTPException(status_code=400, detail="dataset_id is required for governed query access")
        if request.limit > settings.query_row_limit:
            raise HTTPException(
                status_code=400,
                detail=f"Query limit {request.limit} exceeds configured maximum {settings.query_row_limit}",
            )
        dataset = repo.get_dataset(request.dataset_id)
        if dataset is None:
            raise HTTPException(status_code=404, detail="Dataset not found")
        dataset_table = dataset.get("materialized_table")
        if not dataset_table:
            raise HTTPException(status_code=400, detail="Dataset must be approved and materialized before querying")
        with sqlite3.connect(settings.database_path) as conn:
            conn.row_factory = sqlite3.Row
            if request.sql:
                try:
                    sql = validate_select_sql(request.sql, request.limit)
                    validate_table_scope(conn, sql, {dataset_table})
                except QueryGenerationError as exc:
                    raise HTTPException(status_code=400, detail=str(exc)) from exc
                explanation = "User-provided read-only SQL was executed."
            else:
                try:
                    sql, explanation = generate_query(request.question, conn, dataset_table, settings, request.limit)
                    validate_table_scope(conn, sql, {dataset_table})
                except QueryGenerationError as exc:
                    raise HTTPException(status_code=400, detail=str(exc)) from exc
            try:
                rows, columns = run_query_with_columns(conn, sql, {dataset_table}, max_rows=request.limit)
            except QueryGenerationError as exc:
                raise HTTPException(status_code=400, detail=str(exc)) from exc
        latest_proposal = repo.latest_proposal(request.dataset_id)
        latest_materialization_proposal = repo.latest_materialization_proposal(request.dataset_id)
        history_id = repo.add_query_history(
            request.dataset_id,
            request.question,
            sql,
            len(rows),
            explanation=explanation,
            columns=columns,
            result_preview=rows,
            materialized_table=dataset_table,
            materialization_run_id=repo.latest_materialization_run_id(request.dataset_id, dataset_table),
            target_mode="merged",
            proposal_id=(latest_proposal or {}).get("id"),
            proposal_version=(latest_proposal or {}).get("version"),
            materialization_proposal_id=(latest_materialization_proposal or {}).get("id"),
            materialization_proposal_version=(latest_materialization_proposal or {}).get("version"),
        )
        return QueryResponse(sql=sql, rows=rows, columns=columns, explanation=explanation, history_id=history_id)

    @app.get("/query/history", response_model=list[QueryHistoryEntry])
    def query_history(limit: int = Query(default=50, ge=1, le=200)) -> list[QueryHistoryEntry]:
        return [QueryHistoryEntry.model_validate(row) for row in repo.list_query_history(limit)]

    @app.get("/datasets/{dataset_id}/query-history", response_model=list[QueryHistoryEntry])
    def dataset_query_history(dataset_id: str, limit: int = Query(default=50, ge=1, le=200)) -> list[QueryHistoryEntry]:
        _dataset_or_404(repo, dataset_id)
        return [QueryHistoryEntry.model_validate(row) for row in repo.list_query_history(limit, dataset_id)]

    @app.get("/datasets/{dataset_id}/approvals", response_model=list[ApprovalTimelineEntry])
    def approval_timeline(dataset_id: str) -> list[ApprovalTimelineEntry]:
        _dataset_or_404(repo, dataset_id)
        return [ApprovalTimelineEntry.model_validate(row) for row in repo.list_approval_decisions(dataset_id)]

    @app.get("/datasets/{dataset_id}/analytics", response_model=AnalyticsResponse)
    def analytics(dataset_id: str) -> AnalyticsResponse:
        dataset = _dataset_or_404(repo, dataset_id)
        table = dataset.get("materialized_table")
        if not table:
            raise HTTPException(status_code=400, detail="Dataset is not materialized")
        with sqlite3.connect(settings.database_path) as conn:
            return AnalyticsResponse(
                dataset_id=dataset_id,
                materialized_table=table,
                materialization_run_id=repo.latest_materialization_run_id(dataset_id, table),
                tables=[build_table_analytics(conn, table)],
            )

    @app.get("/datasets/{dataset_id}/export", response_model=DatasetExportResponse)
    def export_dataset_evidence(
        dataset_id: str,
        include_query_previews: bool = Query(False, description="Include up to 5 query result preview rows per query history entry."),
    ) -> DatasetExportResponse:
        dataset = _dataset_or_404(repo, dataset_id)
        analytics_payload: AnalyticsResponse | None = None
        table = dataset.get("materialized_table")
        if table:
            with sqlite3.connect(settings.database_path) as conn:
                analytics_payload = AnalyticsResponse(
                    dataset_id=dataset_id,
                    materialized_table=table,
                    materialization_run_id=repo.latest_materialization_run_id(dataset_id, table),
                    tables=[build_table_analytics(conn, table)],
                )
        return DatasetExportResponse(
            exported_at=utc_now(),
            runtime_provenance=build_diagnostics(settings),
            dataset=_export_detail(dataset),
            proposal_history=[ProposalHistoryEntry.model_validate(row) for row in repo.list_proposals(dataset_id, 100)],
            materialization_proposal_history=[
                MaterializationProposalHistoryEntry.model_validate(row)
                for row in repo.list_materialization_proposals(dataset_id, 100)
            ],
            approvals=[ApprovalTimelineEntry.model_validate(row) for row in repo.list_approval_decisions(dataset_id)],
            query_history_previews_included=include_query_previews,
            query_history=_export_query_history(repo, dataset_id, include_query_previews),
            analytics=analytics_payload,
        )

    @app.get("/settings", response_model=AppSettingsResponse)
    def get_app_settings() -> AppSettingsResponse:
        return AppSettingsResponse(**public_settings_payload(settings))

    @app.put("/settings", response_model=AppSettingsResponse)
    def update_app_settings(request: AppSettingsRequest) -> AppSettingsResponse:
        save_app_settings(AppSettingsPayload(**request.model_dump(exclude_unset=True)), settings)
        return AppSettingsResponse(**public_settings_payload(settings))

    dist_dir = Path(__file__).resolve().parents[2] / "frontend" / "dist"
    if (dist_dir / "assets").exists():
        app.mount("/assets", StaticFiles(directory=dist_dir / "assets"), name="assets")

        @app.get("/")
        def index() -> FileResponse:
            return FileResponse(dist_dir / "index.html")

    return app


app = create_app()
