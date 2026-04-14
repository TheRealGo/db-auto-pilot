from __future__ import annotations

from pathlib import Path
from typing import Any

import pandas as pd
from fastapi import FastAPI, File, HTTPException, UploadFile
from fastapi.responses import FileResponse
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles

from app.config import AppSettingsPayload, Settings, get_settings, load_app_settings, save_app_settings
from app.repository import MetadataRepository
from app.schemas import (
    AppSettingsRequest,
    AppSettingsResponse,
    ApproveRequest,
    ApproveResponse,
    DatasetDetail,
    DatasetSummary,
    ProposalResponse,
    ProposalRevisionRequest,
    QueryHistoryEntry,
    QueryRequest,
    QueryResponse,
    SourceFileInfo,
)
from app.services.ingestion import (
    create_sqlite_table_from_dataframe,
    file_profile,
    get_sheet_count,
    load_dataframe_map,
    normalize_column_name,
    save_upload,
    slugify,
)
from app.services.proposals import ProposalGenerationError, generate_proposal
from app.services.querying import QueryGenerationError, generate_query, run_query, validate_select_sql


def create_app(settings: Settings | None = None) -> FastAPI:
    settings = settings or get_settings()
    repository = MetadataRepository(settings.sqlite_path)

    app = FastAPI(title="db-auto-pilot API", version="0.1.0")
    app.add_middleware(
        CORSMiddleware,
        allow_origins=settings.cors_origins or ["*"],
        allow_methods=["*"],
        allow_headers=["*"],
        allow_credentials=True,
    )
    app.state.settings = settings
    app.state.repository = repository

    frontend_dist_dir = settings.resolved_frontend_dist_dir
    if frontend_dist_dir.exists():
        assets_dir = frontend_dist_dir / "assets"
        if assets_dir.exists():
            app.mount("/assets", StaticFiles(directory=assets_dir), name="assets")

    @app.get("/health")
    def health() -> dict[str, str]:
        return {"status": "ok"}

    @app.get("/settings", response_model=AppSettingsResponse)
    def get_app_settings() -> dict[str, Any]:
        payload = load_app_settings(settings)
        return payload.model_dump()

    @app.put("/settings", response_model=AppSettingsResponse)
    def update_app_settings(request: AppSettingsRequest) -> dict[str, Any]:
        payload = AppSettingsPayload.model_validate(request.model_dump())
        saved = save_app_settings(settings, payload)
        return saved.model_dump()

    @app.get("/datasets", response_model=list[DatasetSummary])
    def list_datasets() -> list[dict[str, Any]]:
        return repository.list_datasets()

    @app.post("/datasets", response_model=DatasetDetail)
    async def create_dataset(files: list[UploadFile] = File(...)) -> dict[str, Any]:
        if not files:
            raise HTTPException(status_code=400, detail="At least one file is required.")
        dataset_name = Path(files[0].filename or "dataset").stem
        dataset = repository.create_dataset(dataset_name)
        dataset_dir = settings.uploads_dir / dataset["id"]
        source_items = []

        try:
            for upload in files:
                if not upload.filename:
                    continue
                suffix = Path(upload.filename).suffix.lower()
                if suffix not in {".xlsx", ".xls", ".csv"}:
                    raise HTTPException(status_code=400, detail=f"Unsupported file: {upload.filename}")
                destination = dataset_dir / upload.filename
                save_upload(upload, destination)
                sheet_count = get_sheet_count(destination)
                source_items.append(
                    repository.add_source_file(
                        dataset["id"],
                        upload.filename,
                        str(destination),
                        suffix.lstrip("."),
                        sheet_count,
                    )
                )
        finally:
            for upload in files:
                await upload.close()

        return {
            "dataset": repository.get_dataset(dataset["id"]),
            "source_files": source_items,
            "latest_proposal": None,
            "tables": [],
            "approval_decisions": [],
            "column_lineage": [],
        }

    @app.get("/datasets/{dataset_id}", response_model=DatasetDetail)
    def get_dataset(dataset_id: str) -> dict[str, Any]:
        dataset = repository.get_dataset(dataset_id)
        if not dataset:
            raise HTTPException(status_code=404, detail="Dataset not found.")
        return {
            "dataset": dataset,
            "source_files": repository.list_source_files(dataset_id),
            "latest_proposal": repository.get_latest_proposal(dataset_id),
            "tables": repository.list_dataset_tables(dataset_id),
            "approval_decisions": repository.list_approval_decisions(dataset_id),
            "column_lineage": repository.list_column_lineage(dataset_id),
        }

    @app.post("/datasets/{dataset_id}/proposal", response_model=ProposalResponse)
    def create_proposal(dataset_id: str) -> dict[str, Any]:
        dataset = repository.get_dataset(dataset_id)
        if not dataset:
            raise HTTPException(status_code=404, detail="Dataset not found.")
        source_files = repository.list_source_files(dataset_id)
        raw_tables = []
        for source_file in source_files:
            raw_tables.extend(file_profile(dataset_id, source_file))
        try:
            proposal = generate_proposal(settings, dataset["name"], raw_tables)
        except ProposalGenerationError as exc:
            raise HTTPException(status_code=502, detail=str(exc)) from exc
        record = repository.create_proposal(dataset_id, proposal)
        repository.update_dataset_status(dataset_id, "awaiting_approval")
        return record

    @app.post("/datasets/{dataset_id}/proposal/revise", response_model=ProposalResponse)
    def revise_dataset_proposal(dataset_id: str, request: ProposalRevisionRequest) -> dict[str, Any]:
        dataset = repository.get_dataset(dataset_id)
        if not dataset:
            raise HTTPException(status_code=404, detail="Dataset not found.")
        source_files = repository.list_source_files(dataset_id)
        raw_tables = []
        for source_file in source_files:
            raw_tables.extend(file_profile(dataset_id, source_file))
        try:
            proposal = generate_proposal(settings, dataset["name"], raw_tables, feedback=request.feedback)
        except ProposalGenerationError as exc:
            raise HTTPException(status_code=502, detail=str(exc)) from exc
        record = repository.create_proposal(dataset_id, proposal, feedback=request.feedback)
        repository.update_dataset_status(dataset_id, "awaiting_approval")
        return record

    @app.post("/datasets/{dataset_id}/approve", response_model=ApproveResponse)
    def approve_proposal(dataset_id: str, request: ApproveRequest) -> dict[str, Any]:
        dataset = repository.get_dataset(dataset_id)
        if not dataset:
            raise HTTPException(status_code=404, detail="Dataset not found.")
        proposal_record = repository.get_proposal(request.approved_proposal_id)
        if not proposal_record or proposal_record["dataset_id"] != dataset_id:
            raise HTTPException(status_code=404, detail="Proposal not found.")
        source_files = repository.list_source_files(dataset_id)
        dataframe_map = load_dataframe_map(dataset_id, source_files)

        created_tables = []
        table_metadata: list[dict[str, Any]] = []
        lineage_items: list[dict[str, Any]] = []
        with repository.connect() as conn:
            for raw_table in proposal_record["proposal"]["raw_tables"]:
                df = dataframe_map[raw_table["table_name"]]
                create_sqlite_table_from_dataframe(conn, raw_table["table_name"], df)
                created_tables.append({"mode": "raw", "table_name": raw_table["table_name"]})
                table_metadata.append(
                    {
                        "mode": "raw",
                        "table_name": raw_table["table_name"],
                        "display_name": raw_table["display_name"],
                        "schema": {
                            "columns": [
                                {
                                    "name": column,
                                    "logical_type": "number"
                                    if pd.api.types.is_numeric_dtype(df[column])
                                    else "text",
                                }
                                for column in df.columns
                            ]
                        },
                    }
                )

            for merged_table in proposal_record["proposal"]["merged_tables"]:
                output_frames = []
                merged_column_defs = merged_table["columns"]
                for source_table_name in merged_table["source_tables"]:
                    source_df = dataframe_map.get(source_table_name)
                    if source_df is None:
                        continue
                    merged_df = pd.DataFrame(index=source_df.index)
                    for column_def in merged_column_defs:
                        canonical_name = normalize_column_name(column_def["name"])
                        merged_df[canonical_name] = None
                        candidates = [
                            source["source_column"]
                            for source in column_def["source_columns"]
                            if source["source_table"] == source_table_name
                        ]
                        if candidates:
                            merged_df[canonical_name] = source_df[candidates[0]]
                        lineage_items.append(
                            {
                                "table_name": merged_table["display_name"],
                                "column_name": canonical_name,
                                "source_columns": column_def["source_columns"],
                                "status": column_def.get("status", "merge"),
                            }
                        )
                    merged_df["_source_row_index"] = source_df["_row_index"]
                    merged_df["_source_sheet"] = source_df["_source_sheet"]
                    merged_df["_source_file"] = source_df["_source_file"]
                    merged_df["_source_table"] = source_table_name
                    output_frames.append(merged_df)
                if not output_frames:
                    continue
                combined = pd.concat(output_frames, ignore_index=True)
                physical_name = (
                    f'merged_{dataset_id.replace("-", "")[:8]}_{slugify(merged_table["display_name"])}'
                )[:55]
                create_sqlite_table_from_dataframe(conn, physical_name, combined)
                created_tables.append({"mode": "merged", "table_name": physical_name})
                table_metadata.append(
                    {
                        "mode": "merged",
                        "table_name": physical_name,
                        "display_name": merged_table["display_name"],
                        "schema": {
                            "columns": [
                                {
                                    "name": column,
                                    "logical_type": "number"
                                    if pd.api.types.is_numeric_dtype(combined[column])
                                    else "text",
                                }
                                for column in combined.columns
                            ],
                            "lineage": [
                                {
                                    "column_name": normalize_column_name(column_def["name"]),
                                    "source_columns": column_def["source_columns"],
                                    "status": column_def.get("status", "merge"),
                                }
                                for column_def in merged_column_defs
                            ],
                        },
                    }
                )

        repository.replace_dataset_tables(dataset_id, table_metadata)
        repository.replace_approval_decisions(
            dataset_id,
            request.approved_proposal_id,
            proposal_record["proposal"].get("user_decisions", []),
        )
        repository.replace_column_lineage(dataset_id, lineage_items)
        repository.clear_query_history(dataset_id)
        repository.update_dataset_status(dataset_id, "approved", request.approved_proposal_id)
        return {
            "dataset_id": dataset_id,
            "approved_proposal_id": request.approved_proposal_id,
            "created_tables": created_tables,
        }

    @app.post("/datasets/{dataset_id}/query", response_model=QueryResponse)
    def query_dataset(dataset_id: str, request: QueryRequest) -> dict[str, Any]:
        dataset = repository.get_dataset(dataset_id)
        if not dataset:
            raise HTTPException(status_code=404, detail="Dataset not found.")
        if dataset["status"] != "approved":
            raise HTTPException(status_code=400, detail="Approve a proposal before querying.")
        tables = repository.list_dataset_tables(dataset_id, request.target_mode)
        if not tables:
            raise HTTPException(status_code=400, detail=f"No {request.target_mode} tables available.")
        try:
            generated = generate_query(settings, request.question, tables)
        except QueryGenerationError as exc:
            raise HTTPException(status_code=502, detail=str(exc)) from exc
        sql = generated["sql"]
        explanation = generated["explanation"]
        try:
            sql = validate_select_sql(sql)
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        with repository.connect() as conn:
            try:
                columns, rows = run_query(conn, sql)
            except Exception as exc:  # pragma: no cover
                raise HTTPException(status_code=400, detail=f"Query failed: {exc}") from exc
        repository.add_query_history(
            dataset_id,
            request.target_mode,
            request.question,
            sql,
            explanation,
            {"columns": columns, "rows": rows},
        )
        return {
            "sql": sql,
            "explanation": explanation,
            "generator": generated["generator"],
            "warning": generated["warning"],
            "columns": columns,
            "rows": rows,
        }

    @app.get("/datasets/{dataset_id}/query-history", response_model=list[QueryHistoryEntry])
    def get_query_history(dataset_id: str) -> list[dict[str, Any]]:
        dataset = repository.get_dataset(dataset_id)
        if not dataset:
            raise HTTPException(status_code=404, detail="Dataset not found.")
        return repository.list_query_history(dataset_id)

    if frontend_dist_dir.exists():
        @app.get("/")
        def serve_index() -> FileResponse:
            return FileResponse(frontend_dist_dir / "index.html")

        @app.get("/{full_path:path}")
        def serve_spa(full_path: str) -> FileResponse:
            requested = frontend_dist_dir / full_path
            if requested.is_file():
                return FileResponse(requested)
            return FileResponse(frontend_dist_dir / "index.html")

    return app


app = create_app()
