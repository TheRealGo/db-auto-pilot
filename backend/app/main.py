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
    MaterializationProposalApproveRequest,
    MaterializationProposalRequest,
    MaterializationProposalResponse,
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
    infer_logical_type,
    load_dataframe_map,
    save_upload,
)
from app.services.materialization import (
    MaterializationError,
    MaterializationExecutionError,
    MaterializationGuardError,
    MaterializationTimeoutError,
    MaterializationTransportError,
    build_materialization_retry_guidance,
    draft_materialization_plan,
    execute_materialization_code,
    generate_materialization_proposal,
    validate_generated_code,
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
            "proposals": [],
            "latest_materialization_proposal": None,
            "tables": [],
            "approval_decisions": [],
            "column_lineage": [],
            "materialization_proposals": [],
            "materialization_runs": [],
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
            "proposals": repository.list_proposals(dataset_id),
            "latest_materialization_proposal": repository.get_latest_materialization_proposal(dataset_id),
            "tables": repository.list_dataset_tables(dataset_id),
            "approval_decisions": repository.list_approval_decisions(dataset_id),
            "column_lineage": repository.list_column_lineage(dataset_id),
            "materialization_proposals": repository.list_materialization_proposals(dataset_id),
            "materialization_runs": repository.list_materialization_runs(dataset_id),
        }

    @app.post("/datasets/{dataset_id}/proposal", response_model=ProposalResponse)
    def create_proposal(dataset_id: str) -> dict[str, Any]:
        dataset = repository.get_dataset(dataset_id)
        if not dataset:
            raise HTTPException(status_code=404, detail="Dataset not found.")
        source_files = repository.list_source_files(dataset_id)
        prior_proposal = repository.get_latest_proposal(dataset_id)
        raw_tables = []
        for source_file in source_files:
            raw_tables.extend(file_profile(dataset_id, source_file))
        dataframe_map = load_dataframe_map(dataset_id, source_files)
        try:
            proposal = generate_proposal(settings, dataset["name"], raw_tables, dataframe_map)
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
        prior_proposal = repository.get_latest_proposal(dataset_id)
        raw_tables = []
        for source_file in source_files:
            raw_tables.extend(file_profile(dataset_id, source_file))
        dataframe_map = load_dataframe_map(dataset_id, source_files)
        try:
            proposal = generate_proposal(
                settings,
                dataset["name"],
                raw_tables,
                dataframe_map,
                feedback=request.feedback,
                prior_proposal=prior_proposal["proposal"] if prior_proposal else None,
            )
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
        repository.replace_approval_decisions(
            dataset_id,
            request.approved_proposal_id,
            proposal_record["proposal"].get("user_decisions", []),
        )
        repository.update_dataset_status(dataset_id, "awaiting_materialization_approval", request.approved_proposal_id)
        return {
            "dataset_id": dataset_id,
            "approved_proposal_id": request.approved_proposal_id,
            "created_tables": [],
        }

    @app.post("/datasets/{dataset_id}/materialization-proposal", response_model=MaterializationProposalResponse)
    def create_materialization_proposal(
        dataset_id: str,
        request: MaterializationProposalRequest,
    ) -> dict[str, Any]:
        dataset = repository.get_dataset(dataset_id)
        if not dataset:
            raise HTTPException(status_code=404, detail="Dataset not found.")
        proposal_id = request.proposal_id or dataset.get("approved_proposal_id")
        if not proposal_id:
            raise HTTPException(status_code=400, detail="Approve a proposal before creating a materialization proposal.")
        proposal_record = repository.get_proposal(proposal_id)
        if not proposal_record or proposal_record["dataset_id"] != dataset_id:
            raise HTTPException(status_code=404, detail="Proposal not found.")
        proposal_payload = proposal_record["proposal"]
        materialization_plan = draft_materialization_plan(proposal_payload, dataset_id)
        try:
            materialization = generate_materialization_proposal(
                settings,
                dataset["name"],
                materialization_plan,
            )
        except MaterializationError as exc:
            raise HTTPException(status_code=502, detail=str(exc)) from exc
        record = repository.create_materialization_proposal(dataset_id, proposal_id, materialization)
        repository.update_dataset_status(dataset_id, "awaiting_materialization_approval", proposal_id)
        return record

    @app.post(
        "/datasets/{dataset_id}/materialization-proposal/{materialization_proposal_id}/retry",
        response_model=MaterializationProposalResponse,
    )
    def retry_materialization_proposal(
        dataset_id: str,
        materialization_proposal_id: str,
    ) -> dict[str, Any]:
        dataset = repository.get_dataset(dataset_id)
        if not dataset:
            raise HTTPException(status_code=404, detail="Dataset not found.")
        materialization_proposal = repository.get_materialization_proposal(materialization_proposal_id)
        if not materialization_proposal or materialization_proposal["dataset_id"] != dataset_id:
            raise HTTPException(status_code=404, detail="Materialization proposal not found.")
        runs = repository.list_materialization_runs(dataset_id)
        source_run = next(
            (
                run
                for run in runs
                if run["proposal_id"] == materialization_proposal["proposal_id"] and run["status"] == "failed"
            ),
            None,
        )
        proposal_record = repository.get_proposal(materialization_proposal["proposal_id"])
        proposal_payload = proposal_record["proposal"] if proposal_record else None
        retry_context = build_materialization_retry_guidance(
            source_run["result"] if source_run else None,
            proposal_payload,
            previous_run_id=source_run["id"] if source_run else None,
        )
        try:
            materialization = generate_materialization_proposal(
                settings,
                dataset["name"],
                materialization_proposal["materialization"]["plan"],
                retry_context=retry_context,
            )
        except MaterializationError as exc:
            raise HTTPException(status_code=502, detail=str(exc)) from exc
        record = repository.create_materialization_proposal(
            dataset_id,
            materialization_proposal["proposal_id"],
            materialization,
            source_run_id=source_run["id"] if source_run else None,
        )
        repository.update_dataset_status(
            dataset_id,
            "awaiting_materialization_approval",
            materialization_proposal["proposal_id"],
        )
        return record

    @app.post(
        "/datasets/{dataset_id}/materialization-proposal/{materialization_proposal_id}/approve",
        response_model=ApproveResponse,
    )
    def approve_materialization_proposal(
        dataset_id: str,
        materialization_proposal_id: str,
        request: MaterializationProposalApproveRequest,
    ) -> dict[str, Any]:
        dataset = repository.get_dataset(dataset_id)
        if not dataset:
            raise HTTPException(status_code=404, detail="Dataset not found.")
        materialization_record = repository.get_materialization_proposal(materialization_proposal_id)
        if not materialization_record or materialization_record["dataset_id"] != dataset_id:
            raise HTTPException(status_code=404, detail="Materialization proposal not found.")
        if request.approved_materialization_proposal_id != materialization_proposal_id:
            raise HTTPException(status_code=400, detail="Mismatched materialization proposal approval request.")
        proposal_record = repository.get_proposal(materialization_record["proposal_id"])
        if not proposal_record:
            raise HTTPException(status_code=404, detail="Proposal not found.")
        source_files = repository.list_source_files(dataset_id)
        dataframe_map = load_dataframe_map(dataset_id, source_files)
        proposal_payload = proposal_record["proposal"]
        materialization_payload = materialization_record["materialization"]
        generated_code = materialization_payload["generated_code"]
        generation_summary = materialization_payload["summary"]
        guard_summary = None
        try:
            guard_summary = validate_generated_code(generated_code)
            materialization_result = execute_materialization_code(
                generated_code,
                dataframe_map,
                materialization_payload["plan"],
                guard_summary,
                materialization_payload.get("normalization_decisions"),
            )
        except MaterializationGuardError as exc:
            repository.create_materialization_run(
                dataset_id,
                materialization_record["proposal_id"],
                "failed",
                generated_code,
                {
                    "plan": materialization_payload["plan"],
                    "generation_summary": generation_summary,
                    "guard_summary": exc.summary or guard_summary,
                    "error_stage": "guard",
                    "error": str(exc),
                },
            )
            raise HTTPException(status_code=502, detail=str(exc)) from exc
        except MaterializationExecutionError as exc:
            repository.create_materialization_run(
                dataset_id,
                materialization_record["proposal_id"],
                "failed",
                generated_code,
                {
                    "plan": materialization_payload["plan"],
                    "generation_summary": generation_summary,
                    "guard_summary": guard_summary,
                    "error_stage": "result_validation" if "return" in str(exc) or "missing" in str(exc) or "too many" in str(exc) or "provenance" in str(exc) else "execution",
                    "error": str(exc),
                },
            )
            raise HTTPException(status_code=502, detail=str(exc)) from exc
        except MaterializationTimeoutError as exc:
            repository.create_materialization_run(
                dataset_id,
                materialization_record["proposal_id"],
                "failed",
                generated_code,
                {
                    "plan": materialization_payload["plan"],
                    "generation_summary": generation_summary,
                    "guard_summary": guard_summary,
                    "error_stage": "timeout",
                    "error": str(exc),
                },
            )
            raise HTTPException(status_code=502, detail=str(exc)) from exc
        except MaterializationTransportError as exc:
            repository.create_materialization_run(
                dataset_id,
                materialization_record["proposal_id"],
                "failed",
                generated_code,
                {
                    "plan": materialization_payload["plan"],
                    "generation_summary": generation_summary,
                    "guard_summary": guard_summary,
                    "error_stage": "transport",
                    "error": str(exc),
                },
            )
            raise HTTPException(status_code=502, detail=str(exc)) from exc
        except MaterializationError as exc:
            repository.create_materialization_run(
                dataset_id,
                materialization_record["proposal_id"],
                "failed",
                generated_code,
                {
                    "plan": materialization_payload["plan"],
                    "generation_summary": generation_summary,
                    "guard_summary": guard_summary,
                    "error_stage": "materialization",
                    "error": str(exc),
                },
            )
            raise HTTPException(status_code=502, detail=str(exc)) from exc

        created_tables = []
        table_metadata: list[dict[str, Any]] = []
        with repository.connect() as conn:
            for raw_table in proposal_payload["raw_tables"]:
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
                                    "logical_type": infer_logical_type(df[column]),
                                }
                                for column in df.columns
                            ]
                        },
                    }
                )

            for merged_table in materialization_result["merged_tables"]:
                combined = merged_table["dataframe"]
                physical_name = merged_table["physical_name"]
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
                                    "logical_type": infer_logical_type(combined[column]),
                                }
                                for column in combined.columns
                            ],
                            "lineage": [
                                item
                                for item in materialization_result["lineage_items"]
                                if item["table_name"] == merged_table["display_name"]
                            ],
                        },
                    }
                )

        repository.replace_dataset_tables(dataset_id, table_metadata)
        repository.replace_approval_decisions(
            dataset_id,
            materialization_record["proposal_id"],
            proposal_payload.get("user_decisions", []),
        )
        repository.replace_column_lineage(dataset_id, materialization_result["lineage_items"])
        repository.clear_query_history(dataset_id)
        repository.update_dataset_status(dataset_id, "approved", materialization_record["proposal_id"])
        repository.create_materialization_run(
            dataset_id,
            materialization_record["proposal_id"],
            "completed",
            generated_code,
            {
                "plan": materialization_payload["plan"],
                "generation_summary": generation_summary,
                "execution_notes": materialization_result.get("execution_notes", []),
                "guard_summary": materialization_result.get("guard_summary"),
                "repair_summary": materialization_result.get("repair_summary"),
                "resource_summary": materialization_result.get("resource_summary"),
                "quality_summary": materialization_result.get("quality_summary"),
                "warnings": materialization_result.get("warnings", []),
                "merged_tables": [
                    {
                        "component_id": item["component_id"],
                        "display_name": item["display_name"],
                        "physical_name": item["physical_name"],
                        "row_count": int(len(item["dataframe"].index)),
                    }
                    for item in materialization_result["merged_tables"]
                ],
            },
        )
        return {
            "dataset_id": dataset_id,
            "approved_proposal_id": materialization_record["proposal_id"],
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
