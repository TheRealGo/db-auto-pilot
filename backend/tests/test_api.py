from __future__ import annotations

import io
from pathlib import Path

import pandas as pd
from fastapi.testclient import TestClient

import app.main as main_module
from app.config import get_settings
from app.main import create_app
from app.services.proposals import make_json_safe


def stub_materialization_code() -> str:
    return """
import pandas as pd

merged_tables = []
lineage_items = []
execution_notes = ["stubbed materialization"]
for component in PLAN["components"]:
    frames = []
    for source_table in component["source_tables"]:
        source_df = source_frames[source_table]
        output_df = pd.DataFrame(index=source_df.index)
        for column in component["columns"]:
            output_df[column["name"]] = None
            matching_source = next(
                (source for source in column["source_columns"] if source["source_table"] == source_table),
                None,
            )
            if matching_source:
                output_df[column["name"]] = source_df[matching_source["source_column"]]
            lineage_items.append(
                {
                    "table_name": component["display_name"],
                    "column_name": column["name"],
                    "source_columns": column["source_columns"],
                    "status": column["status"],
                }
            )
        output_df["_source_row_index"] = source_df["_row_index"]
        output_df["_source_sheet"] = source_df["_source_sheet"]
        output_df["_source_file"] = source_df["_source_file"]
        output_df["_source_table"] = source_table
        frames.append(output_df)
    merged_df = pd.concat(frames, ignore_index=True)
    merged_tables.append(
        {
            "component_id": component["component_id"],
            "display_name": component["display_name"],
            "physical_name": component["physical_name"],
            "dataframe": merged_df,
        }
    )
result = {
    "merged_tables": merged_tables,
    "lineage_items": lineage_items,
    "execution_notes": execution_notes,
}
    """.strip()


def dangerous_materialization_code() -> str:
    return """
import os
result = {
    "merged_tables": [],
    "lineage_items": [],
    "execution_notes": [],
}
    """.strip()


def stub_materialization_proposal(plan: dict[str, object], retry_context: dict[str, object] | None = None) -> dict[str, object]:
    return {
        "summary": "stubbed materialization proposal",
        "normalization_decisions": [
            {
                "component_id": component["component_id"],
                "column_name": column["name"],
                "actions": ["trim_whitespace"],
                "config": {},
                "reason": "normalize text inputs",
            }
            for component in plan["components"]
            for column in component["columns"]
        ],
        "transformation_notes": ["align source columns into merged components"],
        "risk_notes": ["review merged columns before approval"],
        "expected_outputs": [component["display_name"] for component in plan["components"]],
        "quality_expectations": ["numeric/date parsing should remain stable after normalization"],
        "generated_code": stub_materialization_code(),
        "plan": plan,
        "retry_context": retry_context,
    }


def dangerous_materialization_proposal(plan: dict[str, object], retry_context: dict[str, object] | None = None) -> dict[str, object]:
    return {
        "summary": "dangerous materialization proposal",
        "normalization_decisions": [],
        "transformation_notes": [],
        "risk_notes": ["contains banned import"],
        "expected_outputs": [],
        "quality_expectations": [],
        "generated_code": dangerous_materialization_code(),
        "plan": plan,
        "retry_context": retry_context,
    }


def stub_proposal(
    raw_tables: list[dict[str, object]],
    feedback: str | None = None,
    prior_proposal: dict[str, object] | None = None,
) -> dict[str, object]:
    note = "stubbed llm proposal"
    if feedback:
        note = f"{note}: {feedback}"
    first_table = raw_tables[0]
    first_column = first_table["columns"][0]
    return {
        "dataset_name": "stubbed",
        "summary": note,
        "canonical_proposal": {
            "overview": {
                "summary": note,
                "feedback_applied": bool(feedback),
                "source_table_count": len(raw_tables),
                "merged_component_count": 1,
                "review_item_count": 1 if feedback else 0,
                "blocking_review_count": 1 if feedback else 0,
                "question_count": 0,
            },
            "approval_checklist": [],
            "candidates": [],
            "prior_summary": prior_proposal.get("summary") if prior_proposal else None,
        },
        "raw_tables": raw_tables,
        "observations": [],
        "comparison_candidates": [],
        "decisions": [],
        "questions_for_user": [],
        "normalization_plan": [
            {
                "source_table": table["table_name"],
                "source_column": column["db_name"],
                "actions": ["trim_whitespace"],
                "reason": "normalize text inputs",
            }
            for table in raw_tables
            for column in table["columns"]
        ],
        "merged_tables": [
            {
                "table_name": "merged_1",
                "display_name": "Merged Table 1",
                "source_tables": [table["table_name"] for table in raw_tables],
                "columns": [
                    {
                        "name": first_column["db_name"],
                        "logical_type": first_column["logical_type"],
                        "source_columns": [
                            {
                                "source_table": table["table_name"],
                                "source_column": table["columns"][0]["db_name"],
                                "display_name": table["columns"][0]["original_name"],
                            }
                            for table in raw_tables
                        ],
                        "notes": note,
                        "status": "merge",
                    }
                ],
            }
        ],
        "schema_draft": [
            {
                "component_id": "component_1",
                "display_name": "Merged Table 1",
                "source_tables": [table["table_name"] for table in raw_tables],
                "columns": [
                    {
                        "name": first_column["db_name"],
                        "logical_type": first_column["logical_type"],
                        "status": "merge",
                        "source_count": len(raw_tables),
                        "rationale": note,
                    }
                ],
            }
        ],
        "column_mappings": [
            {
                "component_id": "component_1",
                "canonical_name": first_column["db_name"],
                "logical_type": first_column["logical_type"],
                "matches": [
                    {
                        "source_table": table["table_name"],
                        "source_column": table["columns"][0]["db_name"],
                        "display_name": table["columns"][0]["original_name"],
                    }
                    for table in raw_tables
                ],
                "rationale": note,
                "confidence": 0.95,
                "merge_recommended": True,
                "decision": "merge",
                "review_status": "ready",
                "override_applied": bool(feedback),
                "evidence_summary": {
                    "candidate_score": 0.95,
                    "signal": "merge",
                    "value_overlap": 1.0,
                    "reasons": [note],
                },
            }
        ],
        "review_items": (
            [
                {
                    "type": "keep_separate",
                    "severity": "blocking",
                    "canonical_name": first_column["db_name"],
                    "component_id": "component_1",
                    "message": feedback,
                    "override_applied": True,
                    "evidence_summary": {
                        "candidate_score": 0.95,
                        "signal": "review",
                        "value_overlap": 1.0,
                        "reasons": [feedback],
                    },
                    "matches": [
                        {
                            "source_table": table["table_name"],
                            "source_column": table["columns"][0]["db_name"],
                            "display_name": table["columns"][0]["original_name"],
                        }
                        for table in raw_tables
                    ],
                }
            ]
            if feedback
            else []
        ),
        "feedback_overrides": {
            "raw_feedback": feedback,
            "overrides": (
                [
                    {
                        "candidate_id": "cand_001",
                        "type": "keep_separate",
                        "reason": feedback,
                    }
                ]
                if feedback
                else []
            ),
        },
        "normalization_actions": [
            {
                "table_name": table["table_name"],
                "display_name": table["display_name"],
                "source_column": column["original_name"],
                "normalized_column": column["db_name"],
                "actions": ["trim_whitespace", "normalize_column_name"],
            }
            for table in raw_tables
            for column in table["columns"]
        ],
        "normalization_rules": [
            "trim whitespace from text cells",
            "normalize column names to snake_case for DB columns",
        ],
        "notes": [note],
        "user_decisions": (
            [
                {
                    "canonical_name": first_column["db_name"],
                    "source_columns": [
                        {
                            "source_table": table["table_name"],
                            "source_column": table["columns"][0]["db_name"],
                        }
                        for table in raw_tables
                    ],
                    "decision": "keep_separate",
                    "reason": feedback,
                }
            ]
            if feedback
            else []
        ),
        "table_component_map": {table["table_name"]: "component_1" for table in raw_tables},
        "materialization_plan_draft": {
            "components": [
                {
                    "component_id": "component_1",
                    "display_name": "Merged Table 1",
                    "source_tables": [table["table_name"] for table in raw_tables],
                    "columns": [
                        {
                            "name": first_column["db_name"],
                            "logical_type": first_column["logical_type"],
                            "status": "merge",
                            "reason": note,
                            "source_columns": [
                                {
                                    "source_table": table["table_name"],
                                    "source_column": table["columns"][0]["db_name"],
                                    "display_name": table["columns"][0]["original_name"],
                                    "actions": ["trim_whitespace"],
                                }
                                for table in raw_tables
                            ],
                        }
                    ],
                }
            ]
        },
    }


def test_dataset_flow(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setenv("DB_AUTO_PILOT_DATA_DIR", str(tmp_path / "data"))
    get_settings.cache_clear()
    app = create_app(get_settings())
    client = TestClient(app)
    monkeypatch.setattr(
        main_module,
        "generate_proposal",
        lambda settings, dataset_name, raw_tables, dataframe_map, feedback=None, prior_proposal=None: stub_proposal(raw_tables, feedback, prior_proposal),
    )
    monkeypatch.setattr(
        main_module,
        "generate_materialization_proposal",
        lambda settings, dataset_name, plan, retry_context=None: stub_materialization_proposal(plan, retry_context),
    )

    sales_csv = io.BytesIO("customer_name,amount\nAlice,120\nBob,80\n".encode("utf-8"))
    jp_csv = io.BytesIO("顧客名,売上\nAlice,120\nBob,80\n".encode("utf-8"))

    response = client.post(
        "/datasets",
        files=[
            ("files", ("sales.csv", sales_csv, "text/csv")),
            ("files", ("sales_jp.csv", jp_csv, "text/csv")),
        ],
    )
    assert response.status_code == 200
    dataset = response.json()
    dataset_id = dataset["dataset"]["id"]

    proposal = client.post(f"/datasets/{dataset_id}/proposal")
    assert proposal.status_code == 200
    proposal_payload = proposal.json()
    proposal_id = proposal_payload["id"]
    assert proposal_payload["proposal"]["schema_draft"]
    assert proposal_payload["proposal"]["normalization_actions"]

    approval = client.post(
        f"/datasets/{dataset_id}/approve",
        json={"approved_proposal_id": proposal_id},
    )
    assert approval.status_code == 200
    assert approval.json()["created_tables"] == []

    materialization = client.post(
        f"/datasets/{dataset_id}/materialization-proposal",
        json={"proposal_id": proposal_id},
    )
    assert materialization.status_code == 200
    materialization_payload = materialization.json()
    assert materialization_payload["materialization"]["normalization_decisions"]
    assert "quality_expectations" in materialization_payload["materialization"]

    materialization_approval = client.post(
        f"/datasets/{dataset_id}/materialization-proposal/{materialization_payload['id']}/approve",
        json={"approved_materialization_proposal_id": materialization_payload["id"]},
    )
    assert materialization_approval.status_code == 200
    assert len(materialization_approval.json()["created_tables"]) >= 2

    monkeypatch.setattr(
        main_module,
        "generate_query",
        lambda settings, question, tables: {
            "sql": f'SELECT * FROM "{tables[0]["table_name"]}" LIMIT 10',
            "explanation": "stubbed llm query",
            "generator": "openai",
            "warning": None,
        },
    )

    query = client.post(
        f"/datasets/{dataset_id}/query",
        json={"target_mode": "raw", "question": "すべて見せて"},
    )
    assert query.status_code == 200
    payload = query.json()
    assert payload["columns"]
    assert payload["rows"]
    assert "LIMIT" in payload["sql"]
    assert payload["generator"] == "openai"

    detail = client.get(f"/datasets/{dataset_id}")
    assert detail.status_code == 200
    detail_payload = detail.json()
    assert any(table["mode"] == "merged" for table in detail_payload["tables"])
    merged_table = next(table for table in detail_payload["tables"] if table["mode"] == "merged")
    merged_columns = {column["name"] for column in merged_table["schema"]["columns"]}
    assert {"_source_file", "_source_sheet", "_source_row_index", "_source_table"} <= merged_columns
    assert detail_payload["column_lineage"]
    assert detail_payload["latest_materialization_proposal"]
    assert detail_payload["materialization_proposals"]
    assert detail_payload["materialization_runs"]
    assert detail_payload["materialization_runs"][0]["status"] == "completed"
    assert detail_payload["materialization_runs"][0]["result"]["resource_summary"]["merged_table_count"] >= 1
    assert detail_payload["materialization_runs"][0]["result"]["repair_summary"]["status"] in {"unchanged", "repaired"}
    assert detail_payload["materialization_runs"][0]["result"]["quality_summary"]["status"] == "completed"


def test_revision_persists_user_decision(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setenv("DB_AUTO_PILOT_DATA_DIR", str(tmp_path / "data"))
    get_settings.cache_clear()
    app = create_app(get_settings())
    client = TestClient(app)
    monkeypatch.setattr(
        main_module,
        "generate_proposal",
        lambda settings, dataset_name, raw_tables, dataframe_map, feedback=None, prior_proposal=None: stub_proposal(raw_tables, feedback, prior_proposal),
    )
    monkeypatch.setattr(
        main_module,
        "generate_materialization_proposal",
        lambda settings, dataset_name, plan, retry_context=None: stub_materialization_proposal(plan, retry_context),
    )

    first = io.BytesIO("顧客名,売上\nAlice,120\n".encode("utf-8"))
    second = io.BytesIO("customer_name,amount\nAlice,120\n".encode("utf-8"))

    create = client.post(
        "/datasets",
        files=[
            ("files", ("jp.csv", first, "text/csv")),
            ("files", ("en.csv", second, "text/csv")),
        ],
    )
    dataset_id = create.json()["dataset"]["id"]

    proposal = client.post(f"/datasets/{dataset_id}/proposal")
    assert proposal.status_code == 200

    revise = client.post(
        f"/datasets/{dataset_id}/proposal/revise",
        json={"feedback": "customer_name と 顧客名 は統合しないでください"},
    )
    assert revise.status_code == 200
    revised_payload = revise.json()["proposal"]
    assert revised_payload["canonical_proposal"]["overview"]["feedback_applied"] is True
    assert revised_payload["canonical_proposal"]["overview"]["blocking_review_count"] == 1
    assert revised_payload["user_decisions"]
    assert any(item["type"] == "keep_separate" for item in revised_payload["review_items"])
    assert any(item["severity"] == "blocking" for item in revised_payload["review_items"])

    approval = client.post(
        f"/datasets/{dataset_id}/approve",
        json={"approved_proposal_id": revise.json()["id"]},
    )
    assert approval.status_code == 200

    materialization = client.post(
        f"/datasets/{dataset_id}/materialization-proposal",
        json={"proposal_id": revise.json()["id"]},
    )
    assert materialization.status_code == 200

    detail = client.get(f"/datasets/{dataset_id}")
    assert detail.status_code == 200
    assert len(detail.json()["proposals"]) == 2
    decisions = detail.json()["approval_decisions"]
    assert decisions
    assert any(decision["decision"] == "keep_separate" for decision in decisions)


def test_query_requires_llm(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setenv("DB_AUTO_PILOT_DATA_DIR", str(tmp_path / "data"))
    monkeypatch.delenv("OPENAI_API_KEY", raising=False)
    monkeypatch.delenv("OPENAI_ENDPOINT", raising=False)
    monkeypatch.delenv("OPENAI_MODEL", raising=False)
    get_settings.cache_clear()
    app = create_app(get_settings())
    client = TestClient(app)
    monkeypatch.setattr(
        main_module,
        "generate_proposal",
        lambda settings, dataset_name, raw_tables, dataframe_map, feedback=None, prior_proposal=None: stub_proposal(raw_tables, feedback, prior_proposal),
    )
    monkeypatch.setattr(
        main_module,
        "generate_materialization_proposal",
        lambda settings, dataset_name, plan, retry_context=None: stub_materialization_proposal(plan, retry_context),
    )

    csv = io.BytesIO("department,amount\nSales,120\nSales,80\nHR,50\n".encode("utf-8"))
    create = client.post("/datasets", files=[("files", ("sales.csv", csv, "text/csv"))])
    dataset_id = create.json()["dataset"]["id"]
    proposal = client.post(f"/datasets/{dataset_id}/proposal").json()
    approval = client.post(
        f"/datasets/{dataset_id}/approve",
        json={"approved_proposal_id": proposal["id"]},
    )
    assert approval.status_code == 200

    materialization = client.post(
        f"/datasets/{dataset_id}/materialization-proposal",
        json={"proposal_id": proposal["id"]},
    )
    assert materialization.status_code == 200
    materialization_approval = client.post(
        f"/datasets/{dataset_id}/materialization-proposal/{materialization.json()['id']}/approve",
        json={"approved_materialization_proposal_id": materialization.json()["id"]},
    )
    assert materialization_approval.status_code == 200

    query = client.post(
        f"/datasets/{dataset_id}/query",
        json={"target_mode": "raw", "question": "件数を教えて"},
    )
    assert query.status_code == 502
    assert query.json()["detail"] == "OpenAI API key is not configured."


def test_proposal_requires_llm(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setenv("DB_AUTO_PILOT_DATA_DIR", str(tmp_path / "data"))
    monkeypatch.delenv("OPENAI_API_KEY", raising=False)
    monkeypatch.delenv("OPENAI_ENDPOINT", raising=False)
    monkeypatch.delenv("OPENAI_MODEL", raising=False)
    get_settings.cache_clear()
    app = create_app(get_settings())
    client = TestClient(app)

    csv = io.BytesIO("department,amount\nSales,120\n".encode("utf-8"))
    create = client.post("/datasets", files=[("files", ("sales.csv", csv, "text/csv"))])
    dataset_id = create.json()["dataset"]["id"]

    proposal = client.post(f"/datasets/{dataset_id}/proposal")
    assert proposal.status_code == 502
    assert proposal.json()["detail"] == "OpenAI API key is not configured."


def test_app_settings_roundtrip(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setenv("DB_AUTO_PILOT_DATA_DIR", str(tmp_path / "data"))
    get_settings.cache_clear()
    app = create_app(get_settings())
    client = TestClient(app)

    initial = client.get("/settings")
    assert initial.status_code == 200
    assert initial.json() == {"api_key": None, "endpoint": None, "model": None}

    update = client.put(
        "/settings",
        json={
            "api_key": "test-key",
            "endpoint": "https://example.invalid/v1",
            "model": "gpt-test",
        },
    )
    assert update.status_code == 200
    assert update.json() == {
        "api_key": "test-key",
        "endpoint": "https://example.invalid/v1",
        "model": "gpt-test",
    }

    loaded = client.get("/settings")
    assert loaded.status_code == 200
    assert loaded.json()["api_key"] == "test-key"
    assert loaded.json()["endpoint"] == "https://example.invalid/v1"
    assert loaded.json()["model"] == "gpt-test"


def test_make_json_safe_handles_timestamps() -> None:
    payload = {
        "timestamp": pd.Timestamp("2024-01-02T03:04:05"),
        "items": [pd.Timestamp("2024-05-06"), {"nested": pd.Timestamp("2024-07-08")}],
    }

    converted = make_json_safe(payload)

    assert converted["timestamp"] == "2024-01-02T03:04:05"
    assert converted["items"][0] == "2024-05-06T00:00:00"
    assert converted["items"][1]["nested"] == "2024-07-08T00:00:00"


def test_generate_proposal_accepts_excel_datetime_columns(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setenv("DB_AUTO_PILOT_DATA_DIR", str(tmp_path / "data"))
    get_settings.cache_clear()
    app = create_app(get_settings())
    client = TestClient(app)
    monkeypatch.setattr(
        main_module,
        "generate_proposal",
        lambda settings, dataset_name, raw_tables, dataframe_map, feedback=None, prior_proposal=None: stub_proposal(raw_tables, feedback, prior_proposal),
    )

    dataframe = pd.DataFrame(
        {
            "event_date": [pd.Timestamp("2024-01-02"), pd.Timestamp("2024-01-03")],
            "amount": [100, 150],
        }
    )
    workbook = io.BytesIO()
    with pd.ExcelWriter(workbook, engine="openpyxl") as writer:
        dataframe.to_excel(writer, index=False, sheet_name="Sheet1")
    workbook.seek(0)

    create = client.post(
        "/datasets",
        files=[("files", ("dated.xlsx", workbook, "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"))],
    )
    assert create.status_code == 200
    dataset_id = create.json()["dataset"]["id"]

    proposal = client.post(f"/datasets/{dataset_id}/proposal")
    assert proposal.status_code == 200
    payload = proposal.json()
    assert payload["proposal"]["raw_tables"][0]["columns"][0]["logical_type"] == "datetime"


def test_approval_persists_failed_materialization_run(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setenv("DB_AUTO_PILOT_DATA_DIR", str(tmp_path / "data"))
    get_settings.cache_clear()
    app = create_app(get_settings())
    client = TestClient(app)
    monkeypatch.setattr(
        main_module,
        "generate_proposal",
        lambda settings, dataset_name, raw_tables, dataframe_map, feedback=None, prior_proposal=None: stub_proposal(raw_tables, feedback, prior_proposal),
    )
    monkeypatch.setattr(
        main_module,
        "generate_materialization_proposal",
        lambda settings, dataset_name, plan, retry_context=None: dangerous_materialization_proposal(plan, retry_context),
    )

    csv = io.BytesIO("customer_name,amount\nAlice,120\n".encode("utf-8"))
    create = client.post("/datasets", files=[("files", ("sales.csv", csv, "text/csv"))])
    dataset_id = create.json()["dataset"]["id"]
    proposal = client.post(f"/datasets/{dataset_id}/proposal").json()

    approval = client.post(
        f"/datasets/{dataset_id}/approve",
        json={"approved_proposal_id": proposal["id"]},
    )
    assert approval.status_code == 200

    materialization = client.post(
        f"/datasets/{dataset_id}/materialization-proposal",
        json={"proposal_id": proposal["id"]},
    )
    assert materialization.status_code == 200

    materialization_approval = client.post(
        f"/datasets/{dataset_id}/materialization-proposal/{materialization.json()['id']}/approve",
        json={"approved_materialization_proposal_id": materialization.json()["id"]},
    )
    assert materialization_approval.status_code == 502
    assert "violated safety rules" in materialization_approval.json()["detail"]

    retry = client.post(
        f"/datasets/{dataset_id}/materialization-proposal/{materialization.json()['id']}/retry",
    )
    assert retry.status_code == 200
    retry_context = retry.json()["materialization"]["retry_context"]
    assert retry_context
    assert retry_context["reason"] == "failed_run_retry"
    assert retry_context["previous_error_stage"] == "guard"
    assert retry_context["guard_violations"]
    assert retry_context["focus_points"]

    detail = client.get(f"/datasets/{dataset_id}")
    assert detail.status_code == 200
    runs = detail.json()["materialization_runs"]
    assert runs
    assert runs[0]["status"] == "failed"
    assert retry.json()["source_run_id"] == runs[0]["id"]
    assert retry_context["previous_run_id"] == runs[0]["id"]
    assert runs[0]["result"]["error_stage"] == "guard"
    assert runs[0]["result"]["guard_summary"]["violations"]
    assert retry_context["column_patches"] == []
    assert not detail.json()["tables"]
