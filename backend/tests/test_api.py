from __future__ import annotations

import io
from pathlib import Path

import pandas as pd
from fastapi.testclient import TestClient

import app.main as main_module
from app.config import get_settings
from app.main import create_app
from app.services.proposals import make_json_safe


def stub_proposal(raw_tables: list[dict[str, object]], feedback: str | None = None) -> dict[str, object]:
    note = "stubbed llm proposal"
    if feedback:
        note = f"{note}: {feedback}"
    first_table = raw_tables[0]
    first_column = first_table["columns"][0]
    return {
        "dataset_name": "stubbed",
        "summary": note,
        "raw_tables": raw_tables,
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
            }
        ],
        "review_items": (
            [
                {
                    "type": "keep_separate",
                    "canonical_name": first_column["db_name"],
                    "component_id": "component_1",
                    "message": feedback,
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
    }


def test_dataset_flow(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setenv("DB_AUTO_PILOT_DATA_DIR", str(tmp_path / "data"))
    get_settings.cache_clear()
    app = create_app(get_settings())
    client = TestClient(app)
    monkeypatch.setattr(main_module, "generate_proposal", lambda settings, dataset_name, raw_tables, feedback=None: stub_proposal(raw_tables, feedback))

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
    assert len(approval.json()["created_tables"]) >= 2

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


def test_revision_persists_user_decision(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setenv("DB_AUTO_PILOT_DATA_DIR", str(tmp_path / "data"))
    get_settings.cache_clear()
    app = create_app(get_settings())
    client = TestClient(app)
    monkeypatch.setattr(main_module, "generate_proposal", lambda settings, dataset_name, raw_tables, feedback=None: stub_proposal(raw_tables, feedback))

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
    assert revised_payload["user_decisions"]
    assert any(item["type"] == "keep_separate" for item in revised_payload["review_items"])

    approval = client.post(
        f"/datasets/{dataset_id}/approve",
        json={"approved_proposal_id": revise.json()["id"]},
    )
    assert approval.status_code == 200

    detail = client.get(f"/datasets/{dataset_id}")
    assert detail.status_code == 200
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
    monkeypatch.setattr(main_module, "generate_proposal", lambda settings, dataset_name, raw_tables, feedback=None: stub_proposal(raw_tables, feedback))

    csv = io.BytesIO("department,amount\nSales,120\nSales,80\nHR,50\n".encode("utf-8"))
    create = client.post("/datasets", files=[("files", ("sales.csv", csv, "text/csv"))])
    dataset_id = create.json()["dataset"]["id"]
    proposal = client.post(f"/datasets/{dataset_id}/proposal").json()
    approval = client.post(
        f"/datasets/{dataset_id}/approve",
        json={"approved_proposal_id": proposal["id"]},
    )
    assert approval.status_code == 200

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
    monkeypatch.setattr(main_module, "generate_proposal", lambda settings, dataset_name, raw_tables, feedback=None: stub_proposal(raw_tables, feedback))

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
