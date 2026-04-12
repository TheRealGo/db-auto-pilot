from __future__ import annotations

import io
from pathlib import Path

from fastapi.testclient import TestClient

from app.config import get_settings
from app.main import create_app


def test_dataset_flow(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setenv("DB_AUTO_PILOT_DATA_DIR", str(tmp_path / "data"))
    get_settings.cache_clear()
    app = create_app(get_settings())
    client = TestClient(app)

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

    query = client.post(
        f"/datasets/{dataset_id}/query",
        json={"target_mode": "raw", "question": "すべて見せて"},
    )
    assert query.status_code == 200
    payload = query.json()
    assert payload["columns"]
    assert payload["rows"]
    assert "LIMIT" in payload["sql"]

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


def test_query_count_fallback(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setenv("DB_AUTO_PILOT_DATA_DIR", str(tmp_path / "data"))
    get_settings.cache_clear()
    app = create_app(get_settings())
    client = TestClient(app)

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
    assert query.status_code == 200
    payload = query.json()
    assert "COUNT" in payload["sql"]
    assert payload["rows"][0][0] == 3
