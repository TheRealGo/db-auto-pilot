from __future__ import annotations

import json
import sqlite3
import uuid
from contextlib import contextmanager
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Iterator


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def json_compatible(value: Any) -> Any:
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, dict):
        return {str(key): json_compatible(item) for key, item in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [json_compatible(item) for item in value]
    if hasattr(value, "isoformat"):
        try:
            return value.isoformat()
        except Exception:
            pass
    if hasattr(value, "item"):
        try:
            return json_compatible(value.item())
        except Exception:
            pass
    return str(value)


class MetadataRepository:
    def __init__(self, db_path: Path):
        self.db_path = db_path
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self.initialize()

    @contextmanager
    def connect(self) -> Iterator[sqlite3.Connection]:
        conn = sqlite3.connect(self.db_path, check_same_thread=False)
        conn.row_factory = sqlite3.Row
        try:
            yield conn
            conn.commit()
        finally:
            conn.close()

    def initialize(self) -> None:
        with self.connect() as conn:
            conn.executescript(
                """
                PRAGMA foreign_keys = ON;

                CREATE TABLE IF NOT EXISTS datasets (
                    id TEXT PRIMARY KEY,
                    name TEXT NOT NULL,
                    status TEXT NOT NULL,
                    approved_proposal_id TEXT,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS source_files (
                    id TEXT PRIMARY KEY,
                    dataset_id TEXT NOT NULL,
                    filename TEXT NOT NULL,
                    stored_path TEXT NOT NULL,
                    file_type TEXT NOT NULL,
                    sheet_count INTEGER NOT NULL,
                    created_at TEXT NOT NULL,
                    FOREIGN KEY(dataset_id) REFERENCES datasets(id) ON DELETE CASCADE
                );

                CREATE TABLE IF NOT EXISTS proposals (
                    id TEXT PRIMARY KEY,
                    dataset_id TEXT NOT NULL,
                    version INTEGER NOT NULL,
                    status TEXT NOT NULL,
                    feedback TEXT,
                    proposal_json TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    FOREIGN KEY(dataset_id) REFERENCES datasets(id) ON DELETE CASCADE
                );

                CREATE TABLE IF NOT EXISTS dataset_tables (
                    id TEXT PRIMARY KEY,
                    dataset_id TEXT NOT NULL,
                    mode TEXT NOT NULL,
                    table_name TEXT NOT NULL,
                    display_name TEXT NOT NULL,
                    schema_json TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    FOREIGN KEY(dataset_id) REFERENCES datasets(id) ON DELETE CASCADE
                );

                CREATE TABLE IF NOT EXISTS query_history (
                    id TEXT PRIMARY KEY,
                    dataset_id TEXT NOT NULL,
                    target_mode TEXT NOT NULL,
                    question TEXT NOT NULL,
                    sql_text TEXT NOT NULL,
                    explanation TEXT NOT NULL,
                    result_json TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    FOREIGN KEY(dataset_id) REFERENCES datasets(id) ON DELETE CASCADE
                );

                CREATE TABLE IF NOT EXISTS approval_decisions (
                    id TEXT PRIMARY KEY,
                    dataset_id TEXT NOT NULL,
                    proposal_id TEXT NOT NULL,
                    canonical_name TEXT NOT NULL,
                    decision_json TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    FOREIGN KEY(dataset_id) REFERENCES datasets(id) ON DELETE CASCADE
                );

                CREATE TABLE IF NOT EXISTS merged_column_lineage (
                    id TEXT PRIMARY KEY,
                    dataset_id TEXT NOT NULL,
                    table_name TEXT NOT NULL,
                    column_name TEXT NOT NULL,
                    lineage_json TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    FOREIGN KEY(dataset_id) REFERENCES datasets(id) ON DELETE CASCADE
                );
                """
            )

    def create_dataset(self, name: str) -> dict[str, Any]:
        dataset_id = str(uuid.uuid4())
        now = utc_now()
        row = {
            "id": dataset_id,
            "name": name,
            "status": "uploaded",
            "approved_proposal_id": None,
            "created_at": now,
            "updated_at": now,
        }
        with self.connect() as conn:
            conn.execute(
                """
                INSERT INTO datasets(id, name, status, approved_proposal_id, created_at, updated_at)
                VALUES(:id, :name, :status, :approved_proposal_id, :created_at, :updated_at)
                """,
                row,
            )
        return row

    def update_dataset_status(
        self,
        dataset_id: str,
        status: str,
        approved_proposal_id: str | None = None,
        name: str | None = None,
    ) -> None:
        with self.connect() as conn:
            current = conn.execute("SELECT * FROM datasets WHERE id = ?", (dataset_id,)).fetchone()
            if not current:
                raise KeyError(dataset_id)
            conn.execute(
                """
                UPDATE datasets
                SET status = ?, approved_proposal_id = ?, name = ?, updated_at = ?
                WHERE id = ?
                """,
                (
                    status,
                    approved_proposal_id if approved_proposal_id is not None else current["approved_proposal_id"],
                    name if name is not None else current["name"],
                    utc_now(),
                    dataset_id,
                ),
            )

    def add_source_file(
        self,
        dataset_id: str,
        filename: str,
        stored_path: str,
        file_type: str,
        sheet_count: int,
    ) -> dict[str, Any]:
        row = {
            "id": str(uuid.uuid4()),
            "dataset_id": dataset_id,
            "filename": filename,
            "stored_path": stored_path,
            "file_type": file_type,
            "sheet_count": sheet_count,
            "created_at": utc_now(),
        }
        with self.connect() as conn:
            conn.execute(
                """
                INSERT INTO source_files(id, dataset_id, filename, stored_path, file_type, sheet_count, created_at)
                VALUES(:id, :dataset_id, :filename, :stored_path, :file_type, :sheet_count, :created_at)
                """,
                row,
            )
        return row

    def list_datasets(self) -> list[dict[str, Any]]:
        with self.connect() as conn:
            rows = conn.execute(
                "SELECT * FROM datasets ORDER BY datetime(created_at) DESC"
            ).fetchall()
        return [dict(row) for row in rows]

    def get_dataset(self, dataset_id: str) -> dict[str, Any] | None:
        with self.connect() as conn:
            row = conn.execute("SELECT * FROM datasets WHERE id = ?", (dataset_id,)).fetchone()
        return dict(row) if row else None

    def list_source_files(self, dataset_id: str) -> list[dict[str, Any]]:
        with self.connect() as conn:
            rows = conn.execute(
                "SELECT * FROM source_files WHERE dataset_id = ? ORDER BY filename",
                (dataset_id,),
            ).fetchall()
        return [dict(row) for row in rows]

    def create_proposal(
        self,
        dataset_id: str,
        proposal: dict[str, Any],
        feedback: str | None = None,
        status: str = "proposed",
    ) -> dict[str, Any]:
        with self.connect() as conn:
            version = (
                conn.execute(
                    "SELECT COALESCE(MAX(version), 0) + 1 FROM proposals WHERE dataset_id = ?",
                    (dataset_id,),
                ).fetchone()[0]
            )
            row = {
                "id": str(uuid.uuid4()),
                "dataset_id": dataset_id,
                "version": version,
                "status": status,
                "feedback": feedback,
                "proposal_json": json.dumps(json_compatible(proposal), ensure_ascii=False),
                "created_at": utc_now(),
            }
            conn.execute(
                """
                INSERT INTO proposals(id, dataset_id, version, status, feedback, proposal_json, created_at)
                VALUES(:id, :dataset_id, :version, :status, :feedback, :proposal_json, :created_at)
                """,
                row,
            )
        return self.deserialize_proposal(row)

    def deserialize_proposal(self, row: dict[str, Any]) -> dict[str, Any]:
        item = dict(row)
        item["proposal"] = json.loads(item.pop("proposal_json"))
        return item

    def get_proposal(self, proposal_id: str) -> dict[str, Any] | None:
        with self.connect() as conn:
            row = conn.execute("SELECT * FROM proposals WHERE id = ?", (proposal_id,)).fetchone()
        return self.deserialize_proposal(dict(row)) if row else None

    def get_latest_proposal(self, dataset_id: str) -> dict[str, Any] | None:
        with self.connect() as conn:
            row = conn.execute(
                """
                SELECT * FROM proposals
                WHERE dataset_id = ?
                ORDER BY version DESC
                LIMIT 1
                """,
                (dataset_id,),
            ).fetchone()
        return self.deserialize_proposal(dict(row)) if row else None

    def replace_dataset_tables(self, dataset_id: str, tables: list[dict[str, Any]]) -> None:
        with self.connect() as conn:
            conn.execute("DELETE FROM dataset_tables WHERE dataset_id = ?", (dataset_id,))
            now = utc_now()
            for table in tables:
                conn.execute(
                    """
                    INSERT INTO dataset_tables(id, dataset_id, mode, table_name, display_name, schema_json, created_at)
                    VALUES(?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        str(uuid.uuid4()),
                        dataset_id,
                        table["mode"],
                        table["table_name"],
                        table["display_name"],
                        json.dumps(table["schema"], ensure_ascii=False),
                        now,
                    ),
                )

    def replace_approval_decisions(
        self,
        dataset_id: str,
        proposal_id: str,
        decisions: list[dict[str, Any]],
    ) -> None:
        with self.connect() as conn:
            conn.execute("DELETE FROM approval_decisions WHERE dataset_id = ?", (dataset_id,))
            now = utc_now()
            for decision in decisions:
                conn.execute(
                    """
                    INSERT INTO approval_decisions(id, dataset_id, proposal_id, canonical_name, decision_json, created_at)
                    VALUES(?, ?, ?, ?, ?, ?)
                    """,
                    (
                        str(uuid.uuid4()),
                        dataset_id,
                        proposal_id,
                        decision["canonical_name"],
                        json.dumps(decision, ensure_ascii=False),
                        now,
                    ),
                )

    def list_approval_decisions(self, dataset_id: str) -> list[dict[str, Any]]:
        with self.connect() as conn:
            rows = conn.execute(
                """
                SELECT * FROM approval_decisions
                WHERE dataset_id = ?
                ORDER BY datetime(created_at) DESC, canonical_name
                """,
                (dataset_id,),
            ).fetchall()
        return [json.loads(row["decision_json"]) for row in rows]

    def replace_column_lineage(self, dataset_id: str, lineage_items: list[dict[str, Any]]) -> None:
        with self.connect() as conn:
            conn.execute("DELETE FROM merged_column_lineage WHERE dataset_id = ?", (dataset_id,))
            now = utc_now()
            for item in lineage_items:
                conn.execute(
                    """
                    INSERT INTO merged_column_lineage(id, dataset_id, table_name, column_name, lineage_json, created_at)
                    VALUES(?, ?, ?, ?, ?, ?)
                    """,
                    (
                        str(uuid.uuid4()),
                        dataset_id,
                        item["table_name"],
                        item["column_name"],
                        json.dumps(item, ensure_ascii=False),
                        now,
                    ),
                )

    def list_column_lineage(self, dataset_id: str) -> list[dict[str, Any]]:
        with self.connect() as conn:
            rows = conn.execute(
                """
                SELECT * FROM merged_column_lineage
                WHERE dataset_id = ?
                ORDER BY table_name, column_name
                """,
                (dataset_id,),
            ).fetchall()
        return [json.loads(row["lineage_json"]) for row in rows]

    def list_dataset_tables(self, dataset_id: str, mode: str | None = None) -> list[dict[str, Any]]:
        query = "SELECT * FROM dataset_tables WHERE dataset_id = ?"
        params: list[Any] = [dataset_id]
        if mode:
            query += " AND mode = ?"
            params.append(mode)
        query += " ORDER BY mode, display_name"
        with self.connect() as conn:
            rows = conn.execute(query, params).fetchall()
        output = []
        for row in rows:
            item = dict(row)
            item["schema"] = json.loads(item.pop("schema_json"))
            output.append(item)
        return output

    def clear_query_history(self, dataset_id: str) -> None:
        with self.connect() as conn:
            conn.execute("DELETE FROM query_history WHERE dataset_id = ?", (dataset_id,))

    def add_query_history(
        self,
        dataset_id: str,
        target_mode: str,
        question: str,
        sql: str,
        explanation: str,
        result: dict[str, Any],
    ) -> dict[str, Any]:
        row = {
            "id": str(uuid.uuid4()),
            "dataset_id": dataset_id,
            "target_mode": target_mode,
            "question": question,
            "sql_text": sql,
            "explanation": explanation,
            "result_json": json.dumps(result, ensure_ascii=False),
            "created_at": utc_now(),
        }
        with self.connect() as conn:
            conn.execute(
                """
                INSERT INTO query_history(id, dataset_id, target_mode, question, sql_text, explanation, result_json, created_at)
                VALUES(:id, :dataset_id, :target_mode, :question, :sql_text, :explanation, :result_json, :created_at)
                """,
                row,
            )
        return {
            "id": row["id"],
            "dataset_id": dataset_id,
            "target_mode": target_mode,
            "question": question,
            "sql": sql,
            "explanation": explanation,
            "result": result,
            "created_at": row["created_at"],
        }

    def list_query_history(self, dataset_id: str) -> list[dict[str, Any]]:
        with self.connect() as conn:
            rows = conn.execute(
                """
                SELECT * FROM query_history
                WHERE dataset_id = ?
                ORDER BY datetime(created_at) DESC
                """,
                (dataset_id,),
            ).fetchall()
        output = []
        for row in rows:
            item = dict(row)
            output.append(
                {
                    "id": item["id"],
                    "dataset_id": item["dataset_id"],
                    "target_mode": item["target_mode"],
                    "question": item["question"],
                    "sql": item["sql_text"],
                    "explanation": item["explanation"],
                    "result": json.loads(item["result_json"]),
                    "created_at": item["created_at"],
                }
            )
        return output
