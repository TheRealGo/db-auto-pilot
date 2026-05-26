from __future__ import annotations

import json
import shutil
import sqlite3
import uuid
from contextlib import contextmanager
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Iterator

from app.version import SCHEMA_VERSION


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def json_compatible(value: Any) -> Any:
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, dict):
        return {str(k): json_compatible(v) for k, v in value.items()}
    if isinstance(value, list):
        return [json_compatible(v) for v in value]
    return value


class MetadataRepository:
    def __init__(self, database_path: Path):
        self.database_path = database_path
        self.database_path.parent.mkdir(parents=True, exist_ok=True)
        self._backup_before_migration()
        self.initialize()

    @contextmanager
    def connect(self) -> Iterator[sqlite3.Connection]:
        conn = sqlite3.connect(self.database_path)
        conn.execute("pragma foreign_keys = on")
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
                create table if not exists datasets (
                    id text primary key,
                    name text not null,
                    status text not null,
                    created_at text not null,
                    updated_at text not null,
                    materialized_table text,
                    proposal_json text,
                    materialization_json text
                );
                create table if not exists source_files (
                    id text primary key,
                    dataset_id text not null references datasets(id) on delete cascade,
                    filename text not null,
                    sheet_name text,
                    table_name text not null,
                    file_path text not null,
                    rows integer not null,
                    columns_json text not null,
                    profile_json text not null
                );
                create table if not exists query_history (
                    id text primary key,
                    dataset_id text,
                    question text not null,
                    sql text not null,
                    row_count integer not null,
                    created_at text not null
                );
                create table if not exists proposals (
                    id text primary key,
                    dataset_id text not null references datasets(id) on delete cascade,
                    version integer not null,
                    feedback text not null default '',
                    proposal_json text not null,
                    created_at text not null
                );
                create table if not exists approval_decisions (
                    id text primary key,
                    dataset_id text not null references datasets(id) on delete cascade,
                    proposal_id text,
                    decision_type text not null,
                    notes text not null default '',
                    payload_json text not null,
                    created_at text not null
                );
                create table if not exists materialization_runs (
                    id text primary key,
                    dataset_id text not null references datasets(id) on delete cascade,
                    table_name text not null,
                    status text not null,
                    row_count integer not null default 0,
                    column_count integer not null default 0,
                    plan_json text not null,
                    error text,
                    created_at text not null
                );
                create table if not exists materialization_proposals (
                    id text primary key,
                    dataset_id text not null references datasets(id) on delete cascade,
                    version integer not null,
                    feedback text not null default '',
                    plan_json text not null,
                    created_at text not null
                );
                create table if not exists dataset_tables (
                    id text primary key,
                    dataset_id text not null references datasets(id) on delete cascade,
                    table_name text not null,
                    mode text not null,
                    row_count integer not null,
                    column_count integer not null,
                    created_at text not null
                );
                create table if not exists merged_column_lineage (
                    id text primary key,
                    dataset_id text not null references datasets(id) on delete cascade,
                    table_name text not null,
                    output_column text not null,
                    source_table text,
                    source_column text,
                    action text not null,
                    created_at text not null
                );
                """
            )
            self._ensure_columns(
                conn,
                "datasets",
                {
                    "materialized_table": "text",
                    "proposal_json": "text",
                    "materialization_json": "text",
                },
            )
            self._ensure_columns(
                conn,
                "query_history",
                {
                    "sql": "text not null default ''",
                    "row_count": "integer not null default 0",
                    "explanation": "text not null default ''",
                    "columns_json": "text not null default '[]'",
                    "result_preview_json": "text not null default '[]'",
                    "materialized_table": "text",
                    "materialization_run_id": "text",
                    "target_mode": "text not null default 'merged'",
                    "proposal_id": "text",
                    "proposal_version": "integer",
                    "materialization_proposal_id": "text",
                    "materialization_proposal_version": "integer",
                },
            )
            self._migrate_query_history_legacy_columns(conn)
            self._ensure_columns(
                conn,
                "approval_decisions",
                {
                    "decision_type": "text not null default 'approval'",
                    "notes": "text not null default ''",
                    "payload_json": "text not null default '{}'",
                },
            )
            self._migrate_approval_decisions_legacy_columns(conn)
            self._ensure_columns(
                conn,
                "materialization_proposals",
                {
                    "version": "integer not null default 0",
                    "feedback": "text not null default ''",
                    "plan_json": "text not null default '{}'",
                },
            )
            self._migrate_materialization_proposals_legacy_columns(conn)
            self._normalize_version_sequence(conn, "proposals")
            self._normalize_version_sequence(conn, "materialization_proposals")
            conn.executescript(
                """
                create unique index if not exists ux_proposals_dataset_version
                    on proposals(dataset_id, version);
                create unique index if not exists ux_materialization_proposals_dataset_version
                    on materialization_proposals(dataset_id, version);
                create index if not exists ix_approval_decisions_dataset_created
                    on approval_decisions(dataset_id, created_at desc);
                """
            )
            conn.execute(f"pragma user_version = {SCHEMA_VERSION}")

    def _backup_before_migration(self) -> Path | None:
        if not self.database_path.exists() or self.database_path.stat().st_size == 0:
            return None
        with sqlite3.connect(self.database_path) as conn:
            current_version = int(conn.execute("pragma user_version").fetchone()[0])
        if current_version >= SCHEMA_VERSION:
            return None
        timestamp = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")
        backup_path = self.database_path.with_name(f"{self.database_path.stem}.pre-migration-v{current_version}-{timestamp}-{uuid.uuid4().hex[:8]}{self.database_path.suffix}.bak")
        shutil.copy2(self.database_path, backup_path)
        return backup_path

    def _ensure_columns(self, conn: sqlite3.Connection, table: str, columns: dict[str, str]) -> None:
        existing = {row["name"] for row in conn.execute(f"pragma table_info({table})").fetchall()}
        for name, definition in columns.items():
            if name not in existing:
                conn.execute(f"alter table {table} add column {name} {definition}")

    def _migrate_query_history_legacy_columns(self, conn: sqlite3.Connection) -> None:
        columns = {row["name"] for row in conn.execute("pragma table_info(query_history)").fetchall()}
        if "sql_text" in columns:
            conn.execute("update query_history set sql = sql_text where coalesce(sql, '') = ''")
        if "result_json" in columns:
            rows = conn.execute(
                """
                select id, result_json from query_history
                where coalesce(result_preview_json, '[]') = '[]' and coalesce(result_json, '') != ''
                """
            ).fetchall()
            for row in rows:
                try:
                    parsed = json.loads(row["result_json"])
                except json.JSONDecodeError:
                    parsed = []
                preview = parsed if isinstance(parsed, list) else []
                conn.execute(
                    "update query_history set result_preview_json = ?, row_count = ? where id = ?",
                    (
                        json.dumps(json_compatible(preview[:20]), ensure_ascii=False),
                        len(preview),
                        row["id"],
                    ),
                )

    def _migrate_materialization_proposals_legacy_columns(self, conn: sqlite3.Connection) -> None:
        columns = {row["name"] for row in conn.execute("pragma table_info(materialization_proposals)").fetchall()}
        if "materialization_json" in columns and "plan_json" in columns:
            conn.execute(
                """
                update materialization_proposals
                set plan_json = materialization_json
                where coalesce(plan_json, '{}') = '{}' and coalesce(materialization_json, '') != ''
                """
            )
        rows = conn.execute(
            """
            select id, dataset_id from materialization_proposals
            where coalesce(version, 0) = 0
            order by dataset_id, created_at, id
            """
        ).fetchall()
        counters: dict[str, int] = {}
        for row in rows:
            dataset_id = row["dataset_id"]
            counters[dataset_id] = counters.get(dataset_id, 0) + 1
            conn.execute(
                "update materialization_proposals set version = ? where id = ?",
                (counters[dataset_id], row["id"]),
            )

    def _migrate_approval_decisions_legacy_columns(self, conn: sqlite3.Connection) -> None:
        columns = {row["name"] for row in conn.execute("pragma table_info(approval_decisions)").fetchall()}
        if "canonical_name" in columns:
            conn.execute(
                """
                update approval_decisions
                set decision_type = canonical_name
                where coalesce(decision_type, 'approval') = 'approval' and coalesce(canonical_name, '') != ''
                """
            )
        if "decision_json" in columns:
            conn.execute(
                """
                update approval_decisions
                set payload_json = decision_json
                where coalesce(payload_json, '{}') = '{}' and coalesce(decision_json, '') != ''
                """
            )

    def _normalize_version_sequence(self, conn: sqlite3.Connection, table: str) -> None:
        rows = conn.execute(
            f"""
            select id, dataset_id, version
            from {table}
            order by dataset_id, version, created_at, id
            """
        ).fetchall()
        counters: dict[str, int] = {}
        for row in rows:
            dataset_id = row["dataset_id"]
            counters[dataset_id] = counters.get(dataset_id, 0) + 1
            version = counters[dataset_id]
            if int(row["version"]) != version:
                conn.execute(f"update {table} set version = ? where id = ?", (version, row["id"]))

    def create_dataset(self, name: str) -> str:
        dataset_id = str(uuid.uuid4())
        now = utc_now()
        with self.connect() as conn:
            conn.execute(
                "insert into datasets(id, name, status, created_at, updated_at) values (?, ?, ?, ?, ?)",
                (dataset_id, name, "uploaded", now, now),
            )
        return dataset_id

    def add_source_file(
        self,
        dataset_id: str,
        filename: str,
        sheet_name: str | None,
        table_name: str,
        file_path: Path,
        rows: int,
        columns: list[str],
        profile: dict[str, Any],
    ) -> str:
        source_id = str(uuid.uuid4())
        with self.connect() as conn:
            conn.execute(
                """
                insert into source_files(id, dataset_id, filename, sheet_name, table_name, file_path, rows, columns_json, profile_json)
                values (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    source_id,
                    dataset_id,
                    filename,
                    sheet_name,
                    table_name,
                    str(file_path),
                    rows,
                    json.dumps(columns, ensure_ascii=False),
                    json.dumps(json_compatible(profile), ensure_ascii=False),
                ),
            )
            conn.execute("update datasets set updated_at = ? where id = ?", (utc_now(), dataset_id))
        return source_id

    def list_datasets(self) -> list[dict[str, Any]]:
        with self.connect() as conn:
            rows = conn.execute(
                """
                select d.*, count(s.id) as source_count
                from datasets d left join source_files s on s.dataset_id = d.id
                group by d.id
                order by d.updated_at desc
                """
            ).fetchall()
        return [dict(row) for row in rows]

    def get_dataset(self, dataset_id: str) -> dict[str, Any] | None:
        with self.connect() as conn:
            row = conn.execute("select * from datasets where id = ?", (dataset_id,)).fetchone()
            if row is None:
                return None
            sources = conn.execute("select * from source_files where dataset_id = ? order by filename, sheet_name", (dataset_id,)).fetchall()
            proposal_row = conn.execute(
                """
                select id, version, proposal_json from proposals
                where dataset_id = ?
                order by version desc
                limit 1
                """,
                (dataset_id,),
            ).fetchone()
        dataset = dict(row)
        dataset["sources"] = [self._source_from_row(source) for source in sources]
        dataset["source_count"] = len(dataset["sources"])
        dataset["materialization_runs"] = self.list_materialization_runs(dataset_id)
        if proposal_row is not None:
            dataset["proposal"] = json.loads(proposal_row["proposal_json"])
            dataset["proposal_id"] = proposal_row["id"]
            dataset["proposal_version"] = int(proposal_row["version"])
        else:
            dataset["proposal"] = json.loads(dataset["proposal_json"]) if dataset.get("proposal_json") else None
            dataset["proposal_id"] = None
            dataset["proposal_version"] = None
        dataset["materialization"] = json.loads(dataset["materialization_json"]) if dataset.get("materialization_json") else None
        return dataset

    def _source_from_row(self, row: sqlite3.Row) -> dict[str, Any]:
        data = dict(row)
        data["columns"] = json.loads(data.pop("columns_json"))
        data["profile"] = json.loads(data.pop("profile_json"))
        return data

    def save_proposal(self, dataset_id: str, proposal: dict[str, Any], status: str = "proposed", feedback: str = "") -> str:
        proposal_id = str(uuid.uuid4())
        proposal_json = json.dumps(json_compatible(proposal), ensure_ascii=False)
        with self.connect() as conn:
            conn.execute("begin immediate")
            table_columns = {row["name"] for row in conn.execute("pragma table_info(proposals)").fetchall()}
            version = conn.execute("select coalesce(max(version), 0) + 1 from proposals where dataset_id = ?", (dataset_id,)).fetchone()[0]
            values: dict[str, Any] = {
                "id": proposal_id,
                "dataset_id": dataset_id,
                "version": int(version),
                "feedback": feedback,
                "proposal_json": proposal_json,
                "created_at": utc_now(),
                "status": status,
            }
            insert_columns = [column for column in values if column in table_columns]
            placeholders = ", ".join("?" for _ in insert_columns)
            conn.execute(
                f"insert into proposals({', '.join(insert_columns)}) values ({placeholders})",
                tuple(values[column] for column in insert_columns),
            )
            conn.execute(
                "update datasets set proposal_json = ?, status = ?, updated_at = ? where id = ?",
                (proposal_json, status, utc_now(), dataset_id),
            )
        return proposal_id

    def latest_proposal(self, dataset_id: str) -> dict[str, Any] | None:
        with self.connect() as conn:
            row = conn.execute(
                """
                select id, version, feedback, proposal_json, created_at
                from proposals
                where dataset_id = ?
                order by version desc
                limit 1
                """,
                (dataset_id,),
            ).fetchone()
        if row is None:
            return None
        proposal = dict(row)
        proposal["proposal"] = json.loads(proposal.pop("proposal_json"))
        return proposal

    def list_proposals(self, dataset_id: str, limit: int = 20) -> list[dict[str, Any]]:
        with self.connect() as conn:
            rows = conn.execute(
                """
                select id, dataset_id, version, feedback, proposal_json, created_at
                from proposals
                where dataset_id = ?
                order by version desc
                limit ?
                """,
                (dataset_id, limit),
            ).fetchall()
        entries = []
        for row in rows:
            entry = dict(row)
            entry["proposal"] = json.loads(entry.pop("proposal_json"))
            entries.append(entry)
        return entries

    def mark_proposal_approved(self, dataset_id: str, proposal: dict[str, Any]) -> None:
        with self.connect() as conn:
            conn.execute(
                "update datasets set proposal_json = ?, status = ?, updated_at = ? where id = ?",
                (json.dumps(json_compatible(proposal), ensure_ascii=False), "approved", utc_now(), dataset_id),
            )

    def save_materialization_proposal(self, dataset_id: str, plan: dict[str, Any], feedback: str = "") -> str:
        proposal_id = str(uuid.uuid4())
        with self.connect() as conn:
            conn.execute("begin immediate")
            table_columns = {row["name"] for row in conn.execute("pragma table_info(materialization_proposals)").fetchall()}
            version = conn.execute(
                "select coalesce(max(version), 0) + 1 from materialization_proposals where dataset_id = ?",
                (dataset_id,),
            ).fetchone()[0]
            plan_json = json.dumps(json_compatible(plan), ensure_ascii=False)
            values: dict[str, Any] = {
                "id": proposal_id,
                "dataset_id": dataset_id,
                "version": int(version),
                "feedback": feedback,
                "plan_json": plan_json,
                "created_at": utc_now(),
                "proposal_id": "",
                "status": "proposed",
                "source_run_id": "",
                "materialization_json": plan_json,
            }
            insert_columns = [column for column in values if column in table_columns]
            placeholders = ", ".join("?" for _ in insert_columns)
            conn.execute(
                f"insert into materialization_proposals({', '.join(insert_columns)}) values ({placeholders})",
                tuple(values[column] for column in insert_columns),
            )
        return proposal_id

    def latest_materialization_proposal(self, dataset_id: str) -> dict[str, Any] | None:
        with self.connect() as conn:
            row = conn.execute(
                """
                select id, version, feedback, plan_json, created_at
                from materialization_proposals
                where dataset_id = ?
                order by version desc
                limit 1
                """,
                (dataset_id,),
            ).fetchone()
        if row is None:
            return None
        proposal = dict(row)
        proposal["plan"] = json.loads(proposal.pop("plan_json"))
        return proposal

    def list_materialization_proposals(self, dataset_id: str, limit: int = 20) -> list[dict[str, Any]]:
        with self.connect() as conn:
            rows = conn.execute(
                """
                select id, dataset_id, version, feedback, plan_json, created_at
                from materialization_proposals
                where dataset_id = ?
                order by version desc
                limit ?
                """,
                (dataset_id, limit),
            ).fetchall()
        entries = []
        for row in rows:
            entry = dict(row)
            entry["plan"] = json.loads(entry.pop("plan_json"))
            entries.append(entry)
        return entries

    def add_approval_decision(
        self,
        dataset_id: str,
        proposal_id: str | None,
        decision_type: str,
        notes: str,
        payload: dict[str, Any],
    ) -> str:
        approval_id = str(uuid.uuid4())
        with self.connect() as conn:
            table_columns = {row["name"] for row in conn.execute("pragma table_info(approval_decisions)").fetchall()}
            payload_json = json.dumps(json_compatible(payload), ensure_ascii=False)
            values: dict[str, Any] = {
                "id": approval_id,
                "dataset_id": dataset_id,
                "proposal_id": proposal_id,
                "decision_type": decision_type,
                "notes": notes,
                "payload_json": payload_json,
                "created_at": utc_now(),
                "canonical_name": decision_type,
                "decision_json": payload_json,
            }
            insert_columns = [column for column in values if column in table_columns]
            placeholders = ", ".join("?" for _ in insert_columns)
            conn.execute(
                f"insert into approval_decisions({', '.join(insert_columns)}) values ({placeholders})",
                tuple(values[column] for column in insert_columns),
            )
        return approval_id

    def list_approval_decisions(self, dataset_id: str) -> list[dict[str, Any]]:
        with self.connect() as conn:
            rows = conn.execute(
                """
                select * from approval_decisions
                where dataset_id = ?
                order by created_at desc
                """,
                (dataset_id,),
            ).fetchall()
        entries = []
        for row in rows:
            entry = dict(row)
            entry["payload"] = json.loads(entry.pop("payload_json"))
            entries.append(entry)
        return entries

    def save_materialization(self, dataset_id: str, table_name: str, payload: dict[str, Any], status: str = "materialized") -> str:
        run_id = str(uuid.uuid4())
        with self.connect() as conn:
            conn.execute(
                """
                update datasets
                set materialized_table = ?, materialization_json = ?, status = ?, updated_at = ?
                where id = ?
                """,
                (table_name, json.dumps(json_compatible(payload), ensure_ascii=False), status, utc_now(), dataset_id),
            )
            conn.execute(
                """
                insert into materialization_runs(id, dataset_id, table_name, status, row_count, column_count, plan_json, error, created_at)
                values (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    run_id,
                    dataset_id,
                    table_name,
                    status,
                    int(payload.get("rows", 0)),
                    len(payload.get("columns", [])),
                    json.dumps(json_compatible(payload.get("plan", {})), ensure_ascii=False),
                    payload.get("error"),
                    utc_now(),
                ),
            )
            conn.execute(
                """
                insert into dataset_tables(id, dataset_id, table_name, mode, row_count, column_count, created_at)
                values (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    str(uuid.uuid4()),
                    dataset_id,
                    table_name,
                    payload.get("mode", "merged"),
                    int(payload.get("rows", 0)),
                    len(payload.get("columns", [])),
                    utc_now(),
                ),
            )
            lineage = payload.get("lineage")
            if not isinstance(lineage, list):
                lineage = []
                for group in payload.get("plan", {}).get("canonical_columns", []):
                    for member in group.get("members", []) or [{"table": None, "column": None}]:
                        lineage.append(
                            {
                                "output_column": group.get("canonical_name", ""),
                                "source_table": member.get("table"),
                                "source_column": member.get("column"),
                                "action": group.get("action", "keep"),
                            }
                        )
            for entry in lineage:
                conn.execute(
                    """
                    insert into merged_column_lineage(id, dataset_id, table_name, output_column, source_table, source_column, action, created_at)
                    values (?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        str(uuid.uuid4()),
                        dataset_id,
                        table_name,
                        entry.get("output_column", ""),
                        entry.get("source_table"),
                        entry.get("source_column"),
                        entry.get("action", "keep"),
                        utc_now(),
                    ),
                )
        return run_id

    def record_materialization_failure(self, dataset_id: str, table_name: str, plan: dict[str, Any], error: str) -> str:
        run_id = str(uuid.uuid4())
        payload = dict(plan)
        payload.setdefault("retry_guidance", "Review the failed materialization evidence, revise mappings or limits, and retry.")
        with self.connect() as conn:
            conn.execute(
                """
                insert into materialization_runs(id, dataset_id, table_name, status, row_count, column_count, plan_json, error, created_at)
                values (?, ?, ?, 'failed', 0, 0, ?, ?, ?)
                """,
                (
                    run_id,
                    dataset_id,
                    table_name,
                    json.dumps(json_compatible(payload), ensure_ascii=False),
                    error,
                    utc_now(),
                ),
            )
        return run_id

    def list_materialization_runs(self, dataset_id: str, limit: int = 20) -> list[dict[str, Any]]:
        with self.connect() as conn:
            rows = conn.execute(
                """
                select * from materialization_runs
                where dataset_id = ?
                order by created_at desc
                limit ?
                """,
                (dataset_id, limit),
            ).fetchall()
        runs = []
        for row in rows:
            run = dict(row)
            run["plan"] = json.loads(run.pop("plan_json") or "{}")
            runs.append(run)
        return runs

    def latest_materialization_run(self, dataset_id: str, status: str | None = None) -> dict[str, Any] | None:
        with self.connect() as conn:
            if status is None:
                row = conn.execute(
                    """
                    select * from materialization_runs
                    where dataset_id = ?
                    order by created_at desc
                    limit 1
                    """,
                    (dataset_id,),
                ).fetchone()
            else:
                row = conn.execute(
                    """
                    select * from materialization_runs
                    where dataset_id = ? and status = ?
                    order by created_at desc
                    limit 1
                    """,
                    (dataset_id, status),
                ).fetchone()
        if row is None:
            return None
        run = dict(row)
        run["plan"] = json.loads(run.pop("plan_json") or "{}")
        return run

    def latest_materialization_run_id(self, dataset_id: str, table_name: str) -> str | None:
        with self.connect() as conn:
            row = conn.execute(
                """
                select id from materialization_runs
                where dataset_id = ? and table_name = ? and status = 'materialized'
                order by created_at desc
                limit 1
                """,
                (dataset_id, table_name),
            ).fetchone()
        return row["id"] if row else None

    def add_query_history(
        self,
        dataset_id: str | None,
        question: str,
        sql: str,
        row_count: int,
        explanation: str = "",
        columns: list[str] | None = None,
        result_preview: list[dict[str, Any]] | None = None,
        materialized_table: str | None = None,
        materialization_run_id: str | None = None,
        target_mode: str = "merged",
        proposal_id: str | None = None,
        proposal_version: int | None = None,
        materialization_proposal_id: str | None = None,
        materialization_proposal_version: int | None = None,
    ) -> str:
        history_id = str(uuid.uuid4())
        with self.connect() as conn:
            table_columns = {row["name"] for row in conn.execute("pragma table_info(query_history)").fetchall()}
            preview_json = json.dumps(json_compatible((result_preview or [])[:20]), ensure_ascii=False)
            values: dict[str, Any] = {
                "id": history_id,
                "dataset_id": dataset_id,
                "question": question,
                "sql": sql,
                "row_count": row_count,
                "created_at": utc_now(),
                "explanation": explanation,
                "columns_json": json.dumps(columns or [], ensure_ascii=False),
                "result_preview_json": preview_json,
                "materialized_table": materialized_table,
                "materialization_run_id": materialization_run_id,
                "target_mode": target_mode,
                "proposal_id": proposal_id,
                "proposal_version": proposal_version,
                "materialization_proposal_id": materialization_proposal_id,
                "materialization_proposal_version": materialization_proposal_version,
            }
            if "sql_text" in table_columns:
                values["sql_text"] = sql
            if "result_json" in table_columns:
                values["result_json"] = preview_json
            insert_columns = [column for column in values if column in table_columns]
            placeholders = ", ".join("?" for _ in insert_columns)
            conn.execute(
                f"insert into query_history({', '.join(insert_columns)}) values ({placeholders})",
                tuple(values[column] for column in insert_columns),
            )
        return history_id

    def list_query_history(self, limit: int = 50, dataset_id: str | None = None) -> list[dict[str, Any]]:
        with self.connect() as conn:
            if dataset_id is None:
                rows = conn.execute(
                    "select * from query_history order by created_at desc limit ?",
                    (limit,),
                ).fetchall()
            else:
                rows = conn.execute(
                    """
                    select * from query_history
                    where dataset_id = ?
                    order by created_at desc
                    limit ?
                    """,
                    (dataset_id, limit),
                ).fetchall()
        entries = []
        for row in rows:
            entry = dict(row)
            entry["columns"] = json.loads(entry.pop("columns_json", "[]") or "[]")
            entry["result_preview"] = json.loads(entry.pop("result_preview_json", "[]") or "[]")
            entries.append(entry)
        return entries
