from __future__ import annotations

import re
import sqlite3
from typing import Any

from app.config import Settings, effective_openai_settings


class QueryGenerationError(RuntimeError):
    pass


FORBIDDEN_SQL = re.compile(r"\b(insert|update|delete|drop|alter|create|attach|detach|pragma|vacuum|replace|truncate)\b", re.I)
INTERNAL_TABLES = {
    "datasets",
    "source_files",
    "proposals",
    "approval_decisions",
    "materialization_runs",
    "materialization_proposals",
    "dataset_tables",
    "merged_column_lineage",
    "query_history",
}


def quote_identifier(identifier: str) -> str:
    return '"' + identifier.replace('"', '""') + '"'


def validate_select_sql(sql: str, default_limit: int = 1000) -> str:
    normalized = sql.strip()
    if not normalized:
        raise QueryGenerationError("Empty SQL generated")
    if ";" in normalized:
        raise QueryGenerationError("Multiple SQL statements are not allowed")
    if "--" in normalized or "/*" in normalized or "*/" in normalized:
        raise QueryGenerationError("SQL comments are not allowed")
    if not re.match(r"^(select|with)\b", normalized, re.I):
        raise QueryGenerationError("Only SELECT or WITH read-only statements are allowed")
    if FORBIDDEN_SQL.search(normalized):
        raise QueryGenerationError("Query contains forbidden SQL operation")
    if not re.search(r"\blimit\b", normalized, re.I):
        normalized += f" limit {int(default_limit)}"
    return normalized


def validate_table_scope(conn: sqlite3.Connection, sql: str, allowed_tables: set[str]) -> None:
    if not allowed_tables:
        raise QueryGenerationError("No dataset table is available for query scope validation")
    validated = validate_select_sql(sql)
    referenced: set[str] = set()

    def authorizer(action: int, arg1: str | None, arg2: str | None, dbname: str | None, source: str | None) -> int:
        if action == sqlite3.SQLITE_READ and arg1:
            referenced.add(arg1)
            if arg1 not in allowed_tables:
                return sqlite3.SQLITE_DENY
        if action not in {sqlite3.SQLITE_SELECT, sqlite3.SQLITE_READ, sqlite3.SQLITE_FUNCTION}:
            return sqlite3.SQLITE_DENY
        return sqlite3.SQLITE_OK

    conn.set_authorizer(authorizer)
    try:
        conn.execute(f"explain query plan {validated}").fetchall()
    except sqlite3.DatabaseError as exc:
        raise QueryGenerationError(f"Query references tables outside the selected dataset: {exc}") from exc
    finally:
        conn.set_authorizer(None)
    if not referenced.intersection(allowed_tables):
        raise QueryGenerationError("Query must reference the selected dataset materialized table")


def _schema(conn: sqlite3.Connection, allowed_tables: set[str] | None = None) -> dict[str, list[dict[str, Any]]]:
    tables = conn.execute("select name from sqlite_master where type = 'table' and name not like 'sqlite_%'").fetchall()
    result: dict[str, list[dict[str, Any]]] = {}
    for (table,) in tables:
        if table in INTERNAL_TABLES:
            continue
        if allowed_tables is not None and table not in allowed_tables:
            continue
        result[table] = [
            {"name": row[1], "type": row[2]}
            for row in conn.execute(f"pragma table_info({quote_identifier(table)})")
        ]
    return result


def schema_prompt(conn: sqlite3.Connection, allowed_tables: set[str] | None = None) -> str:
    return "\n".join(
        f"{table}({', '.join(column['name'] + ' ' + column['type'] for column in columns)})"
        for table, columns in _schema(conn, allowed_tables).items()
    )


def fallback_query(question: str, conn: sqlite3.Connection, dataset_table: str | None = None, limit: int = 200) -> tuple[str, str]:
    schema = _schema(conn)
    table = dataset_table if dataset_table in schema else next(iter(schema), None)
    if not table:
        raise QueryGenerationError("No materialized analytical table exists")
    columns = [column["name"] for column in schema[table]]
    lowered = question.lower()
    numeric = [c for c in columns if any(token in c.lower() for token in ["sales", "revenue", "amount", "total", "売上", "金額"])]
    dimensions = [c for c in columns if any(token in c.lower() for token in ["department", "dept", "division", "部署", "部門"])]
    if any(token in lowered for token in ["count", "rows", "records"]) or any(token in question for token in ["件数", "何件", "行数"]):
        sql = f"select count(*) as row_count from {quote_identifier(table)} limit {int(limit)}"
        return sql, f"{table} の行数を集計しました。"
    if numeric and dimensions and ("合計" in question or "sum" in lowered or "total" in lowered):
        sql = (
            f"select {quote_identifier(dimensions[0])}, sum(cast({quote_identifier(numeric[0])} as real)) as total "
            f"from {quote_identifier(table)} group by {quote_identifier(dimensions[0])} "
            f"order by total desc limit {int(limit)}"
        )
        return sql, f"{dimensions[0]} 別に {numeric[0]} の合計を集計しました。"
    if numeric and ("平均" in question or "average" in lowered or "avg" in lowered or "mean" in lowered):
        if dimensions:
            sql = (
                f"select {quote_identifier(dimensions[0])}, avg(cast({quote_identifier(numeric[0])} as real)) as average "
                f"from {quote_identifier(table)} group by {quote_identifier(dimensions[0])} "
                f"order by average desc limit {int(limit)}"
            )
            return sql, f"{dimensions[0]} 別に {numeric[0]} の平均を集計しました。"
        sql = f"select avg(cast({quote_identifier(numeric[0])} as real)) as average from {quote_identifier(table)} limit {int(limit)}"
        return sql, f"{numeric[0]} の平均を集計しました。"
    sql = f"select * from {quote_identifier(table)} limit {int(limit)}"
    return sql, f"{table} の先頭 {limit} 行を返しました。"


def openai_sql(
    question: str,
    conn: sqlite3.Connection,
    dataset_table: str | None = None,
    settings: Settings | None = None,
    limit: int = 200,
) -> str | None:
    effective = effective_openai_settings(settings)
    if not effective.llm_enabled:
        return None
    try:
        from openai import OpenAI

        client = OpenAI(api_key=effective.api_key)
        allowed_tables = {dataset_table} if dataset_table else None
        prompt = (
            "Generate one SQLite SELECT statement only. No markdown. "
            "Respect this schema:\n"
            f"{schema_prompt(conn, allowed_tables)}\n"
            f"Question: {question}\nLimit rows to at most {limit}."
        )
        response = client.responses.create(model=effective.model, input=prompt)
        return response.output_text.strip()
    except Exception:
        return None


def generate_query(
    question: str,
    conn: sqlite3.Connection,
    dataset_table: str | None = None,
    settings: Settings | None = None,
    limit: int = 200,
) -> tuple[str, str]:
    sql = openai_sql(question, conn, dataset_table, settings, limit)
    if sql:
        return validate_select_sql(sql, limit), "LLM generated a read-only SQLite query."
    sql, explanation = fallback_query(question, conn, dataset_table, limit)
    return validate_select_sql(sql, limit), explanation


def run_query(conn: sqlite3.Connection, sql: str) -> list[dict[str, Any]]:
    rows, _ = run_query_with_columns(conn, sql)
    return rows


def _authorizer_for_tables(allowed_tables: set[str], referenced_tables: set[str] | None = None):
    def authorizer(action: int, arg1: str | None, arg2: str | None, dbname: str | None, source: str | None) -> int:
        if action == sqlite3.SQLITE_READ and arg1:
            if referenced_tables is not None:
                referenced_tables.add(arg1)
            if arg1 not in allowed_tables:
                return sqlite3.SQLITE_DENY
        if action not in {sqlite3.SQLITE_SELECT, sqlite3.SQLITE_READ, sqlite3.SQLITE_FUNCTION}:
            return sqlite3.SQLITE_DENY
        return sqlite3.SQLITE_OK

    return authorizer


def run_query_with_columns(
    conn: sqlite3.Connection,
    sql: str,
    allowed_tables: set[str] | None = None,
    max_rows: int | None = None,
) -> tuple[list[dict[str, Any]], list[str]]:
    validated = validate_select_sql(sql)
    referenced_tables: set[str] = set()
    if allowed_tables:
        conn.set_authorizer(_authorizer_for_tables(allowed_tables, referenced_tables))
    try:
        cursor = conn.execute(validated)
        columns = [description[0] for description in cursor.description or []]
        if max_rows is None:
            fetched = cursor.fetchall()
        else:
            fetched = cursor.fetchmany(int(max_rows) + 1)
            if len(fetched) > int(max_rows):
                raise QueryGenerationError(f"Query returned more than {max_rows} rows; lower the SQL LIMIT or request limit")
        rows = [dict(zip(columns, row)) for row in fetched]
        if allowed_tables and not referenced_tables.intersection(allowed_tables):
            raise QueryGenerationError("Query must reference the selected dataset materialized table")
        return rows, columns
    except sqlite3.DatabaseError as exc:
        raise QueryGenerationError(f"Query rejected by SQLite authorizer: {exc}") from exc
    finally:
        if allowed_tables:
            conn.set_authorizer(None)
