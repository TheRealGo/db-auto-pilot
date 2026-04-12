from __future__ import annotations

import json
import re
import sqlite3
from typing import Any

from app.config import Settings

try:
    from openai import OpenAI
    from openai import OpenAIError
except ImportError:  # pragma: no cover
    OpenAI = None
    OpenAIError = Exception


FORBIDDEN_SQL = {"insert", "update", "delete", "drop", "alter", "create", "attach", "pragma"}


def validate_select_sql(sql: str) -> str:
    normalized = sql.strip().rstrip(";")
    lowered = normalized.lower()
    if not normalized:
        raise ValueError("Empty SQL generated.")
    if not (lowered.startswith("select") or lowered.startswith("with")):
        raise ValueError("Only SELECT statements are allowed.")
    if any(re.search(rf"\b{keyword}\b", lowered) for keyword in FORBIDDEN_SQL):
        raise ValueError("Update or DDL statements are not allowed.")
    if ";" in normalized:
        raise ValueError("Only a single SQL statement is allowed.")
    if " limit " not in f" {lowered} ":
        normalized = f"{normalized} LIMIT 1000"
    return normalized


def schema_prompt(tables: list[dict[str, Any]]) -> list[dict[str, Any]]:
    payload = []
    for table in tables:
        payload.append(
            {
                "table_name": table["table_name"],
                "display_name": table["display_name"],
                "columns": table["schema"]["columns"],
            }
        )
    return payload


def heuristic_sql(question: str, tables: list[dict[str, Any]]) -> tuple[str, str]:
    if not tables:
        raise ValueError("No approved tables available for querying.")
    table_name = tables[0]["table_name"]
    columns = tables[0]["schema"]["columns"]
    numeric_columns = [column["name"] for column in columns if column["logical_type"] in {"integer", "number"}]
    text_columns = [column["name"] for column in columns if column["logical_type"] == "text"]
    lowered = question.lower()
    table_label = tables[0]["display_name"]
    if "件数" in question or "count" in lowered:
        return (
            f'SELECT COUNT(*) AS row_count FROM "{table_name}"',
            f"`{table_label}` の件数を集計しました。",
        )
    if ("上位" in question or "top" in lowered) and numeric_columns:
        measure = numeric_columns[0]
        limit_match = re.search(r"(\d+)", question)
        limit = limit_match.group(1) if limit_match else "10"
        return (
            f'SELECT * FROM "{table_name}" ORDER BY "{measure}" DESC LIMIT {limit}',
            f"`{table_label}` を `{measure}` の降順で上位 {limit} 件表示しました。",
        )
    if "合計" in question and numeric_columns:
        measure = numeric_columns[0]
        if "別" in question and text_columns:
            dimension = text_columns[0]
            return (
                f'SELECT "{dimension}", SUM("{measure}") AS total_{measure} FROM "{table_name}" GROUP BY "{dimension}" ORDER BY total_{measure} DESC',
                f"`{table_label}` から `{dimension}` ごとに `{measure}` を合計しました。",
            )
        return (
            f'SELECT SUM("{measure}") AS total_{measure} FROM "{table_name}"',
            f"`{table_label}` から `{measure}` の合計を計算しました。",
        )
    return (
        f'SELECT * FROM "{table_name}" LIMIT 100',
        f"`{table_label}` の先頭100件を表示しています。OpenAI API を設定すると自然言語の意図に沿った SQL 生成が有効になります。",
    )


def openai_sql(settings: Settings, question: str, tables: list[dict[str, Any]]) -> tuple[str, str] | None:
    if not settings.openai_api_key or OpenAI is None:
        return None
    client = OpenAI(api_key=settings.openai_api_key)
    prompt = {
        "question": question,
        "tables": schema_prompt(tables),
        "constraints": [
            "Return JSON only",
            "Only generate a single SELECT or WITH ... SELECT query",
            "Use physical table and column names exactly as provided",
        ],
        "response_shape": {"sql": "string", "explanation": "string"},
    }
    try:
        response = client.responses.create(
            model=settings.openai_model,
            input=[
                {
                    "role": "system",
                    "content": (
                        "You translate user questions into SQLite SELECT queries. "
                        "Return strict JSON with keys sql and explanation."
                    ),
                },
                {"role": "user", "content": json.dumps(prompt, ensure_ascii=False)},
            ],
        )
    except OpenAIError:
        return None
    text = getattr(response, "output_text", "").strip()
    if not text:
        return None
    try:
        payload = json.loads(text)
    except json.JSONDecodeError:
        start = text.find("{")
        end = text.rfind("}")
        if start < 0 or end <= start:
            return None
        payload = json.loads(text[start : end + 1])
    return payload["sql"], payload["explanation"]


def run_query(conn: sqlite3.Connection, sql: str) -> tuple[list[str], list[list[Any]]]:
    cursor = conn.execute(sql)
    columns = [item[0] for item in cursor.description] if cursor.description else []
    rows = [list(row) for row in cursor.fetchall()]
    return columns, rows
