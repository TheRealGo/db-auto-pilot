from __future__ import annotations

import json
import re
import sqlite3
from typing import Any

from app.config import Settings, effective_openai_settings

try:
    from openai import OpenAI
    from openai import OpenAIError
except ImportError:  # pragma: no cover
    OpenAI = None
    OpenAIError = Exception


FORBIDDEN_SQL = {"insert", "update", "delete", "drop", "alter", "create", "attach", "pragma"}


class QueryGenerationError(RuntimeError):
    pass


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
    prompt_tables = []
    for table in tables:
        lineage = table["schema"].get("lineage", [])
        lineage_lookup = {item["column_name"]: item for item in lineage if isinstance(item, dict)}
        prompt_tables.append(
            {
                "table_name": table["table_name"],
                "display_name": table["display_name"],
                "mode": table.get("mode"),
                "columns": [
                    {
                        "name": column["name"],
                        "logical_type": column.get("logical_type", "text"),
                        "is_provenance": column["name"].startswith("_source_"),
                        "source_columns": lineage_lookup.get(column["name"], {}).get("source_columns", []),
                    }
                    for column in table["schema"]["columns"]
                ],
                "query_hints": {
                    "metric_columns": [
                        column["name"]
                        for column in table["schema"]["columns"]
                        if column.get("logical_type") in {"integer", "number"}
                    ],
                    "datetime_columns": [
                        column["name"]
                        for column in table["schema"]["columns"]
                        if column.get("logical_type") in {"date", "datetime"}
                    ],
                    "dimension_columns": [
                        column["name"]
                        for column in table["schema"]["columns"]
                        if not column["name"].startswith("_source_") and column.get("logical_type") not in {"integer", "number"}
                    ],
                },
            }
        )
    return prompt_tables


def openai_sql(settings: Settings, question: str, tables: list[dict[str, Any]]) -> tuple[str, str]:
    openai_settings = effective_openai_settings(settings)
    if not openai_settings.api_key:
        raise QueryGenerationError("OpenAI API key is not configured.")
    if OpenAI is None:
        raise QueryGenerationError("OpenAI SDK is not installed.")

    client_options: dict[str, Any] = {"api_key": openai_settings.api_key}
    if openai_settings.endpoint:
        client_options["base_url"] = openai_settings.endpoint
    try:
        client = OpenAI(**client_options)
    except Exception as exc:  # pragma: no cover
        raise QueryGenerationError(f"OpenAI client initialization failed: {exc}") from exc

    prompt = {
        "question": question,
        "tables": schema_prompt(tables),
        "constraints": [
            "Return JSON only",
            "Only generate a single SELECT or WITH ... SELECT query",
            "Use physical table and column names exactly as provided",
            "Prefer non-provenance business columns unless the user explicitly asks for source tracking",
            "Use metric/date hints to choose aggregate and time filters",
        ],
        "response_shape": {"sql": "string", "explanation": "string"},
    }
    try:
        response = client.chat.completions.create(
            model=openai_settings.model,
            messages=[
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
    except OpenAIError as exc:
        raise QueryGenerationError(f"OpenAI query generation failed: {exc}") from exc
    except Exception as exc:  # pragma: no cover
        raise QueryGenerationError(f"Unexpected OpenAI query error: {exc}") from exc

    message = response.choices[0].message if response.choices else None
    text = (message.content or "").strip() if message else ""
    if not text:
        raise QueryGenerationError("OpenAI returned an empty response.")
    try:
        payload = json.loads(text)
    except json.JSONDecodeError:
        start = text.find("{")
        end = text.rfind("}")
        if start < 0 or end <= start:
            raise QueryGenerationError("OpenAI response was not valid JSON.")
        payload = json.loads(text[start : end + 1])
    if "sql" not in payload or "explanation" not in payload:
        raise QueryGenerationError("OpenAI response did not include sql/explanation.")
    return payload["sql"], payload["explanation"]


def generate_query(settings: Settings, question: str, tables: list[dict[str, Any]]) -> dict[str, Any]:
    sql, explanation = openai_sql(settings, question, tables)
    return {
        "sql": sql,
        "explanation": explanation,
        "generator": "openai",
        "warning": None,
    }


def run_query(conn: sqlite3.Connection, sql: str) -> tuple[list[str], list[list[Any]]]:
    cursor = conn.execute(sql)
    columns = [item[0] for item in cursor.description] if cursor.description else []
    rows = [list(row) for row in cursor.fetchall()]
    return columns, rows
