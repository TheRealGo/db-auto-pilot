from __future__ import annotations

import sqlite3
import uuid
from pathlib import Path
from typing import Any

import pandas as pd

from app.services.ingestion import slugify


class MaterializationError(RuntimeError):
    pass


class MaterializationGuardError(MaterializationError):
    pass


class MaterializationTransportError(MaterializationError):
    pass


class MaterializationTimeoutError(MaterializationError):
    pass


class MaterializationExecutionError(MaterializationError):
    pass


PROVENANCE_COLUMNS = ["_source_table", "_source_row_number"]
RESERVED_OUTPUT_COLUMNS = set(PROVENANCE_COLUMNS) | {slugify(column) for column in PROVENANCE_COLUMNS}


def _quote_identifier(identifier: str) -> str:
    return '"' + identifier.replace('"', '""') + '"'


def _table_exists(conn: sqlite3.Connection, table_name: str) -> bool:
    return (
        conn.execute(
            "select 1 from sqlite_master where type = 'table' and name = ?",
            (table_name,),
        ).fetchone()
        is not None
    )


def _publish_staging_table(
    conn: sqlite3.Connection,
    temp_table: str,
    table_name: str,
    backup_table: str | None = None,
) -> None:
    backup = backup_table or f"{table_name}_backup_{uuid.uuid4().hex}"
    has_existing = _table_exists(conn, table_name)
    conn.execute("begin immediate")
    try:
        if has_existing:
            conn.execute(f"alter table {_quote_identifier(table_name)} rename to {_quote_identifier(backup)}")
        conn.execute(f"alter table {_quote_identifier(temp_table)} rename to {_quote_identifier(table_name)}")
        if has_existing:
            conn.execute(f"drop table {_quote_identifier(backup)}")
        conn.commit()
    except Exception:
        conn.rollback()
        raise


def draft_materialization_plan(proposal: dict[str, Any], feedback: str = "") -> dict[str, Any]:
    source_tables = proposal.get("source_tables", [])
    canonical_columns = proposal.get("canonical_columns", [])
    output_columns = [slugify(group.get("canonical_name", "column")) for group in canonical_columns]
    expected_rows = sum(int(table.get("rows", 0)) for table in source_tables if isinstance(table, dict))
    review_required = [
        slugify(group.get("canonical_name", "column"))
        for group in canonical_columns
        if isinstance(group, dict) and group.get("review_required") is True
    ]
    plan = {
        "version": 1,
        "mode": proposal.get("materialization_strategy", {}).get("mode", "union-by-canonical-columns"),
        "canonical_columns": canonical_columns,
        "cleansing_policy": proposal.get("cleansing_policy", []),
        "provenance_columns": PROVENANCE_COLUMNS,
        "expected_output": {
            "mode": "union-by-canonical-columns",
            "estimated_rows": expected_rows,
            "estimated_columns": len(output_columns) + len(PROVENANCE_COLUMNS),
            "columns": output_columns + PROVENANCE_COLUMNS,
        },
        "quality_expectations": [
            "Every output row includes provenance columns for source table and source row number.",
            "Configured cleansing policies are applied before canonical column coalescing.",
            "Materialization fails before publish if row, column, reference, or reserved-column guards are violated.",
        ],
        "risk_notes": [
            f"Review required before accepting mappings for: {', '.join(review_required)}"
        ] if review_required else [],
        "feedback": feedback,
    }
    return plan


def _plan_group_index(plan: dict[str, Any] | None) -> dict[str, dict[str, Any]]:
    groups = plan.get("canonical_columns", []) if isinstance(plan, dict) else []
    if not isinstance(groups, list):
        return {}
    indexed: dict[str, dict[str, Any]] = {}
    for group in groups:
        if not isinstance(group, dict):
            continue
        canonical_name = group.get("canonical_name")
        if isinstance(canonical_name, str) and canonical_name:
            indexed[canonical_name] = group
    return indexed


def _plan_member_set(group: dict[str, Any]) -> set[tuple[str, str]]:
    members = group.get("members", [])
    if not isinstance(members, list):
        return set()
    result = set()
    for member in members:
        if not isinstance(member, dict):
            continue
        table = member.get("table")
        column = member.get("column")
        if isinstance(table, str) and isinstance(column, str):
            result.add((table, column))
    return result


def summarize_materialization_plan_changes(previous: dict[str, Any] | None, current: dict[str, Any]) -> list[str]:
    if previous is None:
        return ["Initial materialization proposal generated."]

    changes: list[str] = []
    if previous.get("feedback", "") != current.get("feedback", ""):
        changes.append("Materialization feedback changed.")
    if previous.get("mode") != current.get("mode"):
        changes.append(f"Changed materialization mode: {previous.get('mode')} -> {current.get('mode')}.")

    previous_groups = _plan_group_index(previous)
    current_groups = _plan_group_index(current)
    for name in sorted(set(current_groups) - set(previous_groups))[:8]:
        changes.append(f"Added output mapping: {name}.")
    for name in sorted(set(previous_groups) - set(current_groups))[:8]:
        changes.append(f"Removed output mapping: {name}.")
    for name in sorted(set(previous_groups) & set(current_groups)):
        if _plan_member_set(previous_groups[name]) != _plan_member_set(current_groups[name]):
            changes.append(f"Changed source members for output mapping: {name}.")

    previous_expected = previous.get("expected_output", {}) if isinstance(previous.get("expected_output"), dict) else {}
    current_expected = current.get("expected_output", {}) if isinstance(current.get("expected_output"), dict) else {}
    for field in ["estimated_rows", "estimated_columns"]:
        if previous_expected.get(field) != current_expected.get(field):
            changes.append(f"Changed expected output {field}: {previous_expected.get(field)} -> {current_expected.get(field)}.")

    if previous.get("retry") != current.get("retry") and current.get("retry"):
        changes.append("Added retry evidence from a failed materialization run.")

    return changes[:20] or ["No structural materialization changes detected."]


def validate_materialization_plan_references(plan: dict[str, Any], frames: dict[str, pd.DataFrame]) -> None:
    known = {table: set(frame.columns) for table, frame in frames.items()}
    output_columns: set[str] = set()

    def check(table: Any, column: Any, context: str) -> None:
        if table not in known:
            raise MaterializationGuardError(f"{context} references unknown table: {table}")
        if column not in known[table]:
            raise MaterializationGuardError(f"{context} references unknown column: {table}.{column}")

    for group in plan.get("canonical_columns", []):
        output = slugify(group.get("canonical_name", "column"))
        if output in RESERVED_OUTPUT_COLUMNS:
            raise MaterializationGuardError(f"canonical_columns output collides with reserved provenance column: {output}")
        if output in output_columns:
            raise MaterializationGuardError(f"canonical_columns contains duplicate output column after normalization: {output}")
        output_columns.add(output)
        for member in group.get("members", []):
            check(member.get("table"), member.get("column"), "canonical_columns")
    for action in plan.get("cleansing_policy", []):
        check(action.get("table"), action.get("column"), "cleansing_policy")


def _validate_reserved_source_columns(frames: dict[str, pd.DataFrame]) -> None:
    for table, frame in frames.items():
        collisions = [column for column in frame.columns if column in PROVENANCE_COLUMNS]
        if collisions:
            raise MaterializationGuardError(
                f"{table} contains reserved provenance column names: {', '.join(collisions)}"
            )


def _apply_cleansing(frame: pd.DataFrame, table: str, policy: list[dict[str, Any]]) -> pd.DataFrame:
    cleaned = frame.copy()
    for action in policy:
        if action.get("table") != table or action.get("column") not in cleaned.columns:
            continue
        column = action["column"]
        if action.get("action") == "trim_whitespace":
            cleaned[column] = cleaned[column].map(lambda v: v.strip() if isinstance(v, str) else v)
        elif action.get("action") == "coalesce_empty_to_null":
            cleaned[column] = cleaned[column].replace({"": None})
    return cleaned


def _canonical_frame(table: str, frame: pd.DataFrame, plan: dict[str, Any]) -> pd.DataFrame:
    cleaned = _apply_cleansing(frame, table, plan.get("cleansing_policy", []))
    data: dict[str, Any] = {}
    for group in plan.get("canonical_columns", []):
        canonical = slugify(group.get("canonical_name", "column"))
        values = None
        for member in group.get("members", []):
            if member.get("table") == table and member.get("column") in cleaned.columns:
                candidate = cleaned[member["column"]]
                values = candidate if values is None else values.combine_first(candidate)
        if values is not None:
            data[canonical] = values
    if not data:
        data = {column: cleaned[column] for column in cleaned.columns}
    result = pd.DataFrame(data)
    result["_source_table"] = table
    result["_source_row_number"] = range(1, len(result) + 1)
    return result


def _materialization_lineage(plan: dict[str, Any], frames: dict[str, pd.DataFrame]) -> list[dict[str, Any]]:
    lineage: list[dict[str, Any]] = []
    groups = plan.get("canonical_columns", [])
    if not groups:
        for table, frame in frames.items():
            for column in frame.columns:
                lineage.append(
                    {
                        "output_column": column,
                        "source_table": table,
                        "source_column": column,
                        "action": "keep",
                    }
                )
        return lineage
    for group in groups:
        output = slugify(group.get("canonical_name", "column"))
        for member in group.get("members", []):
            lineage.append(
                {
                    "output_column": output,
                    "source_table": member.get("table"),
                    "source_column": member.get("column"),
                    "action": group.get("action", "keep"),
                }
            )
    return lineage


def _quality_warnings(unified: pd.DataFrame, plan: dict[str, Any], frames: dict[str, pd.DataFrame]) -> list[dict[str, Any]]:
    warnings: list[dict[str, Any]] = []
    for group in plan.get("canonical_columns", []):
        output = slugify(group.get("canonical_name", "column"))
        mapped_tables = {member.get("table") for member in group.get("members", [])}
        for table in frames:
            if table not in mapped_tables:
                warnings.append(
                    {
                        "severity": "warning",
                        "code": "missing_source_column",
                        "table": table,
                        "output_column": output,
                        "message": f"{table} has no mapped source column for {output}; values will be null for that source.",
                    }
                )
        if output in unified.columns and len(unified) > 0:
            null_ratio = float(unified[output].isna().mean())
            if null_ratio >= 0.5:
                warnings.append(
                    {
                        "severity": "warning",
                        "code": "high_null_ratio",
                        "output_column": output,
                        "null_ratio": null_ratio,
                        "message": f"{output} is null in {null_ratio:.0%} of materialized rows.",
                    }
                )
    return warnings


def materialize_frames(
    database_path: Path,
    dataset_id: str,
    frames: dict[str, pd.DataFrame],
    plan: dict[str, Any],
    max_rows: int | None = None,
    max_columns: int | None = None,
) -> dict[str, Any]:
    if not frames:
        raise MaterializationExecutionError("No source frames available")
    validate_materialization_plan_references(plan, frames)
    _validate_reserved_source_columns(frames)
    table_name = f"dataset_{slugify(dataset_id.replace('-', '_'))}"
    temp_table = f"{table_name}_staging_{uuid.uuid4().hex}"
    unified = pd.concat([_canonical_frame(table, frame, plan) for table, frame in frames.items()], ignore_index=True)
    if max_rows is not None and len(unified) > max_rows:
        raise MaterializationExecutionError(f"Materialization row limit exceeded: {len(unified)} rows > {max_rows}")
    if max_columns is not None and len(unified.columns) > max_columns:
        raise MaterializationExecutionError(f"Materialization column limit exceeded: {len(unified.columns)} columns > {max_columns}")
    database_path.parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(database_path) as conn:
        try:
            unified.to_sql(temp_table, conn, if_exists="fail", index=False)
            conn.commit()
            _publish_staging_table(conn, temp_table, table_name)
        except Exception as exc:
            conn.rollback()
            conn.execute(f"drop table if exists {_quote_identifier(temp_table)}")
            raise MaterializationExecutionError(f"Materialization failed before publishing output table: {exc}") from exc
    return {
        "table_name": table_name,
        "rows": int(len(unified)),
        "columns": list(unified.columns),
        "plan": plan,
        "mode": "merged",
        "lineage": _materialization_lineage(plan, frames),
        "quality_warnings": _quality_warnings(unified, plan, frames),
    }


def generate_materialization_proposal(proposal: dict[str, Any], feedback: str = "") -> dict[str, Any]:
    return draft_materialization_plan(proposal, feedback)


def validate_generated_code(code: str) -> None:
    banned = ["import os", "import subprocess", "open(", "exec(", "eval("]
    if any(token in code for token in banned):
        raise MaterializationGuardError("Generated code contains banned operations")


def execute_materialization_code(*args: Any, **kwargs: Any) -> dict[str, Any]:
    raise MaterializationGuardError("Arbitrary generated code execution is disabled in the production path")


def build_materialization_retry_guidance(error: Exception) -> str:
    return f"Revise mappings or cleansing policy, then retry materialization. Last error: {error}"
