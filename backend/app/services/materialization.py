from __future__ import annotations

import ast
import base64
import builtins
import copy
import json
import pickle
import re
import subprocess
import sys
import unicodedata
import warnings
from pathlib import Path
from typing import Any

import pandas as pd

from app.config import Settings, effective_openai_settings
from app.services.ingestion import slugify
from app.services.proposals import make_json_safe

try:
    from openai import OpenAI
    from openai import OpenAIError
except ImportError:  # pragma: no cover
    OpenAI = None
    OpenAIError = Exception


class MaterializationError(RuntimeError):
    pass


class MaterializationGuardError(MaterializationError):
    def __init__(self, message: str, summary: dict[str, Any] | None = None):
        super().__init__(message)
        self.summary = summary or {}


class MaterializationTransportError(MaterializationError):
    pass


class MaterializationTimeoutError(MaterializationError):
    pass


class MaterializationExecutionError(MaterializationError):
    pass


ALLOWED_IMPORTS = {
    "collections",
    "itertools",
    "json",
    "math",
    "numpy",
    "pandas",
    "re",
    "statistics",
}
BANNED_IMPORT_PREFIXES = {
    "builtins",
    "ftplib",
    "glob",
    "http",
    "importlib",
    "os",
    "pathlib",
    "pickle",
    "requests",
    "shutil",
    "socket",
    "subprocess",
    "sys",
    "tempfile",
    "urllib",
}
BANNED_CALLS = {
    "open",
    "eval",
    "exec",
    "compile",
    "__import__",
    "input",
    "breakpoint",
    "globals",
    "locals",
    "vars",
    "dir",
    "getattr",
    "setattr",
    "delattr",
}
BANNED_NAMES = {"__builtins__", "__loader__", "__spec__", "__package__"}
BANNED_ATTRS = {
    "system",
    "popen",
    "spawn",
    "remove",
    "unlink",
    "rmdir",
    "mkdir",
    "makedirs",
    "rename",
    "replace",
    "write_text",
    "write_bytes",
    "open",
    "load_module",
    "walk",
    "iterdir",
    "chmod",
    "chown",
    "symlink",
    "hardlink_to",
    "rglob",
    "glob",
    "read_text",
    "read_bytes",
    "resolve",
    "absolute",
    "cwd",
    "home",
    "exec_module",
    "fork",
    "kill",
    "startfile",
    "connect",
    "send",
    "recv",
    "request",
    "urlopen",
    "mount",
}
PROVENANCE_COLUMNS = {"_source_row_index", "_source_sheet", "_source_file", "_source_table"}
MAX_MERGED_TABLES = 12
MAX_COLUMNS_PER_TABLE = 250
MAX_ROWS_PER_TABLE = 100000
MATERIALIZATION_TIMEOUT_SECONDS = 8
MATERIALIZATION_MAX_STDIO_BYTES = 2_000_000
MATERIALIZATION_MEMORY_LIMIT_BYTES = 512 * 1024 * 1024
MATERIALIZATION_CPU_LIMIT_SECONDS = 6
SUPPORTED_NORMALIZATION_ACTIONS = {
    "coalesce_empty_to_null",
    "lowercase",
    "map_values",
    "normalize_text",
    "normalize_whitespace",
    "parse_date",
    "parse_number",
    "strip_non_numeric",
    "trim_whitespace",
    "uppercase",
}
WARNING_ACTION_HINTS = {
    "null_ratio_increased": ["coalesce_empty_to_null", "trim_whitespace"],
    "low_numeric_parse_ratio": ["coalesce_empty_to_null", "strip_non_numeric", "parse_number"],
    "low_datetime_parse_ratio": ["coalesce_empty_to_null", "normalize_text", "parse_date"],
    "distinct_ratio_dropped": ["map_values"],
}


def _candidate_text_forms(value: str) -> set[str]:
    normalized = _normalize_text_value(value)
    if not normalized:
        return set()
    compact = re.sub(r"[\s\-_./]+", "", normalized)
    alnum = re.sub(r"[^0-9A-Za-z一-龠ぁ-んァ-ヶ]", "", compact)
    return {
        normalized,
        normalized.casefold(),
        compact,
        compact.casefold(),
        alnum,
        alnum.casefold(),
    }


def _suggest_value_mapping(column: dict[str, Any]) -> dict[str, str]:
    grouped: dict[str, set[str]] = {}
    for source in column.get("source_columns", []):
        profile = source.get("profile", {})
        for raw_value in profile.get("unique_samples", []):
            if raw_value is None:
                continue
            text = str(raw_value)
            for form in _candidate_text_forms(text):
                if form:
                    grouped.setdefault(form, set()).add(text)

    mapping: dict[str, str] = {}
    for variants in grouped.values():
        if len(variants) < 2:
            continue
        representative = min(
            variants,
            key=lambda item: (len(_normalize_text_value(item) or item), len(item), item.casefold()),
        )
        for variant in variants:
            if variant != representative:
                mapping[variant] = representative
    return dict(sorted(mapping.items()))


def build_deterministic_materialization_code(plan: dict[str, Any]) -> str:
    components = [
        {
            "component_id": component.get("component_id"),
            "display_name": component.get("display_name"),
            "physical_name": component.get("physical_name"),
            "source_tables": component.get("source_tables", []),
            "columns": [
                {
                    "name": column.get("name"),
                    "status": column.get("status"),
                    "source_columns": [
                        {
                            "source_table": source.get("source_table"),
                            "source_column": source.get("source_column"),
                            "display_name": source.get("display_name"),
                        }
                        for source in column.get("source_columns", [])
                    ],
                }
                for column in component.get("columns", [])
            ],
        }
        for component in plan.get("components", [])
    ]
    payload = json.dumps(components, ensure_ascii=False, indent=2)
    return f"""import pandas as pd

COMPONENTS = {payload}
PROVENANCE_COLUMNS = ["_source_row_index", "_source_sheet", "_source_file", "_source_table"]

merged_tables = []
lineage_items = []
execution_notes = ["backend deterministic assembly"]

for component in COMPONENTS:
    frames = []
    for source_table in component["source_tables"]:
        source_df = source_frames.get(source_table)
        if source_df is None:
            continue
        output_df = pd.DataFrame(index=source_df.index)
        for column in component["columns"]:
            output_df[column["name"]] = None
            matched_source = next(
                (
                    source
                    for source in column["source_columns"]
                    if source["source_table"] == source_table and source["source_column"] in source_df.columns
                ),
                None,
            )
            if matched_source is not None:
                output_df[column["name"]] = source_df[matched_source["source_column"]]
            lineage_items.append(
                {{
                    "table_name": component["display_name"],
                    "column_name": column["name"],
                    "source_columns": column["source_columns"],
                    "status": column["status"],
                }}
            )
        output_df["_source_row_index"] = source_df["_row_index"] if "_row_index" in source_df.columns else source_df.index
        output_df["_source_sheet"] = source_df["_source_sheet"] if "_source_sheet" in source_df.columns else None
        output_df["_source_file"] = source_df["_source_file"] if "_source_file" in source_df.columns else None
        output_df["_source_table"] = source_table
        frames.append(output_df)

    if frames:
        merged_df = pd.concat(frames, ignore_index=True)
    else:
        merged_df = pd.DataFrame(columns=[column["name"] for column in component["columns"]] + PROVENANCE_COLUMNS)

    merged_tables.append(
        {{
            "component_id": component["component_id"],
            "display_name": component["display_name"],
            "physical_name": component["physical_name"],
            "dataframe": merged_df,
        }}
    )

result = {{
    "merged_tables": merged_tables,
    "lineage_items": lineage_items,
    "execution_notes": execution_notes,
}}
"""


def _openai_client(settings: Settings) -> OpenAI:
    openai_settings = effective_openai_settings(settings)
    if not openai_settings.api_key:
        raise MaterializationError("OpenAI API key is not configured.")
    if OpenAI is None:
        raise MaterializationError("OpenAI SDK is not installed.")
    client_options: dict[str, Any] = {"api_key": openai_settings.api_key}
    if openai_settings.endpoint:
        client_options["base_url"] = openai_settings.endpoint
    try:
        return OpenAI(**client_options)
    except Exception as exc:  # pragma: no cover
        raise MaterializationError(f"OpenAI client initialization failed: {exc}") from exc


def generate_materialization_proposal(
    settings: Settings,
    dataset_name: str,
    plan: dict[str, Any],
    retry_context: dict[str, Any] | None = None,
) -> dict[str, Any]:
    client = _openai_client(settings)
    system_prompt = (
        "You design a materialization proposal for merged pandas DataFrames from an approved integration plan. "
        "Return JSON only. "
        "Include human-reviewable normalization decisions, expected outputs, quality expectations, and risk notes. "
        "Prefer deterministic normalization and conservative retry guidance over creative transformations. "
        "If retry_context includes column_patches, keep unrelated columns stable and focus edits on the listed columns."
    )
    payload = {
        "dataset_name": dataset_name,
        "materialization_plan": make_json_safe(plan),
        "retry_context": make_json_safe(retry_context) if retry_context else None,
        "response_shape": {
            "summary": "string",
            "normalization_decisions": [
                {
                    "component_id": "component_1",
                    "column_name": "string",
                    "actions": ["trim_whitespace"],
                    "config": {"mapping": {"dept": "department"}},
                    "reason": "string",
                }
            ],
            "transformation_notes": ["string"],
            "risk_notes": ["string"],
            "expected_outputs": ["string"],
            "quality_expectations": ["string"],
        },
    }
    try:
        response = client.chat.completions.create(
            model=effective_openai_settings(settings).model,
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": json.dumps(payload, ensure_ascii=False)},
            ],
        )
    except OpenAIError as exc:
        raise MaterializationError(f"OpenAI materialization proposal generation failed: {exc}") from exc
    except Exception as exc:  # pragma: no cover
        raise MaterializationError(f"Unexpected OpenAI materialization proposal error: {exc}") from exc
    message = response.choices[0].message if response.choices else None
    text = (message.content or "").strip() if message else ""
    if not text:
        raise MaterializationError("OpenAI returned an empty materialization proposal response.")
    try:
        parsed = json.loads(text)
    except json.JSONDecodeError:
        start = text.find("{")
        end = text.rfind("}")
        if start >= 0 and end > start:
            parsed = json.loads(text[start : end + 1])
        else:
            raise MaterializationError("OpenAI materialization proposal response was not valid JSON.")
    validated = validate_materialization_proposal_payload(parsed, plan)
    return {
        "summary": validated["summary"],
        "normalization_decisions": validated["normalization_decisions"],
        "transformation_notes": validated["transformation_notes"],
        "risk_notes": validated["risk_notes"],
        "expected_outputs": validated["expected_outputs"],
        "quality_expectations": validated["quality_expectations"],
        "generated_code": build_deterministic_materialization_code(plan),
        "plan": plan,
        "retry_context": retry_context,
    }


def validate_generated_code(generated_code: str) -> dict[str, Any]:
    try:
        tree = ast.parse(generated_code)
    except SyntaxError as exc:
        raise MaterializationError(f"Generated materialization code is not valid Python: {exc}") from exc

    imports: list[str] = []
    violations: list[dict[str, str]] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                imports.append(alias.name)
                if alias.name.split(".")[0] in BANNED_IMPORT_PREFIXES or alias.name not in ALLOWED_IMPORTS:
                    violations.append({"type": "import", "value": alias.name})
        elif isinstance(node, ast.ImportFrom):
            module = node.module or ""
            imports.append(module)
            if module.split(".")[0] in BANNED_IMPORT_PREFIXES or module not in ALLOWED_IMPORTS:
                violations.append({"type": "import", "value": module})
        elif isinstance(node, ast.Call):
            if isinstance(node.func, ast.Name) and node.func.id in BANNED_CALLS:
                violations.append({"type": "call", "value": node.func.id})
            if isinstance(node.func, ast.Attribute) and node.func.attr in BANNED_ATTRS:
                violations.append({"type": "attribute", "value": node.func.attr})
        elif isinstance(node, ast.Name):
            if node.id in BANNED_NAMES:
                violations.append({"type": "name", "value": node.id})
        elif isinstance(node, ast.Attribute):
            if node.attr in BANNED_ATTRS or node.attr.startswith("__"):
                violations.append({"type": "attribute", "value": node.attr})
        elif isinstance(node, ast.Constant):
            if isinstance(node.value, str) and node.value in BANNED_ATTRS:
                violations.append({"type": "string", "value": node.value})

    if violations:
        summary = {
            "status": "failed",
            "imports": sorted(set(imports)),
            "violations": sorted(violations, key=lambda item: (item["type"], item["value"])),
        }
        raise MaterializationGuardError(
            "Generated materialization code violated safety rules: "
            + ", ".join(f'{item["type"]}:{item["value"]}' for item in summary["violations"]),
            summary,
        )
    return {
        "status": "passed",
        "imports": sorted(set(imports)),
        "violations": [],
    }


def _component_lookup(plan: dict[str, Any]) -> dict[str, dict[str, Any]]:
    return {
        component["component_id"]: component
        for component in plan.get("components", [])
        if isinstance(component, dict) and "component_id" in component
    }


def _component_column_lookup(plan: dict[str, Any]) -> dict[tuple[str, str], dict[str, Any]]:
    lookup: dict[tuple[str, str], dict[str, Any]] = {}
    for component in plan.get("components", []):
        if not isinstance(component, dict):
            continue
        component_id = str(component.get("component_id", ""))
        for column in component.get("columns", []):
            if not isinstance(column, dict):
                continue
            column_name = str(column.get("name", ""))
            if component_id and column_name:
                lookup[(component_id, column_name)] = column
    return lookup


def _default_actions_for_column(column: dict[str, Any]) -> list[str]:
    logical_type = str(column.get("logical_type", "text"))
    actions: list[str] = []
    if logical_type in {"text", "string"}:
        actions.extend(["coalesce_empty_to_null", "normalize_text"])
    elif logical_type in {"number", "integer", "float"}:
        actions.extend(["coalesce_empty_to_null", "strip_non_numeric", "parse_number"])
    elif logical_type in {"date", "datetime", "timestamp"}:
        actions.extend(["coalesce_empty_to_null", "normalize_text", "parse_date"])
    else:
        actions.extend(["coalesce_empty_to_null", "trim_whitespace"])
    return actions


def _default_config_for_action(action: str) -> dict[str, Any]:
    if action == "parse_number":
        return {"percent_mode": "auto"}
    if action == "parse_date":
        return {"dayfirst": False}
    if action == "map_values":
        return {"mapping": {}, "casefold": True}
    return {}


def _warning_detail(code: str) -> dict[str, Any]:
    return {
        "code": code,
        "severity": "warning",
        "suggested_actions": WARNING_ACTION_HINTS.get(code, []),
    }


def validate_materialization_proposal_payload(parsed: dict[str, Any], plan: dict[str, Any]) -> dict[str, Any]:
    required_keys = {
        "summary",
        "normalization_decisions",
        "transformation_notes",
        "risk_notes",
        "expected_outputs",
        "quality_expectations",
    }
    missing = sorted(required_keys - set(parsed))
    if missing:
        raise MaterializationError(
            "OpenAI materialization proposal response did not include required fields: "
            + ", ".join(missing)
        )
    suggested_mapping_lookup = {
        (component["component_id"], column["name"]): column.get("suggested_value_mapping", {})
        for component in plan.get("components", [])
        if isinstance(component, dict)
        for column in component.get("columns", [])
        if isinstance(column, dict)
    }
    component_lookup = _component_lookup(plan)
    column_lookup = _component_column_lookup(plan)
    decisions: list[dict[str, Any]] = []
    for item in parsed.get("normalization_decisions", []):
        if not isinstance(item, dict):
            raise MaterializationError("OpenAI materialization proposal normalization_decisions must contain objects.")
        component_id = str(item.get("component_id", ""))
        column_name = str(item.get("column_name", ""))
        if component_id not in component_lookup:
            raise MaterializationError(
                f"OpenAI materialization proposal referenced unknown component_id: {component_id}."
            )
        if (component_id, column_name) not in column_lookup:
            raise MaterializationError(
                f"OpenAI materialization proposal referenced unknown column {column_name} for {component_id}."
            )
        actions = [str(action) for action in item.get("actions", [])]
        if not actions:
            actions = _default_actions_for_column(column_lookup[(component_id, column_name)])
        unsupported = sorted(set(actions) - SUPPORTED_NORMALIZATION_ACTIONS)
        if unsupported:
            raise MaterializationError(
                "OpenAI materialization proposal referenced unsupported normalization actions: "
                + ", ".join(unsupported)
            )
        config = item.get("config", {})
        if config is None:
            config = {}
        if not isinstance(config, dict):
            raise MaterializationError("OpenAI materialization proposal normalization_decisions.config must be an object.")
        if "map_values" in actions and not config.get("mapping"):
            suggested_mapping = suggested_mapping_lookup.get((component_id, column_name), {})
            if suggested_mapping:
                config = {**config, "mapping": suggested_mapping, "casefold": True}
        decisions.append(
            {
                "component_id": component_id,
                "column_name": column_name,
                "actions": actions,
                "config": {str(key): make_json_safe(value) for key, value in config.items()},
                "reason": str(item.get("reason", "")),
            }
        )

    existing_pairs = {(item["component_id"], item["column_name"]) for item in decisions}
    for pair, column in column_lookup.items():
        if pair in existing_pairs:
            continue
        decisions.append(
            {
                "component_id": pair[0],
                "column_name": pair[1],
                "actions": _default_actions_for_column(column),
                "config": {},
                "reason": "default normalization based on logical type",
            }
        )

    return {
        "summary": str(parsed["summary"]),
        "normalization_decisions": sorted(
            decisions,
            key=lambda item: (item["component_id"], item["column_name"]),
        ),
        "transformation_notes": [str(item) for item in parsed.get("transformation_notes", [])],
        "risk_notes": [str(item) for item in parsed.get("risk_notes", [])],
        "expected_outputs": [str(item) for item in parsed.get("expected_outputs", [])],
        "quality_expectations": [str(item) for item in parsed.get("quality_expectations", [])],
    }


def _normalize_text_value(value: Any) -> Any:
    if value is None or pd.isna(value):
        return value
    text = unicodedata.normalize("NFKC", str(value))
    text = text.strip()
    text = re.sub(r"\s+", " ", text)
    return text


def _stringify_non_null(series: pd.Series) -> pd.Series:
    return series.map(lambda value: None if pd.isna(value) else str(value))


def _apply_normalization_action(series: pd.Series, action: str, config: dict[str, Any]) -> pd.Series:
    updated = series.copy()
    if action == "coalesce_empty_to_null":
        as_text = _stringify_non_null(updated)
        return as_text.map(lambda value: None if value is None or not value.strip() else value)
    if action == "trim_whitespace":
        as_text = _stringify_non_null(updated)
        return as_text.map(lambda value: None if value is None else value.strip())
    if action == "normalize_whitespace":
        as_text = _stringify_non_null(updated)
        return as_text.map(lambda value: None if value is None else re.sub(r"\s+", " ", value))
    if action == "normalize_text":
        as_text = _stringify_non_null(updated)
        return as_text.map(_normalize_text_value)
    if action == "lowercase":
        as_text = _stringify_non_null(updated)
        return as_text.map(lambda value: None if value is None else value.lower())
    if action == "uppercase":
        as_text = _stringify_non_null(updated)
        return as_text.map(lambda value: None if value is None else value.upper())
    if action == "strip_non_numeric":
        as_text = _stringify_non_null(updated)
        return as_text.map(
            lambda value: None if value is None else re.sub(r"[^0-9eE+\-.,%]", "", value)
        )
    if action == "parse_number":
        percent_mode = str(config.get("percent_mode", "auto"))

        def _parse_number(value: Any) -> Any:
            if value is None or pd.isna(value):
                return None
            if isinstance(value, (int, float)) and not isinstance(value, bool):
                return value
            text = str(value).strip()
            if not text:
                return None
            percent = text.endswith("%")
            text = text.replace("%", "").replace(",", "")
            try:
                parsed = float(text)
            except ValueError:
                return value
            if percent and percent_mode in {"auto", "always"}:
                parsed = parsed / 100.0
            return parsed

        return updated.map(_parse_number)
    if action == "parse_date":
        dayfirst = bool(config.get("dayfirst", False))
        parsed = pd.to_datetime(updated, errors="coerce", dayfirst=dayfirst)
        original = updated.where(updated.notna(), None)
        return parsed.where(parsed.notna(), original)
    if action == "map_values":
        mapping = config.get("mapping", {})
        if not isinstance(mapping, dict):
            return updated
        casefold = bool(config.get("casefold", True))

        def _map_value(value: Any) -> Any:
            if value is None or pd.isna(value):
                return value
            key = str(value)
            if key in mapping:
                return mapping[key]
            if casefold:
                folded = key.casefold()
                for source, target in mapping.items():
                    if str(source).casefold() == folded:
                        return target
            return value

        return updated.map(_map_value)
    return updated


def _series_quality_profile(series: pd.Series) -> dict[str, Any]:
    total_count = int(len(series))
    non_null = series.dropna()
    if total_count == 0:
        return {
            "row_count": 0,
            "null_ratio": 0.0,
            "distinct_ratio": 0.0,
            "numeric_ratio": 0.0,
            "datetime_ratio": 0.0,
        }
    non_null_string = non_null.astype(str) if not non_null.empty else pd.Series(dtype=str)
    numeric_ratio = round(float(pd.to_numeric(non_null, errors="coerce").notna().mean()), 3) if not non_null.empty else 0.0
    if not non_null.empty:
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", UserWarning)
            datetime_ratio = round(float(pd.to_datetime(non_null, errors="coerce").notna().mean()), 3)
    else:
        datetime_ratio = 0.0
    distinct_ratio = (
        round(float(non_null_string.nunique()) / float(len(non_null_string)), 3)
        if not non_null_string.empty
        else 0.0
    )
    return {
        "row_count": total_count,
        "null_ratio": round(float(series.isna().mean()), 3),
        "distinct_ratio": distinct_ratio,
        "numeric_ratio": numeric_ratio,
        "datetime_ratio": datetime_ratio,
    }


def apply_normalization_decisions(
    result: dict[str, Any],
    plan: dict[str, Any],
    normalization_decisions: list[dict[str, Any]] | None,
) -> tuple[dict[str, Any], dict[str, Any], list[str]]:
    if not normalization_decisions:
        return copy.deepcopy(result), {"status": "skipped", "table_summaries": [], "warning_count": 0}, []

    normalized = copy.deepcopy(result)
    decision_lookup = {
        (item["component_id"], item["column_name"]): item
        for item in normalization_decisions
        if isinstance(item, dict) and item.get("component_id") and item.get("column_name")
    }
    component_columns = _component_column_lookup(plan)
    table_summaries: list[dict[str, Any]] = []
    warnings: list[str] = []

    for merged_table in normalized.get("merged_tables", []):
        if not isinstance(merged_table, dict):
            continue
        dataframe = merged_table.get("dataframe")
        component_id = merged_table.get("component_id")
        if not isinstance(dataframe, pd.DataFrame) or not component_id:
            continue
        column_summaries: list[dict[str, Any]] = []
        for column in dataframe.columns:
            if column in PROVENANCE_COLUMNS:
                continue
            decision = decision_lookup.get((component_id, column))
            plan_column = component_columns.get((component_id, column), {})
            if decision is None:
                decision = {
                    "component_id": component_id,
                    "column_name": column,
                    "actions": _default_actions_for_column(plan_column),
                    "config": {},
                    "reason": "default normalization based on logical type",
                }
            before = _series_quality_profile(dataframe[column])
            updated = dataframe[column]
            for action in decision.get("actions", []):
                updated = _apply_normalization_action(updated, action, decision.get("config", {}))
            dataframe[column] = updated
            after = _series_quality_profile(dataframe[column])

            column_warnings: list[str] = []
            logical_type = str(plan_column.get("logical_type", "text"))
            if after["null_ratio"] - before["null_ratio"] > 0.2:
                column_warnings.append("null_ratio_increased")
            if logical_type in {"number", "integer", "float"} and after["numeric_ratio"] < 0.8 and after["row_count"] > 0:
                column_warnings.append("low_numeric_parse_ratio")
            if logical_type in {"date", "datetime", "timestamp"} and after["datetime_ratio"] < 0.8 and after["row_count"] > 0:
                column_warnings.append("low_datetime_parse_ratio")
            if before["distinct_ratio"] - after["distinct_ratio"] > 0.5 and "map_values" not in decision.get("actions", []):
                column_warnings.append("distinct_ratio_dropped")

            for item in column_warnings:
                warnings.append(f'{merged_table.get("display_name", component_id)}.{column}: {item}')

            column_summaries.append(
                {
                    "column_name": column,
                    "logical_type": logical_type,
                    "actions": [str(item) for item in decision.get("actions", [])],
                    "before": before,
                    "after": after,
                    "warnings": column_warnings,
                    "warning_details": [_warning_detail(item) for item in column_warnings],
                }
            )

        table_summaries.append(
            {
                "component_id": component_id,
                "display_name": merged_table.get("display_name", component_id),
                "column_summaries": column_summaries,
            }
        )

    return (
        normalized,
        {
            "status": "completed",
            "table_summaries": table_summaries,
            "warning_count": len(warnings),
            "warning_catalog": [
                _warning_detail(code)
                for code in sorted({warning.split(": ", 1)[1] for warning in warnings if ": " in warning})
            ],
        },
        warnings,
    )


def repair_materialization_result(result: dict[str, Any], plan: dict[str, Any]) -> tuple[dict[str, Any], dict[str, Any]]:
    repaired = copy.deepcopy(result)
    component_lookup = _component_lookup(plan)
    repair_summary = {
        "status": "unchanged",
        "applied_repairs": [],
    }

    if not isinstance(repaired.get("execution_notes"), list):
        repaired["execution_notes"] = [] if repaired.get("execution_notes") is None else [str(repaired["execution_notes"])]
        repair_summary["applied_repairs"].append("normalized_execution_notes")

    merged_tables = repaired.get("merged_tables")
    if isinstance(merged_tables, list):
        for item in merged_tables:
            if not isinstance(item, dict):
                continue
            component = component_lookup.get(item.get("component_id", ""))
            if component and not item.get("display_name"):
                item["display_name"] = component["display_name"]
                repair_summary["applied_repairs"].append("filled_display_name")
            if component and not item.get("physical_name"):
                item["physical_name"] = component["physical_name"]
                repair_summary["applied_repairs"].append("filled_physical_name")

    lineage_items = repaired.get("lineage_items")
    if isinstance(lineage_items, list):
        for item in lineage_items:
            if not isinstance(item, dict):
                continue
            if "status" not in item:
                item["status"] = "merge"
                repair_summary["applied_repairs"].append("filled_lineage_status")
            if "source_columns" not in item or item["source_columns"] is None:
                item["source_columns"] = []
                repair_summary["applied_repairs"].append("filled_source_columns")

    if repair_summary["applied_repairs"]:
        repair_summary["status"] = "repaired"
        repair_summary["applied_repairs"] = sorted(set(repair_summary["applied_repairs"]))
    return repaired, repair_summary


def validate_materialization_result(result: dict[str, Any], plan: dict[str, Any]) -> dict[str, Any]:
    merged_tables = result.get("merged_tables")
    lineage_items = result.get("lineage_items")
    execution_notes = result.get("execution_notes", [])
    if not isinstance(merged_tables, list):
        raise MaterializationExecutionError("Generated materialization code did not return merged_tables as a list.")
    if not isinstance(lineage_items, list):
        raise MaterializationExecutionError("Generated materialization code did not return lineage_items as a list.")
    if not isinstance(execution_notes, list):
        raise MaterializationExecutionError("Generated materialization code did not return execution_notes as a list.")
    if len(merged_tables) > MAX_MERGED_TABLES:
        raise MaterializationExecutionError("Generated materialization code returned too many merged tables.")

    allowed_components = set(_component_lookup(plan))
    resource_summary = {
        "merged_table_count": len(merged_tables),
        "max_rows_seen": 0,
        "max_columns_seen": 0,
    }
    for merged_table in merged_tables:
        if not isinstance(merged_table, dict):
            raise MaterializationExecutionError("Generated materialization code returned a non-dict merged table.")
        missing = {"component_id", "display_name", "physical_name", "dataframe"} - set(merged_table)
        if missing:
            raise MaterializationExecutionError(
                "Generated materialization code returned a merged table missing keys: "
                + ", ".join(sorted(missing))
            )
        if merged_table["component_id"] not in allowed_components:
            raise MaterializationExecutionError("Generated materialization code returned an unknown component_id.")
        dataframe = merged_table["dataframe"]
        if not isinstance(dataframe, pd.DataFrame):
            raise MaterializationExecutionError(
                "Generated materialization code did not return dataframe values as pandas DataFrames."
            )
        if not PROVENANCE_COLUMNS <= set(dataframe.columns):
            raise MaterializationExecutionError("Generated materialization code did not include required provenance columns.")
        if len(dataframe.columns) > MAX_COLUMNS_PER_TABLE:
            raise MaterializationExecutionError("Generated materialization code returned too many columns in a merged table.")
        if len(dataframe.index) > MAX_ROWS_PER_TABLE:
            raise MaterializationExecutionError("Generated materialization code returned too many rows in a merged table.")
        resource_summary["max_rows_seen"] = max(resource_summary["max_rows_seen"], int(len(dataframe.index)))
        resource_summary["max_columns_seen"] = max(resource_summary["max_columns_seen"], int(len(dataframe.columns)))

    for item in lineage_items:
        if not isinstance(item, dict):
            raise MaterializationExecutionError("Generated materialization code returned a non-dict lineage item.")
        missing = {"table_name", "column_name", "source_columns", "status"} - set(item)
        if missing:
            raise MaterializationExecutionError(
                "Generated materialization code returned a lineage item missing keys: "
                + ", ".join(sorted(missing))
            )
        if not isinstance(item["source_columns"], list):
            raise MaterializationExecutionError(
                "Generated materialization code returned source_columns that is not a list."
            )
    return resource_summary


def _serialize_frame(frame: pd.DataFrame) -> str:
    return base64.b64encode(pickle.dumps(frame)).decode("ascii")


def _deserialize_frame(serialized_frame: str) -> pd.DataFrame:
    return pickle.loads(base64.b64decode(serialized_frame.encode("ascii")))


def execute_materialization_code(
    generated_code: str,
    source_frames: dict[str, pd.DataFrame],
    plan: dict[str, Any],
    guard_summary: dict[str, Any] | None = None,
    normalization_decisions: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    payload = {
        "generated_code": generated_code,
        "plan": make_json_safe(plan),
        "source_frames": {name: _serialize_frame(frame.copy(deep=True)) for name, frame in source_frames.items()},
        "memory_limit_bytes": MATERIALIZATION_MEMORY_LIMIT_BYTES,
        "cpu_limit_seconds": MATERIALIZATION_CPU_LIMIT_SECONDS,
    }
    command = [sys.executable, "-m", "app.services.materialization_runner"]
    try:
        completed = subprocess.run(
            command,
            cwd=str(Path(__file__).resolve().parents[2]),
            input=json.dumps(payload, ensure_ascii=False),
            capture_output=True,
            text=True,
            timeout=MATERIALIZATION_TIMEOUT_SECONDS,
        )
    except subprocess.TimeoutExpired as exc:
        raise MaterializationTimeoutError(
            f"Generated materialization code exceeded the {MATERIALIZATION_TIMEOUT_SECONDS}s timeout."
        ) from exc

    if len(completed.stdout.encode("utf-8")) > MATERIALIZATION_MAX_STDIO_BYTES:
        raise MaterializationTransportError("Materialization runner returned too much output.")
    if len(completed.stderr.encode("utf-8")) > MATERIALIZATION_MAX_STDIO_BYTES:
        raise MaterializationTransportError("Materialization runner returned too much error output.")
    if completed.returncode != 0:
        stderr = completed.stderr.strip()
        if not stderr:
            stderr = "Materialization runner exited with a non-zero status."
        raise MaterializationTransportError(stderr)

    try:
        runner_result = json.loads(completed.stdout)
    except json.JSONDecodeError as exc:
        raise MaterializationTransportError("Materialization runner did not return valid JSON.") from exc
    if runner_result.get("status") != "ok":
        stage = runner_result.get("error_stage", "execution")
        message = runner_result.get("error", "Materialization runner failed.")
        if stage == "guard":
            raise MaterializationGuardError(message, runner_result.get("guard_summary"))
        if stage == "timeout":
            raise MaterializationTimeoutError(message)
        raise MaterializationExecutionError(message)

    merged_tables = []
    for item in runner_result.get("merged_tables", []):
        merged_tables.append(
            {
                **item,
                "dataframe": _deserialize_frame(item["dataframe"]),
            }
        )
    result = {
        "merged_tables": merged_tables,
        "lineage_items": runner_result.get("lineage_items", []),
        "execution_notes": runner_result.get("execution_notes", []),
    }
    repaired_result, repair_summary = repair_materialization_result(result, plan)
    normalized_result, quality_summary, warnings = apply_normalization_decisions(
        repaired_result,
        plan,
        normalization_decisions,
    )
    resource_summary = validate_materialization_result(normalized_result, plan)
    if guard_summary is not None:
        normalized_result["guard_summary"] = guard_summary
    normalized_result["repair_summary"] = repair_summary
    normalized_result["resource_summary"] = resource_summary
    normalized_result["quality_summary"] = quality_summary
    normalized_result["warnings"] = warnings
    return normalized_result


def draft_materialization_plan(proposal: dict[str, Any], dataset_id: str) -> dict[str, Any]:
    observation_lookup = {
        (column["source_table"], column["source_column"]): column
        for table in proposal.get("observations", [])
        if isinstance(table, dict)
        for column in table.get("columns", [])
        if isinstance(column, dict)
    }
    components = []
    for draft in proposal.get("schema_draft", []):
        display_name = draft["display_name"]
        components.append(
            {
                "component_id": draft["component_id"],
                "display_name": display_name,
                "physical_name": (
                    f'merged_{dataset_id.replace("-", "")[:8]}_{slugify(display_name)}'
                )[:55],
                "source_tables": draft["source_tables"],
                "columns": [],
            }
        )
    component_lookup = {item["component_id"]: item for item in components}
    normalization_lookup = {
        (item["source_table"], item["source_column"]): item.get("actions", [])
        for item in proposal.get("normalization_plan", [])
    }
    for mapping in proposal.get("column_mappings", []):
        component = component_lookup.get(mapping["component_id"])
        if component is None:
            continue
        source_columns = []
        for match in mapping["matches"]:
            observed = observation_lookup.get((match["source_table"], match["source_column"]))
            source_columns.append(
                {
                    **match,
                    "actions": normalization_lookup.get(
                        (match["source_table"], match["source_column"]),
                        ["trim_whitespace"],
                    ),
                    "profile": observed.get("profile", {}) if observed else {},
                    "logical_type": observed.get("logical_type") if observed else None,
                }
            )
        observed_profiles = [item["profile"] for item in source_columns if item.get("profile")]
        component["columns"].append(
            {
                "name": mapping["canonical_name"],
                "logical_type": mapping["logical_type"],
                "status": mapping["decision"],
                "source_columns": source_columns,
                "rationale": mapping["rationale"],
                "sample_values": [
                    sample
                    for profile in observed_profiles
                    for sample in profile.get("sample_values", [])
                ][:8],
                "recommended_normalization": _default_actions_for_column(
                    {"logical_type": mapping["logical_type"]}
                ),
            }
        )
    for component in components:
        for column in component["columns"]:
            suggested_value_mapping = (
                _suggest_value_mapping(column)
                if str(column.get("logical_type", "text")) in {"text", "string"}
                else {}
            )
            column["suggested_value_mapping"] = suggested_value_mapping
            if suggested_value_mapping and "map_values" not in column["recommended_normalization"]:
                column["recommended_normalization"] = [
                    *column["recommended_normalization"],
                    "map_values",
                ]
    return {"components": components}


def build_materialization_retry_guidance(
    run_result: dict[str, Any] | None,
    proposal_payload: dict[str, Any] | None = None,
    previous_run_id: str | None = None,
) -> dict[str, Any]:
    if not run_result:
        return {
            "reason": "manual_retry",
            "previous_run_id": previous_run_id,
            "focus_points": ["simplify generated code and preserve provenance columns"],
            "column_patches": [],
        }

    guard_summary = run_result.get("guard_summary") or {}
    quality_summary = run_result.get("quality_summary") or {}
    warning_items = [str(item) for item in run_result.get("warnings", [])]
    violations = [
        f'{item.get("type", "")}:{item.get("value", "")}'
        for item in guard_summary.get("violations", [])
        if isinstance(item, dict)
    ]

    focus_points: list[str] = []
    error_stage = run_result.get("error_stage")
    error_message = run_result.get("error")
    if error_stage:
        focus_points.append(f"Resolve the previous {error_stage} failure before adding complexity.")
    if violations:
        focus_points.append("Avoid banned imports, introspection, and unsafe attribute access.")
    if warning_items:
        focus_points.append("Address the quality warnings from the previous run.")
    if quality_summary.get("warning_count", 0):
        focus_points.append("Prefer deterministic normalization for columns with parse or null-ratio warnings.")
    if not focus_points:
        focus_points.append("Keep the retry conservative and preserve provenance columns in every merged table.")

    column_issues: list[dict[str, Any]] = []
    column_patches: list[dict[str, Any]] = []
    for table in quality_summary.get("table_summaries", []):
        if not isinstance(table, dict):
            continue
        display_name = str(table.get("display_name", ""))
        component_id = str(table.get("component_id", ""))
        for column in table.get("column_summaries", []):
            if not isinstance(column, dict) or not column.get("warnings"):
                continue
            warning_types = [str(item) for item in column.get("warnings", [])]
            column_issues.append(
                {
                    "table_name": display_name,
                    "column_name": str(column.get("column_name", "")),
                    "warnings": warning_types,
                    "actions": [str(item) for item in column.get("actions", [])],
                    "warning_details": [_warning_detail(item) for item in warning_types],
                }
            )
            suggested_actions = [str(item) for item in column.get("actions", [])]
            suggested_config_patch: dict[str, Any] = {}
            for warning_type in warning_types:
                for action in WARNING_ACTION_HINTS.get(warning_type, []):
                    if action not in suggested_actions:
                        suggested_actions.append(action)
            if "low_numeric_parse_ratio" in warning_types:
                suggested_config_patch["percent_mode"] = "auto"
            if "low_datetime_parse_ratio" in warning_types:
                suggested_config_patch["dayfirst"] = False
            column_patches.append(
                {
                    "component_id": component_id,
                    "table_name": display_name,
                    "column_name": str(column.get("column_name", "")),
                    "warning_types": warning_types,
                    "warning_details": [_warning_detail(item) for item in warning_types],
                    "suggested_actions": suggested_actions,
                    "suggested_config_patch": suggested_config_patch,
                }
            )

    guidance = {
        "reason": "failed_run_retry" if error_stage else "quality_retry",
        "previous_run_id": previous_run_id,
        "previous_error_stage": error_stage,
        "previous_error": error_message,
        "guard_violations": violations,
        "quality_warnings": warning_items,
        "column_issues": column_issues,
        "column_patches": column_patches,
        "focus_points": focus_points,
    }
    if proposal_payload:
        guidance["proposal_summary"] = proposal_payload.get("summary")
    return guidance
