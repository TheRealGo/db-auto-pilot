from __future__ import annotations

import json
import re
import warnings
from collections import Counter, defaultdict
from datetime import date, datetime
from difflib import SequenceMatcher
from itertools import combinations
from typing import Any

import pandas as pd

from app.config import Settings, effective_openai_settings

try:
    from openai import OpenAI
    from openai import OpenAIError
except ImportError:  # pragma: no cover
    OpenAI = None
    OpenAIError = Exception


class ProposalGenerationError(RuntimeError):
    pass


SYNONYM_GROUPS = (
    {"customer", "client", "account", "顧客", "得意先", "取引先"},
    {"name", "title", "名称", "名前", "氏名"},
    {"department", "division", "section", "部署", "部門", "組織"},
    {"sales", "revenue", "amount", "売上", "金額", "請求額"},
    {"date", "day", "年月日", "日付", "計上日"},
    {"employee", "staff", "担当者", "社員"},
    {"product", "item", "sku", "商品", "製品"},
    {"count", "qty", "quantity", "件数", "数量"},
    {"type", "category", "kind", "種類", "分類"},
)

MAX_AGENT_STEPS = 4
MERGE_SCORE_THRESHOLD = 0.52
REVIEW_SCORE_THRESHOLD = 0.4
BLOCKING_REVIEW_SCORE_THRESHOLD = 0.48


def make_json_safe(value: Any) -> Any:
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, (pd.Timestamp, datetime, date)):
        return value.isoformat()
    if isinstance(value, dict):
        return {str(key): make_json_safe(item) for key, item in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [make_json_safe(item) for item in value]
    if hasattr(value, "item"):
        try:
            return make_json_safe(value.item())
        except Exception:
            return str(value)
    return str(value)


def similarity(left: str, right: str) -> float:
    return SequenceMatcher(None, left, right).ratio()


def tokenize(value: str) -> set[str]:
    tokens = {token for token in re.split(r"[^0-9a-zA-Z一-龠ぁ-んァ-ヶ]+", value.lower()) if token}
    expanded = set(tokens)
    for group in SYNONYM_GROUPS:
        if tokens & group:
            expanded |= group
    return expanded


def column_dtype_summary(series: pd.Series) -> dict[str, Any]:
    non_null = series.dropna()
    sample_values = [make_json_safe(item) for item in non_null.head(5).tolist()]
    unique_samples = [make_json_safe(item) for item in non_null.astype(str).drop_duplicates().head(5).tolist()]
    as_string = non_null.astype(str) if not non_null.empty else pd.Series(dtype=str)
    numeric_ratio = round(float(pd.to_numeric(non_null, errors="coerce").notna().mean()), 3) if not non_null.empty else 0.0
    if not non_null.empty:
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", UserWarning)
            datetime_ratio = round(float(pd.to_datetime(non_null, errors="coerce").notna().mean()), 3)
    else:
        datetime_ratio = 0.0
    return {
        "row_count": int(len(series)),
        "null_ratio": round(float(series.isna().mean()), 3) if len(series) else 0.0,
        "distinct_ratio": round(float(non_null.nunique()) / float(len(non_null)), 3) if not non_null.empty else 0.0,
        "sample_values": sample_values,
        "unique_samples": unique_samples,
        "category_samples": unique_samples if 0 < len(unique_samples) <= 5 else [],
        "value_lengths": {
            "min": int(as_string.str.len().min()) if not as_string.empty else 0,
            "max": int(as_string.str.len().max()) if not as_string.empty else 0,
        },
        "numeric_ratio": numeric_ratio,
        "datetime_ratio": datetime_ratio,
        "boolean_ratio": round(float(as_string.str.lower().isin({"true", "false", "yes", "no", "y", "n", "0", "1"}).mean()), 3)
        if not as_string.empty
        else 0.0,
    }


def _normalized_forms(value: Any) -> set[str]:
    if value is None or pd.isna(value):
        return set()
    text = str(value).strip()
    if not text:
        return set()
    compact = re.sub(r"[\s\-_./]+", "", text.casefold())
    return {text.casefold(), compact}


def sample_value_overlap(left: dict[str, Any], right: dict[str, Any]) -> float:
    left_forms = {
        form
        for value in left["profile"].get("unique_samples", [])
        for form in _normalized_forms(value)
    }
    right_forms = {
        form
        for value in right["profile"].get("unique_samples", [])
        for form in _normalized_forms(value)
    }
    if not left_forms or not right_forms:
        return 0.0
    return round(len(left_forms & right_forms) / float(min(len(left_forms), len(right_forms))), 3)


def _column_text(column: dict[str, Any]) -> str:
    return " ".join(
        [
            str(column.get("display_name", "")),
            str(column.get("normalized_name", "")),
            " ".join(str(item) for item in column.get("normalized_candidates", [])),
            " ".join(str(item) for item in column.get("tokens", [])),
        ]
    ).lower()


def build_observations(
    raw_tables: list[dict[str, Any]],
    dataframe_map: dict[str, pd.DataFrame],
) -> list[dict[str, Any]]:
    observations = []
    for raw_table in raw_tables:
        df = dataframe_map[raw_table["table_name"]]
        table_columns = []
        for column in raw_table["columns"]:
            series = df[column["db_name"]]
            table_columns.append(
                {
                    "source_table": raw_table["table_name"],
                    "source_display_name": raw_table["display_name"],
                    "source_column": column["db_name"],
                    "display_name": column["original_name"],
                    "normalized_name": column["normalized_name"],
                    "normalized_candidates": column.get("normalized_candidates", [column["normalized_name"]]),
                    "logical_type": column["logical_type"],
                    "tokens": sorted(tokenize(column["normalized_name"])),
                    "profile": column_dtype_summary(series),
                }
            )
        observations.append(
            {
                "table_name": raw_table["table_name"],
                "display_name": raw_table["display_name"],
                "source_filename": raw_table["source_filename"],
                "sheet_name": raw_table["sheet_name"],
                "row_count": raw_table["row_count"],
                "columns": table_columns,
            }
        )
    return observations


def candidate_score(left: dict[str, Any], right: dict[str, Any]) -> tuple[float, list[str]]:
    reasons: list[str] = []
    name_score = similarity(left["normalized_name"], right["normalized_name"])
    candidate_overlap = len(set(left.get("normalized_candidates", [])) & set(right.get("normalized_candidates", [])))
    token_overlap = len(set(left["tokens"]) & set(right["tokens"]))
    type_score = 1.0 if left["logical_type"] == right["logical_type"] else 0.35
    distinct_score = 1.0 - min(abs(left["profile"]["distinct_ratio"] - right["profile"]["distinct_ratio"]), 1.0)
    datetime_score = 1.0 - min(abs(left["profile"]["datetime_ratio"] - right["profile"]["datetime_ratio"]), 1.0)
    numeric_score = 1.0 - min(abs(left["profile"]["numeric_ratio"] - right["profile"]["numeric_ratio"]), 1.0)
    value_overlap = sample_value_overlap(left, right)
    score = round(
        (name_score * 0.28)
        + (min(candidate_overlap, 1.0) * 0.08)
        + (min(token_overlap / 2.0, 1.0) * 0.22)
        + (type_score * 0.2)
        + (distinct_score * 0.1)
        + (datetime_score * 0.05)
        + (numeric_score * 0.05),
        3,
    )
    score = round(
        score + min(value_overlap * 0.12, 0.12),
        3,
    )
    if name_score >= 0.7:
        reasons.append("column names are similar")
    if candidate_overlap:
        reasons.append("normalized column aliases overlap")
    if token_overlap:
        reasons.append("semantic tokens overlap")
    if left["logical_type"] == right["logical_type"]:
        reasons.append("logical types match")
    if distinct_score >= 0.8:
        reasons.append("distinct ratios are close")
    if value_overlap >= 0.3:
        reasons.append("sample values overlap")
    if not reasons:
        reasons.append("weak signal match")
    return score, reasons


def build_comparison_candidates(observations: list[dict[str, Any]]) -> list[dict[str, Any]]:
    flat_columns = [
        {
            **column,
            "source_filename": table["source_filename"],
            "sheet_name": table["sheet_name"],
        }
        for table in observations
        for column in table["columns"]
    ]
    candidates = []
    candidate_index = 1
    for left, right in combinations(flat_columns, 2):
        if left["source_table"] == right["source_table"]:
            continue
        score, reasons = candidate_score(left, right)
        if score < REVIEW_SCORE_THRESHOLD:
            continue
        candidates.append(
            {
                "candidate_id": f"cand_{candidate_index:03d}",
                "score": score,
                "left": {
                    "source_table": left["source_table"],
                    "display_name": left["display_name"],
                    "source_column": left["source_column"],
                    "logical_type": left["logical_type"],
                },
                "right": {
                    "source_table": right["source_table"],
                    "display_name": right["display_name"],
                    "source_column": right["source_column"],
                    "logical_type": right["logical_type"],
                },
                "signal": "merge" if score >= MERGE_SCORE_THRESHOLD else "review",
                "value_overlap": sample_value_overlap(left, right),
                "reasoning": reasons,
            }
        )
        candidate_index += 1
    return sorted(candidates, key=lambda item: item["score"], reverse=True)


def parse_feedback_overrides(
    feedback: str | None,
    observations: list[dict[str, Any]],
    comparison_candidates: list[dict[str, Any]],
) -> dict[str, Any]:
    if not feedback or not feedback.strip():
        return {"raw_feedback": feedback, "overrides": []}
    normalized_feedback = feedback.strip()
    lowered = normalized_feedback.lower()
    column_lookup = _column_lookup(observations)
    mentioned_columns = {
        key
        for key, column in column_lookup.items()
        if column["display_name"].lower() in lowered or column["source_column"].lower() in lowered
    }
    overrides: list[dict[str, Any]] = []
    force_merge_markers = ("統合して", "統合してください", "merge", "same column", "同じ列")
    keep_separate_markers = ("統合しない", "別概念", "separate", "keep separate", "mergeしない")
    canonical_markers = re.findall(r"(?:canonical|列名|名前)\s*(?:は|を)?\s*[\"“]?([0-9A-Za-z_一-龠ぁ-んァ-ヶ]+)[\"”]?", normalized_feedback)
    for candidate in comparison_candidates:
        left_key = (candidate["left"]["source_table"], candidate["left"]["source_column"])
        right_key = (candidate["right"]["source_table"], candidate["right"]["source_column"])
        if not ({left_key, right_key} <= mentioned_columns):
            continue
        if any(marker in lowered for marker in keep_separate_markers):
            overrides.append(
                {
                    "candidate_id": candidate["candidate_id"],
                    "type": "keep_separate",
                    "reason": normalized_feedback,
                }
            )
        elif any(marker in lowered for marker in force_merge_markers):
            override: dict[str, Any] = {
                "candidate_id": candidate["candidate_id"],
                "type": "force_merge",
                "reason": normalized_feedback,
            }
            if canonical_markers:
                override["canonical_name"] = canonical_markers[0]
            overrides.append(override)
        elif canonical_markers:
            overrides.append(
                {
                    "candidate_id": candidate["candidate_id"],
                    "type": "canonical_name_override",
                    "canonical_name": canonical_markers[0],
                    "reason": normalized_feedback,
                }
            )
    return {"raw_feedback": normalized_feedback, "overrides": overrides}


def _table_pair_overlap(
    left_table: str,
    right_table: str,
    observations: list[dict[str, Any]],
) -> dict[str, Any]:
    column_lookup = _column_lookup(observations)
    left_columns = [column for table in observations if table["table_name"] == left_table for column in table["columns"]]
    right_columns = [column for table in observations if table["table_name"] == right_table for column in table["columns"]]
    candidates = []
    for left in left_columns:
        for right in right_columns:
            score, reasons = candidate_score(left, right)
            if score < REVIEW_SCORE_THRESHOLD:
                continue
            candidates.append(
                {
                    "left_column": left["source_column"],
                    "right_column": right["source_column"],
                    "score": score,
                    "value_overlap": sample_value_overlap(left, right),
                    "reasoning": reasons,
                }
            )
    candidates.sort(key=lambda item: item["score"], reverse=True)
    return {
        "left_table": left_table,
        "right_table": right_table,
        "pair_count": len(candidates),
        "top_pairs": candidates[:6],
        "table_relation_strength": round(sum(item["score"] for item in candidates[:3]) / float(min(len(candidates), 3)), 3)
        if candidates
        else 0.0,
    }


def _column_group_compare(
    left_columns: list[dict[str, Any]],
    right_columns: list[dict[str, Any]],
) -> dict[str, Any]:
    pair_scores = []
    for left, right in zip(left_columns, right_columns, strict=False):
        score, reasons = candidate_score(left, right)
        pair_scores.append(
            {
                "left_column": left["source_column"],
                "right_column": right["source_column"],
                "score": score,
                "value_overlap": sample_value_overlap(left, right),
                "reasoning": reasons,
            }
        )
    if not pair_scores:
        return {"group_score": 0.0, "pairs": []}
    return {
        "group_score": round(sum(item["score"] for item in pair_scores) / float(len(pair_scores)), 3),
        "pairs": pair_scores,
    }


def ensure_openai_available(settings: Settings) -> None:
    openai_settings = effective_openai_settings(settings)
    if not openai_settings.api_key:
        raise ProposalGenerationError("OpenAI API key is not configured.")
    if OpenAI is None:
        raise ProposalGenerationError("OpenAI SDK is not installed.")


def _openai_json_response(settings: Settings, system_prompt: str, user_payload: dict[str, Any]) -> dict[str, Any]:
    openai_settings = effective_openai_settings(settings)
    client_options: dict[str, Any] = {"api_key": openai_settings.api_key}
    if openai_settings.endpoint:
        client_options["base_url"] = openai_settings.endpoint
    try:
        client = OpenAI(**client_options)
    except Exception as exc:  # pragma: no cover
        raise ProposalGenerationError(f"OpenAI client initialization failed: {exc}") from exc
    safe_payload = make_json_safe(user_payload)
    try:
        response = client.chat.completions.create(
            model=openai_settings.model,
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": json.dumps(safe_payload, ensure_ascii=False)},
            ],
        )
    except OpenAIError as exc:
        raise ProposalGenerationError(f"OpenAI proposal generation failed: {exc}") from exc
    except Exception as exc:  # pragma: no cover
        raise ProposalGenerationError(f"Unexpected OpenAI proposal error: {exc}") from exc

    message = response.choices[0].message if response.choices else None
    text = (message.content or "").strip() if message else ""
    if not text:
        raise ProposalGenerationError("OpenAI returned an empty proposal response.")
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        start = text.find("{")
        end = text.rfind("}")
        if start >= 0 and end > start:
            return json.loads(text[start : end + 1])
    raise ProposalGenerationError("OpenAI proposal response was not valid JSON.")


def _observation_context(
    observations: list[dict[str, Any]],
    comparison_candidates: list[dict[str, Any]],
) -> dict[str, Any]:
    tables = [
        {
            "table_name": table["table_name"],
            "display_name": table["display_name"],
            "row_count": table["row_count"],
            "columns": [
                {
                    "source_column": column["source_column"],
                    "display_name": column["display_name"],
                    "logical_type": column["logical_type"],
                    "profile": {
                        "sample_values": column["profile"]["sample_values"],
                        "distinct_ratio": column["profile"]["distinct_ratio"],
                        "numeric_ratio": column["profile"]["numeric_ratio"],
                        "datetime_ratio": column["profile"]["datetime_ratio"],
                    },
                }
                for column in table["columns"]
            ],
        }
        for table in observations
    ]
    return {
        "tables": tables,
        "table_pairs": [
            _table_pair_overlap(left["table_name"], right["table_name"], observations)
            for left, right in combinations(tables, 2)
        ][:10],
        "top_candidates": comparison_candidates[:20],
        "review_candidates": [candidate for candidate in comparison_candidates if candidate["signal"] == "review"][:20],
    }


def _table_lookup(observations: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    return {table["table_name"]: table for table in observations}


def _column_lookup(observations: list[dict[str, Any]]) -> dict[tuple[str, str], dict[str, Any]]:
    return {
        (column["source_table"], column["source_column"]): column
        for table in observations
        for column in table["columns"]
    }


def run_observation_request(
    request: dict[str, Any],
    observations: list[dict[str, Any]],
    comparison_candidates: list[dict[str, Any]],
    dataframe_map: dict[str, pd.DataFrame],
) -> dict[str, Any]:
    tool = request.get("tool")
    args = request.get("args", {}) if isinstance(request.get("args"), dict) else {}
    table_lookup = _table_lookup(observations)
    column_lookup = _column_lookup(observations)

    if tool == "list_tables":
        return _observation_context(observations, comparison_candidates)["tables"]

    if tool == "describe_columns":
        table_name = args.get("table_name")
        if table_name not in table_lookup:
            raise ProposalGenerationError(f"Agent requested unknown table in describe_columns: {table_name}.")
        table = table_lookup[table_name]
        return {
            "table_name": table_name,
            "columns": table["columns"],
        }

    if tool == "sample_rows":
        table_name = args.get("table_name")
        limit = max(1, min(int(args.get("limit", 5)), 10))
        columns = args.get("columns")
        if table_name not in dataframe_map:
            raise ProposalGenerationError(f"Agent requested unknown table in sample_rows: {table_name}.")
        df = dataframe_map[table_name]
        if columns:
            valid_columns = [column for column in columns if column in df.columns]
            if not valid_columns:
                valid_columns = list(df.columns[: min(5, len(df.columns))])
            df = df[valid_columns]
        return {
            "table_name": table_name,
            "rows": make_json_safe(df.head(limit).to_dict(orient="records")),
        }

    if tool == "distinct_values":
        table_name = args.get("table_name")
        column_name = args.get("column_name")
        limit = max(1, min(int(args.get("limit", 10)), 20))
        if table_name not in dataframe_map or column_name not in dataframe_map[table_name].columns:
            raise ProposalGenerationError(
                f"Agent requested unknown table/column in distinct_values: {table_name}.{column_name}."
            )
        series = dataframe_map[table_name][column_name]
        values = [make_json_safe(item) for item in series.dropna().astype(str).drop_duplicates().head(limit).tolist()]
        return {
            "table_name": table_name,
            "column_name": column_name,
            "values": values,
        }

    if tool == "search_columns":
        query = str(args.get("query", "")).strip().lower()
        if not query:
            raise ProposalGenerationError("Agent requested search_columns without query.")
        matches = []
        for table in observations:
            for column in table["columns"]:
                haystack = " ".join(
                    [
                        column["display_name"],
                        column["normalized_name"],
                        " ".join(column.get("normalized_candidates", [])),
                        " ".join(column.get("tokens", [])),
                    ]
                ).lower()
                if query in haystack:
                    matches.append(
                        {
                            "source_table": column["source_table"],
                            "source_column": column["source_column"],
                            "display_name": column["display_name"],
                            "logical_type": column["logical_type"],
                        }
                    )
        return {"query": query, "matches": matches[:20]}

    if tool == "table_pair_overlap":
        left_table = str(args.get("left_table", ""))
        right_table = str(args.get("right_table", ""))
        if left_table not in table_lookup or right_table not in table_lookup:
            raise ProposalGenerationError("Agent requested unknown table in table_pair_overlap.")
        return _table_pair_overlap(left_table, right_table, observations)

    if tool == "column_group_compare":
        left_group = args.get("left_group", [])
        right_group = args.get("right_group", [])
        if not isinstance(left_group, list) or not isinstance(right_group, list):
            raise ProposalGenerationError("Agent requested invalid groups in column_group_compare.")
        left_columns = []
        right_columns = []
        for item in left_group:
            if not isinstance(item, dict):
                continue
            key = (item.get("source_table"), item.get("source_column"))
            if key in column_lookup:
                left_columns.append(column_lookup[key])
        for item in right_group:
            if not isinstance(item, dict):
                continue
            key = (item.get("source_table"), item.get("source_column"))
            if key in column_lookup:
                right_columns.append(column_lookup[key])
        if not left_columns or not right_columns:
            raise ProposalGenerationError("Agent requested unknown columns in column_group_compare.")
        return _column_group_compare(left_columns, right_columns)

    if tool == "value_overlap":
        left = args.get("left", {})
        right = args.get("right", {})
        left_key = (left.get("source_table"), left.get("source_column"))
        right_key = (right.get("source_table"), right.get("source_column"))
        if left_key not in column_lookup or right_key not in column_lookup:
            raise ProposalGenerationError("Agent requested unknown column in value_overlap.")
        left_column = column_lookup[left_key]
        right_column = column_lookup[right_key]
        return {
            "left": left_column,
            "right": right_column,
            "value_overlap": sample_value_overlap(left_column, right_column),
            "left_samples": left_column["profile"].get("unique_samples", [])[:8],
            "right_samples": right_column["profile"].get("unique_samples", [])[:8],
        }

    if tool == "column_pair_compare":
        left = args.get("left", {})
        right = args.get("right", {})
        left_key = (left.get("source_table"), left.get("source_column"))
        right_key = (right.get("source_table"), right.get("source_column"))
        if left_key not in column_lookup or right_key not in column_lookup:
            raise ProposalGenerationError("Agent requested unknown column in column_pair_compare.")
        left_column = column_lookup[left_key]
        right_column = column_lookup[right_key]
        score, reasons = candidate_score(left_column, right_column)
        return {
            "left": left_column,
            "right": right_column,
            "score": score,
            "reasoning": reasons,
        }

    raise ProposalGenerationError(f"Agent requested unsupported observation tool: {tool}.")


def build_agent_step_prompt(
    dataset_name: str,
    feedback: str | None,
    base_context: dict[str, Any],
    agent_steps: list[dict[str, Any]],
    force_finalize: bool,
    prior_proposal: dict[str, Any] | None,
    feedback_overrides: dict[str, Any] | None,
) -> dict[str, Any]:
    return {
        "dataset_name": dataset_name,
        "feedback": feedback,
        "force_finalize": force_finalize,
        "agent_strategy": {
            "phase_order": ["expand candidate search", "inspect review candidates", "finalize proposal"],
            "finalize_rule": "Do not finalize with low-confidence candidates unless they are marked review or keep_separate.",
        },
        "feedback_overrides": feedback_overrides,
        "prior_proposal_context": (
            {
                "summary": prior_proposal.get("summary"),
                "questions_for_user": prior_proposal.get("questions_for_user", []),
                "review_items": prior_proposal.get("review_items", []),
                "user_decisions": prior_proposal.get("user_decisions", []),
                "canonical_overview": (prior_proposal.get("canonical_proposal") or {}).get("overview"),
            }
            if prior_proposal
            else None
        ),
        "base_context": base_context,
        "agent_steps_so_far": agent_steps,
        "available_tools": [
            {"tool": "list_tables", "args": {}},
            {"tool": "search_columns", "args": {"query": "customer"}},
            {"tool": "table_pair_overlap", "args": {"left_table": "raw_table_a", "right_table": "raw_table_b"}},
            {"tool": "describe_columns", "args": {"table_name": "raw_table_name"}},
            {"tool": "sample_rows", "args": {"table_name": "raw_table_name", "columns": ["col_a"], "limit": 5}},
            {"tool": "distinct_values", "args": {"table_name": "raw_table_name", "column_name": "col_a", "limit": 10}},
            {
                "tool": "column_group_compare",
                "args": {
                    "left_group": [{"source_table": "raw_table_a", "source_column": "col_a"}],
                    "right_group": [{"source_table": "raw_table_b", "source_column": "col_b"}],
                },
            },
            {
                "tool": "value_overlap",
                "args": {
                    "left": {"source_table": "raw_table_a", "source_column": "col_a"},
                    "right": {"source_table": "raw_table_b", "source_column": "col_b"},
                },
            },
            {
                "tool": "column_pair_compare",
                "args": {
                    "left": {"source_table": "raw_table_a", "source_column": "col_a"},
                    "right": {"source_table": "raw_table_b", "source_column": "col_b"},
                },
            },
        ],
        "response_shape": {
            "mode": "observe | finalize",
            "notes": ["string"],
            "observation_requests": [
                {
                    "tool": "one of available_tools",
                    "args": {},
                    "reason": "why this observation is needed",
                }
            ],
            "final_proposal": {
                "summary": "string",
                "decisions": [
                    {
                        "candidate_id": "cand_001",
                        "action": "merge | keep_separate | review",
                        "canonical_name": "string or null",
                        "reason": "string",
                    }
                ],
                "normalization_plan": [
                    {
                        "source_table": "string",
                        "source_column": "string",
                        "actions": ["trim_whitespace", "normalize_case"],
                        "reason": "string",
                    }
                ],
                "questions_for_user": ["string"],
                "notes": ["string"],
            },
        },
    }


def run_agentic_loop(
    settings: Settings,
    dataset_name: str,
    feedback: str | None,
    observations: list[dict[str, Any]],
    comparison_candidates: list[dict[str, Any]],
    dataframe_map: dict[str, pd.DataFrame],
    prior_proposal: dict[str, Any] | None = None,
    feedback_overrides: dict[str, Any] | None = None,
) -> tuple[dict[str, Any], list[dict[str, Any]], list[dict[str, Any]]]:
    base_context = _observation_context(observations, comparison_candidates)
    system_prompt = (
        "You are a local data-integration agent. "
        "You must either request additional observations using the allowed tools or return a final proposal. "
        "Return JSON only. Do not invent source tables, source columns, or candidate_id values. "
        "Use observe mode when you still need evidence. Use finalize mode when you can confidently produce the proposal."
    )
    agent_steps: list[dict[str, Any]] = []
    raw_llm_responses: list[dict[str, Any]] = []
    for step_index in range(1, MAX_AGENT_STEPS + 1):
        force_finalize = step_index == MAX_AGENT_STEPS
        payload = build_agent_step_prompt(
            dataset_name=dataset_name,
            feedback=feedback,
            base_context=base_context,
            agent_steps=agent_steps,
            force_finalize=force_finalize,
            prior_proposal=prior_proposal,
            feedback_overrides=feedback_overrides,
        )
        llm_response = _openai_json_response(settings, system_prompt, payload)
        raw_llm_responses.append(llm_response)
        mode = llm_response.get("mode")
        if mode == "finalize":
            final_proposal = llm_response.get("final_proposal")
            if not isinstance(final_proposal, dict):
                raise ProposalGenerationError("OpenAI finalize response did not include final_proposal.")
            final_proposal.setdefault("notes", [])
            step_notes = llm_response.get("notes", [])
            if isinstance(step_notes, list):
                final_proposal["notes"] = [*final_proposal["notes"], *step_notes]
            return final_proposal, agent_steps, raw_llm_responses

        if mode != "observe":
            raise ProposalGenerationError("OpenAI proposal loop returned an unsupported mode.")

        requests = llm_response.get("observation_requests")
        if not isinstance(requests, list) or not requests:
            raise ProposalGenerationError("OpenAI observe response did not include observation_requests.")
        results = []
        for request in requests[:4]:
            result = run_observation_request(request, observations, comparison_candidates, dataframe_map)
            results.append(
                {
                    "tool": request.get("tool"),
                    "args": request.get("args", {}),
                    "reason": request.get("reason", ""),
                    "result": result,
                }
            )
        agent_steps.append(
            {
                "step": step_index,
                "mode": "observe",
                "notes": llm_response.get("notes", []),
                "observation_requests": results,
            }
        )

    raise ProposalGenerationError("OpenAI proposal loop did not finalize within the allowed number of steps.")


def build_union_groups(
    observations: list[dict[str, Any]],
    decisions: list[dict[str, Any]],
    candidate_lookup: dict[str, dict[str, Any]],
) -> list[dict[str, Any]]:
    parent: dict[tuple[str, str], tuple[str, str]] = {}

    def find(item: tuple[str, str]) -> tuple[str, str]:
        parent.setdefault(item, item)
        if parent[item] != item:
            parent[item] = find(parent[item])
        return parent[item]

    def union(left: tuple[str, str], right: tuple[str, str]) -> None:
        left_root = find(left)
        right_root = find(right)
        if left_root != right_root:
            parent[right_root] = left_root

    all_columns = [
        (column["source_table"], column["source_column"])
        for table in observations
        for column in table["columns"]
    ]
    for column in all_columns:
        parent.setdefault(column, column)

    for decision in decisions:
        if decision.get("action") != "merge":
            continue
        candidate = candidate_lookup.get(decision["candidate_id"])
        if not candidate:
            continue
        union(
            (candidate["left"]["source_table"], candidate["left"]["source_column"]),
            (candidate["right"]["source_table"], candidate["right"]["source_column"]),
        )

    grouped_columns: dict[tuple[str, str], list[tuple[str, str]]] = defaultdict(list)
    for column in all_columns:
        grouped_columns[find(column)].append(column)

    observation_lookup = _column_lookup(observations)
    groups = []
    for index, columns in enumerate(grouped_columns.values(), start=1):
        source_tables = sorted({source_table for source_table, _ in columns})
        source_entries = [observation_lookup[item] for item in columns]
        action = "single_source" if len(columns) == 1 else "merge"
        chosen_name = source_entries[0]["normalized_name"]
        reasons = []
        for decision in decisions:
            candidate = candidate_lookup.get(decision["candidate_id"])
            if not candidate:
                continue
            left_key = (candidate["left"]["source_table"], candidate["left"]["source_column"])
            right_key = (candidate["right"]["source_table"], candidate["right"]["source_column"])
            if left_key in columns and right_key in columns:
                chosen_name = decision.get("canonical_name") or chosen_name
                if decision.get("reason"):
                    reasons.append(decision["reason"])
        groups.append(
            {
                "group_id": f"group_{index}",
                "source_tables": source_tables,
                "column": {
                    "name": chosen_name,
                    "status": action,
                    "logical_type": Counter(entry["logical_type"] for entry in source_entries).most_common(1)[0][0],
                    "reason": " / ".join(reasons) if reasons else "agent recommendation",
                    "sources": [
                        {
                            "source_table": entry["source_table"],
                            "source_column": entry["source_column"],
                            "display_name": entry["display_name"],
                            "logical_type": entry["logical_type"],
                        }
                        for entry in source_entries
                    ],
                },
            }
        )
    return groups


def assign_components(column_groups: list[dict[str, Any]]) -> list[dict[str, Any]]:
    parent: dict[str, str] = {}

    def find(item: str) -> str:
        parent.setdefault(item, item)
        if parent[item] != item:
            parent[item] = find(parent[item])
        return parent[item]

    def union(left: str, right: str) -> None:
        left_root = find(left)
        right_root = find(right)
        if left_root != right_root:
            parent[right_root] = left_root

    all_tables = sorted({table for group in column_groups for table in group["source_tables"]})
    for table in all_tables:
        parent.setdefault(table, table)
    for group in column_groups:
        if len(group["source_tables"]) < 2:
            continue
        first = group["source_tables"][0]
        for table in group["source_tables"][1:]:
            union(first, table)

    grouped_components: dict[str, dict[str, Any]] = {}
    for index, root in enumerate(sorted({find(table) for table in all_tables}), start=1):
        grouped_components[root] = {
            "component_id": f"component_{index}",
            "display_name": f"Merged Table {index}",
            "source_tables": sorted(table for table in all_tables if find(table) == root),
            "columns": [],
        }
    for group in column_groups:
        root = find(group["source_tables"][0])
        component = grouped_components[root]
        component["columns"].append(group["column"])
        group["component_id"] = component["component_id"]
    return list(grouped_components.values())


def derive_proposal_views(
    observations: list[dict[str, Any]],
    decisions: list[dict[str, Any]],
    comparison_candidates: list[dict[str, Any]],
    normalization_plan: list[dict[str, Any]],
) -> dict[str, Any]:
    candidate_lookup = {candidate["candidate_id"]: candidate for candidate in comparison_candidates}
    column_groups = build_union_groups(observations, decisions, candidate_lookup)
    components = assign_components(column_groups)

    normalization_lookup: dict[tuple[str, str], list[str]] = defaultdict(list)
    for item in normalization_plan:
        actions = item.get("actions", [])
        if isinstance(actions, list):
            normalization_lookup[(item.get("source_table", ""), item.get("source_column", ""))] = actions

    column_mappings = []
    review_items = []
    for decision in decisions:
        candidate = candidate_lookup[decision["candidate_id"]]
        group = next(
            (
                current_group
                for current_group in column_groups
                if {
                    (candidate["left"]["source_table"], candidate["left"]["source_column"]),
                    (candidate["right"]["source_table"], candidate["right"]["source_column"]),
                }
                <= {
                    (source["source_table"], source["source_column"])
                    for source in current_group["column"]["sources"]
                }
            ),
            None,
        )
        component_id = group["component_id"] if group else "component_1"
        mapping = {
            "component_id": component_id,
            "canonical_name": decision.get("canonical_name") or candidate["left"]["source_column"],
            "logical_type": candidate["left"]["logical_type"],
            "decision": decision["action"],
            "review_status": (
                "blocked"
                if decision["action"] == "keep_separate"
                else "needs_review" if decision["action"] != "merge" else "ready"
            ),
            "merge_recommended": decision["action"] == "merge",
            "confidence": candidate["score"],
            "rationale": decision["reason"] or "; ".join(candidate["reasoning"]),
            "override_applied": bool(decision.get("override_applied")),
            "evidence_summary": {
                "candidate_score": candidate["score"],
                "signal": candidate.get("signal", "review"),
                "value_overlap": candidate.get("value_overlap", 0.0),
                "reasons": candidate["reasoning"],
            },
            "matches": [
                {
                    "source_table": candidate["left"]["source_table"],
                    "source_column": candidate["left"]["source_column"],
                    "display_name": candidate["left"]["display_name"],
                },
                {
                    "source_table": candidate["right"]["source_table"],
                    "source_column": candidate["right"]["source_column"],
                    "display_name": candidate["right"]["display_name"],
                },
            ],
        }
        column_mappings.append(mapping)
        if decision["action"] in {"review", "keep_separate"}:
            severity = "blocking" if decision["action"] == "keep_separate" or candidate["score"] >= BLOCKING_REVIEW_SCORE_THRESHOLD else "advisory"
            review_items.append(
                {
                    "type": decision["action"],
                    "severity": severity,
                    "canonical_name": mapping["canonical_name"],
                    "component_id": component_id,
                    "message": mapping["rationale"],
                    "override_applied": mapping["override_applied"],
                    "evidence_summary": mapping["evidence_summary"],
                    "matches": mapping["matches"],
                }
            )

    schema_draft = []
    merged_tables = []
    for component in components:
        component_columns = []
        merged_columns = []
        for column in component["columns"]:
            source_columns = [
                {
                    **source,
                    "actions": normalization_lookup.get(
                        (source["source_table"], source["source_column"]),
                        ["trim_whitespace"],
                    ),
                }
                for source in column["sources"]
            ]
            component_columns.append(
                {
                    "name": column["name"],
                    "logical_type": column["logical_type"],
                    "status": column["status"],
                    "source_count": len(source_columns),
                    "rationale": column["reason"],
                }
            )
            merged_columns.append(
                {
                    "name": column["name"],
                    "logical_type": column["logical_type"],
                    "notes": column["reason"],
                    "status": column["status"],
                    "source_columns": source_columns,
                }
            )
        schema_draft.append(
            {
                "component_id": component["component_id"],
                "display_name": component["display_name"],
                "source_tables": component["source_tables"],
                "columns": component_columns,
            }
        )
        merged_tables.append(
            {
                "table_name": component["component_id"],
                "display_name": component["display_name"],
                "source_tables": component["source_tables"],
                "columns": merged_columns,
            }
        )

    materialization_plan_draft = {
        "components": [
            {
                "component_id": component["component_id"],
                "display_name": component["display_name"],
                "source_tables": component["source_tables"],
                "columns": [
                    {
                        "name": column["name"],
                        "status": column["status"],
                        "logical_type": column["logical_type"],
                        "reason": column["notes"],
                        "source_columns": column["source_columns"],
                    }
                    for column in merged["columns"]
                ],
            }
            for component, merged in zip(components, merged_tables, strict=True)
        ]
    }
    return {
        "schema_draft": schema_draft,
        "merged_tables": merged_tables,
        "column_mappings": column_mappings,
        "review_items": review_items,
        "materialization_plan_draft": materialization_plan_draft,
        "table_component_map": {
            table_name: component["component_id"]
            for component in components
            for table_name in component["source_tables"]
        },
    }


def build_canonical_proposal(
    summary: str,
    observations: list[dict[str, Any]],
    proposal_views: dict[str, Any],
    comparison_candidates: list[dict[str, Any]],
    questions_for_user: list[str],
    feedback: str | None,
) -> dict[str, Any]:
    checklist = [
        {
            "title": "Open questions",
            "status": "attention" if questions_for_user else "ready",
            "items": questions_for_user or ["No open questions in the latest proposal."],
        },
        {
            "title": "Blocking reviews",
            "status": "attention" if any(item["severity"] == "blocking" for item in proposal_views["review_items"]) else "ready",
            "items": [
                f'{item["type"]}: {item["canonical_name"]}'
                for item in proposal_views["review_items"]
                if item["severity"] == "blocking"
            ]
            or ["No blocking review decisions remain."],
        },
        {
            "title": "Advisory reviews",
            "status": "attention" if any(item["severity"] == "advisory" for item in proposal_views["review_items"]) else "ready",
            "items": [
                f'{item["type"]}: {item["canonical_name"]}'
                for item in proposal_views["review_items"]
                if item["severity"] == "advisory"
            ]
            or ["No advisory review decisions remain."],
        },
        {
            "title": "Coverage",
            "status": "ready",
            "items": [
                f'{len(observations)} source table(s)',
                f'{len(proposal_views["schema_draft"])} merged component(s)',
            ],
        },
    ]
    candidates = []
    for mapping in proposal_views["column_mappings"]:
        candidates.append(
            {
                "component_id": mapping["component_id"],
                "canonical_name": mapping["canonical_name"],
                "decision": mapping["decision"],
                "confidence": mapping["confidence"],
                "review_status": mapping["review_status"],
                "reason": mapping["rationale"],
                "source_count": len(mapping["matches"]),
                "signal": "merge" if mapping["confidence"] >= MERGE_SCORE_THRESHOLD else "review",
                "override_applied": mapping["override_applied"],
                "evidence_summary": mapping["evidence_summary"],
                "matches": mapping["matches"],
            }
        )
    return {
        "overview": {
            "summary": summary,
            "feedback_applied": bool(feedback),
            "source_table_count": len(observations),
            "merged_component_count": len(proposal_views["schema_draft"]),
            "review_item_count": len(proposal_views["review_items"]),
            "blocking_review_count": len([item for item in proposal_views["review_items"] if item["severity"] == "blocking"]),
            "question_count": len(questions_for_user),
        },
        "approval_checklist": checklist,
        "candidates": candidates,
    }


def apply_feedback_overrides(
    decisions: list[dict[str, Any]],
    feedback_overrides: dict[str, Any] | None,
) -> list[dict[str, Any]]:
    if not feedback_overrides:
        return decisions
    override_lookup = {
        item["candidate_id"]: item
        for item in feedback_overrides.get("overrides", [])
        if isinstance(item, dict) and item.get("candidate_id")
    }
    updated: list[dict[str, Any]] = []
    for decision in decisions:
        override = override_lookup.get(decision["candidate_id"])
        next_decision = dict(decision)
        if override:
            override_type = override.get("type")
            if override_type == "keep_separate":
                next_decision["action"] = "keep_separate"
                next_decision["reason"] = override.get("reason", next_decision["reason"])
            elif override_type == "force_merge":
                next_decision["action"] = "merge"
                next_decision["canonical_name"] = override.get("canonical_name") or next_decision.get("canonical_name")
                next_decision["reason"] = override.get("reason", next_decision["reason"])
            elif override_type == "canonical_name_override":
                next_decision["canonical_name"] = override.get("canonical_name") or next_decision.get("canonical_name")
                next_decision["reason"] = override.get("reason", next_decision["reason"])
            next_decision["override_applied"] = True
        updated.append(next_decision)
    return updated


def canonicalize_proposal(
    dataset_name: str,
    raw_tables: list[dict[str, Any]],
    observations: list[dict[str, Any]],
    comparison_candidates: list[dict[str, Any]],
    llm_response: dict[str, Any],
    agent_steps: list[dict[str, Any]],
    raw_llm_responses: list[dict[str, Any]],
    feedback: str | None = None,
    feedback_overrides: dict[str, Any] | None = None,
) -> dict[str, Any]:
    required_keys = {"summary", "decisions", "normalization_plan", "questions_for_user", "notes"}
    missing_keys = sorted(key for key in required_keys if key not in llm_response)
    if missing_keys:
        raise ProposalGenerationError(
            f"OpenAI proposal response did not include required fields: {', '.join(missing_keys)}."
        )

    candidate_ids = {candidate["candidate_id"] for candidate in comparison_candidates}
    allowed_actions = {"merge", "keep_separate", "review"}
    decisions = []
    for item in llm_response["decisions"]:
        candidate_id = item.get("candidate_id")
        if candidate_id not in candidate_ids:
            raise ProposalGenerationError(f"OpenAI proposal referenced unknown candidate: {candidate_id}.")
        action = str(item.get("action", "review"))
        if action not in allowed_actions:
            raise ProposalGenerationError(f"OpenAI proposal referenced unsupported action: {action}.")
        canonical_name = item.get("canonical_name")
        if action == "merge" and not canonical_name:
            raise ProposalGenerationError("OpenAI proposal must provide canonical_name for merge decisions.")
        decisions.append(
            {
                "candidate_id": candidate_id,
                "action": action,
                "canonical_name": canonical_name,
                "reason": item.get("reason", ""),
            }
        )
    if not decisions:
        raise ProposalGenerationError("OpenAI proposal did not include enough merge decisions.")
    decisions = apply_feedback_overrides(decisions, feedback_overrides)

    normalization_plan = []
    for item in llm_response["normalization_plan"]:
        actions = [str(action) for action in item.get("actions", ["trim_whitespace"])]
        if not actions:
            actions = ["trim_whitespace"]
        normalization_plan.append(
            {
                "source_table": item.get("source_table", ""),
                "source_column": item.get("source_column", ""),
                "actions": actions,
                "reason": item.get("reason", ""),
            }
        )

    observation_lookup = _column_lookup(observations)
    normalization_actions = []
    for item in normalization_plan:
        observed = observation_lookup.get((item["source_table"], item["source_column"]))
        normalization_actions.append(
            {
                "table_name": item["source_table"],
                "display_name": observed["source_display_name"] if observed else item["source_table"],
                "source_column": item["source_column"],
                "normalized_column": item["source_column"],
                "actions": item["actions"],
                "reason": item["reason"],
            }
        )

    proposal_views = derive_proposal_views(
        observations=observations,
        decisions=decisions,
        comparison_candidates=comparison_candidates,
        normalization_plan=normalization_plan,
    )
    proposal_views["merge_recommendations"] = proposal_views["column_mappings"]
    canonical_proposal = build_canonical_proposal(
        summary=str(llm_response["summary"]),
        observations=observations,
        proposal_views=proposal_views,
        comparison_candidates=comparison_candidates,
        questions_for_user=[str(item) for item in llm_response["questions_for_user"]],
        feedback=feedback,
    )
    merge_decisions = [
        {
            "canonical_name": item["canonical_name"],
            "decision": item["decision"],
            "reason": item["rationale"],
            "source_columns": item["matches"],
            "component_id": item["component_id"],
        }
        for item in proposal_views["column_mappings"]
    ]

    return {
        "dataset_name": dataset_name,
        "summary": llm_response["summary"],
        "feedback": feedback,
        "feedback_overrides": feedback_overrides or {"raw_feedback": feedback, "overrides": []},
        "raw_tables": raw_tables,
        "observations": observations,
        "comparison_candidates": comparison_candidates,
        "agent_steps": agent_steps,
        "raw_llm_responses": raw_llm_responses,
        "decisions": decisions,
        "merge_decisions": merge_decisions,
        "questions_for_user": llm_response["questions_for_user"],
        "normalization_plan": normalization_plan,
        "notes": llm_response["notes"],
        "canonical_proposal": canonical_proposal,
        "materialization_plan_draft": proposal_views["materialization_plan_draft"],
        "column_mappings": proposal_views["column_mappings"],
        "review_items": proposal_views["review_items"],
        "schema_draft": proposal_views["schema_draft"],
        "merged_tables": proposal_views["merged_tables"],
        "normalization_actions": normalization_actions,
        "normalization_rules": sorted(
            {action for item in normalization_plan for action in item.get("actions", [])}
        )
        or ["trim_whitespace"],
        "user_decisions": [
            {
                "canonical_name": item["canonical_name"],
                "decision": item["decision"],
                "reason": item["rationale"],
                "source_columns": [
                    {
                        "source_table": match["source_table"],
                        "source_column": match["source_column"],
                    }
                    for match in item["matches"]
                ],
            }
            for item in proposal_views["column_mappings"]
            if item["decision"] != "merge"
        ],
        "merge_recommendations": proposal_views["column_mappings"],
        "table_component_map": proposal_views["table_component_map"],
    }


def generate_proposal(
    settings: Settings,
    dataset_name: str,
    raw_tables: list[dict[str, Any]],
    dataframe_map: dict[str, pd.DataFrame],
    feedback: str | None = None,
    prior_proposal: dict[str, Any] | None = None,
) -> dict[str, Any]:
    ensure_openai_available(settings)
    observations = build_observations(raw_tables, dataframe_map)
    comparison_candidates = build_comparison_candidates(observations)
    if not comparison_candidates:
        raise ProposalGenerationError("Not enough cross-file column candidates were found to build an LLM proposal.")
    feedback_overrides = parse_feedback_overrides(feedback, observations, comparison_candidates)
    final_response, agent_steps, raw_llm_responses = run_agentic_loop(
        settings=settings,
        dataset_name=dataset_name,
        feedback=feedback,
        observations=observations,
        comparison_candidates=comparison_candidates,
        dataframe_map=dataframe_map,
        prior_proposal=prior_proposal,
        feedback_overrides=feedback_overrides,
    )
    return canonicalize_proposal(
        dataset_name=dataset_name,
        raw_tables=raw_tables,
        observations=observations,
        comparison_candidates=comparison_candidates,
        llm_response=final_response,
        agent_steps=agent_steps,
        raw_llm_responses=raw_llm_responses,
        feedback=feedback,
        feedback_overrides=feedback_overrides,
    )
