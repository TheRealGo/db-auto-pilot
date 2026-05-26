from __future__ import annotations

import json
import re
from collections import defaultdict
from difflib import SequenceMatcher
from itertools import combinations
from typing import Any

import pandas as pd

from app.config import Settings, effective_openai_settings


class ProposalGenerationError(RuntimeError):
    pass


SYNONYM_GROUPS = [
    {"department", "dept", "division", "部署", "部門"},
    {"sales", "revenue", "amount", "売上", "売上金額", "金額"},
    {"customer", "client", "顧客", "取引先"},
    {"date", "day", "month", "年月", "日付"},
    {"product", "item", "sku", "商品", "品目"},
]
ALLOWED_GROUP_ACTIONS = {"keep", "coalesce", "merge", "review"}
ALLOWED_GROUP_DECISIONS = {"keep", "merge", "review", "keep_separate"}
ALLOWED_CLEANSING_ACTIONS = {"trim_whitespace", "coalesce_empty_to_null"}


def make_json_safe(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(k): make_json_safe(v) for k, v in value.items()}
    if isinstance(value, list):
        return [make_json_safe(v) for v in value]
    if pd.isna(value):
        return None
    return value


def tokenize(text: str) -> set[str]:
    tokens = set(re.findall(r"[A-Za-z0-9]+|[\u3040-\u30ff\u3400-\u9fff]+", text.lower()))
    for group in SYNONYM_GROUPS:
        if tokens & group:
            tokens |= group
    return tokens


def similarity(left: str, right: str) -> float:
    if left == right:
        return 1.0
    left_tokens = tokenize(left)
    right_tokens = tokenize(right)
    token_score = len(left_tokens & right_tokens) / max(len(left_tokens | right_tokens), 1)
    return max(token_score, SequenceMatcher(None, left.lower(), right.lower()).ratio())


def sample_value_overlap(a: pd.Series, b: pd.Series) -> float:
    left = {str(v).strip().lower() for v in a.dropna().head(100).tolist()}
    right = {str(v).strip().lower() for v in b.dropna().head(100).tolist()}
    return len(left & right) / max(min(len(left), len(right)), 1)


def build_comparison_candidates(frames: dict[str, pd.DataFrame]) -> list[dict[str, Any]]:
    candidates: list[dict[str, Any]] = []
    for (table_a, frame_a), (table_b, frame_b) in combinations(frames.items(), 2):
        for col_a in frame_a.columns:
            for col_b in frame_b.columns:
                name_score = similarity(col_a, col_b)
                value_score = sample_value_overlap(frame_a[col_a], frame_b[col_b])
                score = round((name_score * 0.7) + (value_score * 0.3), 3)
                if score >= 0.45:
                    candidates.append(
                        {
                            "left": {"table": table_a, "column": col_a},
                            "right": {"table": table_b, "column": col_b},
                            "score": score,
                            "reason": "name/value similarity",
                        }
                    )
    return sorted(candidates, key=lambda item: item["score"], reverse=True)


def build_union_groups(frames: dict[str, pd.DataFrame]) -> list[dict[str, Any]]:
    buckets: dict[str, list[dict[str, str]]] = defaultdict(list)
    for table, frame in frames.items():
        for column in frame.columns:
            best_key = column
            best_score = 0.0
            for key in list(buckets):
                score = similarity(key, column)
                if score > best_score:
                    best_key, best_score = key, score
            if best_score < 0.62:
                best_key = column
            buckets[best_key].append({"table": table, "column": column})
    groups = []
    for key, members in buckets.items():
        confidence = 0.7
        decision = "keep"
        review_required = False
        rationale = "Single observed source column is kept as-is."
        if len(members) > 1:
            confidence = round(min(similarity(key, member["column"]) for member in members), 3)
            review_required = confidence < 0.8
            decision = "review" if review_required else "merge"
            rationale = (
                "Candidate columns are similar but need human confirmation before merge."
                if review_required
                else "Candidate columns are close enough for a deterministic merge recommendation."
            )
        groups.append(
            {
                "canonical_name": key,
                "members": members,
                "action": "coalesce" if len(members) > 1 else "keep",
                "confidence": confidence,
                "decision": decision,
                "review_required": review_required,
                "rationale": rationale,
            }
        )
    return groups


def default_cleansing_policy(frames: dict[str, pd.DataFrame]) -> list[dict[str, Any]]:
    actions: list[dict[str, Any]] = []
    for table, frame in frames.items():
        for column in frame.columns:
            series = frame[column]
            if pd.api.types.is_object_dtype(series):
                actions.append({"table": table, "column": column, "action": "trim_whitespace"})
                if series.isna().sum() or (series.astype(str).str.strip() == "").any():
                    actions.append({"table": table, "column": column, "action": "coalesce_empty_to_null"})
    return actions


def build_canonical_proposal(frames: dict[str, pd.DataFrame], feedback: str = "") -> dict[str, Any]:
    return {
        "version": 1,
        "objective": "Create a governed unified analytical dataset from uploaded tabular sources.",
        "feedback": feedback,
        "source_tables": [
            {"table": table, "rows": int(len(frame)), "columns": list(frame.columns)}
            for table, frame in frames.items()
        ],
        "merge_candidates": build_comparison_candidates(frames),
        "canonical_columns": build_union_groups(frames),
        "cleansing_policy": default_cleansing_policy(frames),
        "materialization_strategy": {
            "mode": "union-by-canonical-columns",
            "provenance_columns": ["_source_table", "_source_row_number"],
        },
        "review_notes": [
            "High confidence candidates can be approved directly.",
            "Low confidence or business-specific mappings should be refined with natural-language feedback.",
        ],
    }


def _canonical_group_index(proposal: dict[str, Any] | None) -> dict[str, dict[str, Any]]:
    groups = proposal.get("canonical_columns", []) if isinstance(proposal, dict) else []
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


def _member_set(group: dict[str, Any]) -> set[tuple[str, str]]:
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


def summarize_proposal_changes(previous: dict[str, Any] | None, current: dict[str, Any]) -> list[str]:
    if previous is None:
        return ["Initial proposal generated."]

    changes: list[str] = []
    if previous.get("feedback", "") != current.get("feedback", ""):
        changes.append("User feedback changed.")

    previous_groups = _canonical_group_index(previous)
    current_groups = _canonical_group_index(current)
    added = sorted(set(current_groups) - set(previous_groups))
    removed = sorted(set(previous_groups) - set(current_groups))
    for name in added[:8]:
        changes.append(f"Added canonical column: {name}.")
    for name in removed[:8]:
        changes.append(f"Removed canonical column: {name}.")

    for name in sorted(set(previous_groups) & set(current_groups)):
        previous_group = previous_groups[name]
        current_group = current_groups[name]
        if _member_set(previous_group) != _member_set(current_group):
            changes.append(f"Changed source members for canonical column: {name}.")
        for field in ["action", "decision", "review_required"]:
            if previous_group.get(field) != current_group.get(field):
                changes.append(f"Changed {field} for canonical column {name}: {previous_group.get(field)} -> {current_group.get(field)}.")

    previous_policy_count = len(previous.get("cleansing_policy", [])) if isinstance(previous.get("cleansing_policy", []), list) else 0
    current_policy_count = len(current.get("cleansing_policy", [])) if isinstance(current.get("cleansing_policy", []), list) else 0
    if previous_policy_count != current_policy_count:
        changes.append(f"Changed cleansing policy count: {previous_policy_count} -> {current_policy_count}.")

    return changes[:20] or ["No structural proposal changes detected."]


def apply_feedback_overrides(proposal: dict[str, Any], feedback: str) -> dict[str, Any]:
    updated = dict(proposal)
    if feedback:
        updated["feedback"] = feedback
        updated.setdefault("review_notes", []).append(f"User feedback considered: {feedback}")
    return updated


def llm_privacy_notice(settings: Settings | None = None) -> dict[str, Any]:
    settings = settings or Settings()
    return {
        "data_policy": settings.llm_data_policy,
        "sent_to_model": [
            "source table names",
            "source column names",
            "row counts",
            "column similarity candidates",
            "canonical column references",
            "cleansing action references",
            "user feedback text",
        ],
        "excluded_from_model": [
            "uploaded files",
            "raw rows",
            "sample values",
            "cell values",
        ],
    }


def build_llm_prompt(deterministic_proposal: dict[str, Any], settings: Settings | None = None) -> dict[str, Any]:
    return {
        "task": "Improve this data integration proposal. Return JSON only with the same top-level structure.",
        "privacy": llm_privacy_notice(settings),
        "proposal": deterministic_proposal,
    }


def _require_dict(value: Any, context: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise ProposalGenerationError(f"{context} must be an object")
    return value


def _require_list(value: Any, context: str, allow_empty: bool = False) -> list[Any]:
    if not isinstance(value, list):
        raise ProposalGenerationError(f"{context} must be a list")
    if not allow_empty and not value:
        raise ProposalGenerationError(f"{context} must not be empty")
    return value


def _require_string(value: Any, context: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ProposalGenerationError(f"{context} must be a non-empty string")
    return value


def validate_proposal_contract(proposal: dict[str, Any]) -> None:
    _require_dict(proposal, "proposal")
    for index, source in enumerate(_require_list(proposal.get("source_tables"), "source_tables")):
        item = _require_dict(source, f"source_tables[{index}]")
        _require_string(item.get("table"), f"source_tables[{index}].table")
        if not isinstance(item.get("rows"), int) or item["rows"] < 0:
            raise ProposalGenerationError(f"source_tables[{index}].rows must be a non-negative integer")
        for column_index, column in enumerate(_require_list(item.get("columns"), f"source_tables[{index}].columns")):
            _require_string(column, f"source_tables[{index}].columns[{column_index}]")

    for index, group in enumerate(_require_list(proposal.get("canonical_columns"), "canonical_columns")):
        item = _require_dict(group, f"canonical_columns[{index}]")
        _require_string(item.get("canonical_name"), f"canonical_columns[{index}].canonical_name")
        action = item.get("action")
        if action is not None and action not in ALLOWED_GROUP_ACTIONS:
            raise ProposalGenerationError(f"canonical_columns[{index}].action is not supported: {action}")
        confidence = item.get("confidence")
        if confidence is not None and (not isinstance(confidence, (int, float)) or not 0 <= float(confidence) <= 1):
            raise ProposalGenerationError(f"canonical_columns[{index}].confidence must be between 0 and 1")
        decision = item.get("decision")
        if decision is not None and decision not in ALLOWED_GROUP_DECISIONS:
            raise ProposalGenerationError(f"canonical_columns[{index}].decision is not supported: {decision}")
        review_required = item.get("review_required")
        if review_required is not None and not isinstance(review_required, bool):
            raise ProposalGenerationError(f"canonical_columns[{index}].review_required must be boolean")
        rationale = item.get("rationale")
        if rationale is not None:
            _require_string(rationale, f"canonical_columns[{index}].rationale")
        for member_index, member in enumerate(_require_list(item.get("members"), f"canonical_columns[{index}].members")):
            ref = _require_dict(member, f"canonical_columns[{index}].members[{member_index}]")
            _require_string(ref.get("table"), f"canonical_columns[{index}].members[{member_index}].table")
            _require_string(ref.get("column"), f"canonical_columns[{index}].members[{member_index}].column")

    for index, candidate in enumerate(_require_list(proposal.get("merge_candidates", []), "merge_candidates", allow_empty=True)):
        item = _require_dict(candidate, f"merge_candidates[{index}]")
        score = item.get("score")
        if score is not None and (not isinstance(score, (int, float)) or not 0 <= float(score) <= 1):
            raise ProposalGenerationError(f"merge_candidates[{index}].score must be between 0 and 1")
        if "left" in item:
            _require_dict(item["left"], f"merge_candidates[{index}].left")
        if "right" in item:
            _require_dict(item["right"], f"merge_candidates[{index}].right")

    for index, action in enumerate(_require_list(proposal.get("cleansing_policy", []), "cleansing_policy", allow_empty=True)):
        item = _require_dict(action, f"cleansing_policy[{index}]")
        if item.get("action") not in ALLOWED_CLEANSING_ACTIONS:
            raise ProposalGenerationError(f"cleansing_policy[{index}].action is not supported: {item.get('action')}")
        _require_string(item.get("table"), f"cleansing_policy[{index}].table")
        _require_string(item.get("column"), f"cleansing_policy[{index}].column")


def validate_proposal_references(proposal: dict[str, Any], frames: dict[str, pd.DataFrame]) -> None:
    validate_proposal_contract(proposal)
    known = {table: set(frame.columns) for table, frame in frames.items()}

    def check_ref(ref: dict[str, Any], context: str) -> None:
        table = ref.get("table")
        column = ref.get("column")
        if table not in known:
            raise ProposalGenerationError(f"{context} references unknown table: {table}")
        if column not in known[table]:
            raise ProposalGenerationError(f"{context} references unknown column: {table}.{column}")

    for group in proposal.get("canonical_columns", []):
        for member in group.get("members", []):
            check_ref(member, "canonical_columns")
    for candidate in proposal.get("merge_candidates", []):
        if "left" in candidate:
            check_ref(candidate["left"], "merge_candidates.left")
        if "right" in candidate:
            check_ref(candidate["right"], "merge_candidates.right")
    for action in proposal.get("cleansing_policy", []):
        check_ref(action, "cleansing_policy")


def generate_proposal(frames: dict[str, pd.DataFrame], feedback: str = "", settings: Settings | None = None) -> dict[str, Any]:
    deterministic = build_canonical_proposal(frames, feedback)
    effective = effective_openai_settings(settings)
    if not effective.llm_enabled:
        return deterministic
    try:
        from openai import OpenAI

        client = OpenAI(api_key=effective.api_key)
        prompt = build_llm_prompt(deterministic, settings)
        response = client.responses.create(
            model=effective.model,
            input=json.dumps(prompt, ensure_ascii=False),
            text={"format": {"type": "json_object"}},
        )
        content = response.output_text
        parsed = json.loads(content)
        parsed = make_json_safe(parsed)
        validate_proposal_references(parsed, frames)
        return parsed
    except Exception as exc:
        deterministic.setdefault("review_notes", []).append(f"LLM unavailable; deterministic proposal used: {exc}")
        return deterministic
