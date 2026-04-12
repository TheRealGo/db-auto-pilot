from __future__ import annotations

import copy
import json
import re
from collections import Counter, defaultdict, deque
from difflib import SequenceMatcher
from typing import Any

from app.config import Settings, effective_openai_settings

try:
    from openai import OpenAI
    from openai import OpenAIError
except ImportError:  # pragma: no cover
    OpenAI = None
    OpenAIError = Exception


SYNONYM_GROUPS = (
    {"customer", "client", "account", "顧客", "得意先", "取引先"},
    {"name", "title", "名称", "名前", "氏名"},
    {"department", "division", "section", "部署", "部門", "組織"},
    {"sales", "revenue", "amount", "売上", "金額", "金額合計", "請求額"},
    {"date", "day", "年月日", "日付", "計上日"},
    {"employee", "staff", "担当者", "社員"},
    {"product", "item", "sku", "商品", "製品"},
    {"count", "qty", "quantity", "件数", "数量"},
)


def similarity(left: str, right: str) -> float:
    return SequenceMatcher(None, left, right).ratio()


def tokenize(value: str) -> list[str]:
    return [token for token in re.split(r"[^0-9a-zA-Z一-龠ぁ-んァ-ヶ]+", value.lower()) if token]


def synonym_tokens(value: str) -> set[str]:
    tokens = set(tokenize(value))
    expanded = set(tokens)
    for group in SYNONYM_GROUPS:
        if tokens & group:
            expanded |= group
    return expanded or {value.lower()}


def column_signature(column: dict[str, Any]) -> dict[str, Any]:
    samples = [str(value).strip().lower() for value in column.get("sample_values", []) if value not in (None, "")]
    digit_ratio = 0.0
    if samples:
        digit_ratio = round(sum(any(character.isdigit() for character in sample) for sample in samples) / len(samples), 3)
    return {
        "logical_type": column["logical_type"],
        "null_ratio": float(column.get("null_ratio", 0.0)),
        "distinct_ratio": float(column.get("distinct_ratio", 0.0)),
        "digit_ratio": digit_ratio,
        "tokens": synonym_tokens(column["normalized_name"]),
    }


def compatibility_score(left: dict[str, Any], right: dict[str, Any]) -> float:
    name_score = similarity(left["normalized_name"], right["normalized_name"])
    token_overlap = len(column_signature(left)["tokens"] & column_signature(right)["tokens"])
    signature_left = column_signature(left)
    signature_right = column_signature(right)
    type_score = 1.0 if signature_left["logical_type"] == signature_right["logical_type"] else 0.45
    null_score = 1.0 - min(abs(signature_left["null_ratio"] - signature_right["null_ratio"]), 1.0)
    distinct_score = 1.0 - min(abs(signature_left["distinct_ratio"] - signature_right["distinct_ratio"]), 1.0)
    digit_score = 1.0 - min(abs(signature_left["digit_ratio"] - signature_right["digit_ratio"]), 1.0)
    token_score = min(token_overlap / 2.0, 1.0)
    return round(
        (name_score * 0.35)
        + (token_score * 0.25)
        + (type_score * 0.2)
        + (null_score * 0.1)
        + (distinct_score * 0.05)
        + (digit_score * 0.05),
        3,
    )


def cluster_source_tables(raw_tables: list[dict[str, Any]]) -> list[list[dict[str, Any]]]:
    adjacency: dict[str, set[str]] = {table["table_name"]: set() for table in raw_tables}
    by_name: dict[str, dict[str, Any]] = {table["table_name"]: table for table in raw_tables}
    for index, left in enumerate(raw_tables):
        for right in raw_tables[index + 1 :]:
            best_score = 0.0
            for left_column in left["columns"]:
                for right_column in right["columns"]:
                    best_score = max(best_score, compatibility_score(left_column, right_column))
            if best_score >= 0.62:
                adjacency[left["table_name"]].add(right["table_name"])
                adjacency[right["table_name"]].add(left["table_name"])
    visited: set[str] = set()
    groups: list[list[dict[str, Any]]] = []
    for table_name in adjacency:
        if table_name in visited:
            continue
        queue = deque([table_name])
        visited.add(table_name)
        component = []
        while queue:
            current = queue.popleft()
            component.append(by_name[current])
            for neighbor in adjacency[current]:
                if neighbor not in visited:
                    visited.add(neighbor)
                    queue.append(neighbor)
        groups.append(component)
    return groups


def mapping_decision(confidence: float, matches: list[dict[str, Any]]) -> tuple[bool, str]:
    if len(matches) <= 1:
        return False, "single_source"
    if confidence >= 0.83:
        return True, "merge"
    if confidence >= 0.68:
        return False, "uncertain"
    return False, "keep_separate"


def summarize_rationale(matches: list[dict[str, Any]], confidence: float) -> str:
    source_names = ", ".join(match["display_name"] for match in matches[:3])
    if confidence >= 0.83:
        return f"column names and value profiles align closely across {source_names}"
    if confidence >= 0.68:
        return f"column names are similar but need confirmation before merging: {source_names}"
    return f"column meanings look different enough to keep separate: {source_names}"


def profile_for_match(table: dict[str, Any], column: dict[str, Any]) -> dict[str, Any]:
    return {
        "source_table": table["table_name"],
        "source_column": column["db_name"],
        "display_name": column["original_name"],
        "logical_type": column["logical_type"],
        "sample_values": column.get("sample_values", []),
        "null_ratio": column.get("null_ratio", 0.0),
        "distinct_ratio": column.get("distinct_ratio", 0.0),
    }


def build_component_mappings(component_id: str, component: list[dict[str, Any]]) -> list[dict[str, Any]]:
    grouped: list[dict[str, Any]] = []
    for table in component:
        for column in table["columns"]:
            candidate_match = None
            for existing in grouped:
                reference = existing["matches"][0]
                probe = {
                    "normalized_name": reference["source_column"],
                    "logical_type": reference["logical_type"],
                    "null_ratio": reference["null_ratio"],
                    "distinct_ratio": reference["distinct_ratio"],
                    "sample_values": reference["sample_values"],
                }
                score = compatibility_score(column, probe)
                if score >= 0.68:
                    candidate_match = existing
                    existing["scores"].append(score)
                    break
            if candidate_match is None:
                grouped.append(
                    {
                        "canonical_name": column["normalized_name"],
                        "matches": [],
                        "scores": [],
                    }
                )
                candidate_match = grouped[-1]
            candidate_match["matches"].append(profile_for_match(table, column))

    mappings = []
    for entry in grouped:
        confidence = round(sum(entry["scores"]) / len(entry["scores"]), 3) if entry["scores"] else 0.58
        logical_type = Counter(match["logical_type"] for match in entry["matches"]).most_common(1)[0][0]
        merge_recommended, decision = mapping_decision(confidence, entry["matches"])
        mappings.append(
            {
                "component_id": component_id,
                "canonical_name": entry["canonical_name"],
                "logical_type": logical_type,
                "matches": entry["matches"],
                "rationale": summarize_rationale(entry["matches"], confidence),
                "confidence": confidence,
                "merge_recommended": merge_recommended,
                "decision": decision,
                "review_status": "needs_review" if decision == "uncertain" else "ready",
            }
        )
    return mappings


def merged_columns_from_mapping(mapping: dict[str, Any]) -> list[dict[str, Any]]:
    if mapping["decision"] == "keep_separate":
        columns = []
        seen: set[str] = set()
        for match in mapping["matches"]:
            name = match["source_column"]
            if name in seen:
                continue
            seen.add(name)
            columns.append(
                {
                    "name": name,
                    "logical_type": match["logical_type"],
                    "source_columns": [match],
                    "notes": mapping["rationale"],
                    "status": mapping["decision"],
                }
            )
        return columns
    return [
        {
            "name": mapping["canonical_name"],
            "logical_type": mapping["logical_type"],
            "source_columns": mapping["matches"],
            "notes": mapping["rationale"],
            "status": mapping["decision"],
        }
    ]


def rebuild_proposal_views(proposal: dict[str, Any]) -> dict[str, Any]:
    mappings = proposal["column_mappings"]
    raw_tables = proposal["raw_tables"]
    by_component: dict[str, list[dict[str, Any]]] = defaultdict(list)
    raw_tables_by_component: dict[str, list[str]] = defaultdict(list)
    table_component_map = proposal.get("table_component_map", {})
    for raw_table in raw_tables:
        raw_tables_by_component[table_component_map.get(raw_table["table_name"], "component_1")].append(raw_table["table_name"])
    for mapping in mappings:
        by_component[mapping["component_id"]].append(mapping)

    merged_tables = []
    schema_draft = []
    review_items = []
    for index, component_id in enumerate(sorted(by_component), start=1):
        component_mappings = by_component[component_id]
        merged_columns = []
        draft_columns = []
        for mapping in component_mappings:
            for column in merged_columns_from_mapping(mapping):
                merged_columns.append(column)
                draft_columns.append(
                    {
                        "name": column["name"],
                        "logical_type": column["logical_type"],
                        "status": column["status"],
                        "source_count": len(column["source_columns"]),
                        "rationale": column["notes"],
                    }
                )
            if mapping["decision"] in {"uncertain", "keep_separate"} and len(mapping["matches"]) > 1:
                review_items.append(
                    {
                        "type": mapping["decision"],
                        "canonical_name": mapping["canonical_name"],
                        "component_id": component_id,
                        "message": mapping["rationale"],
                        "matches": [
                            {
                                "source_table": match["source_table"],
                                "source_column": match["source_column"],
                                "display_name": match["display_name"],
                            }
                            for match in mapping["matches"]
                        ],
                    }
                )

        merged_tables.append(
            {
                "table_name": f"merged_{index}",
                "display_name": f"Merged Table {index}",
                "description": "Union view of related source tables after approval",
                "source_tables": raw_tables_by_component.get(component_id, []),
                "columns": merged_columns,
            }
        )
        schema_draft.append(
            {
                "component_id": component_id,
                "display_name": f"Merged Table {index}",
                "source_tables": raw_tables_by_component.get(component_id, []),
                "columns": draft_columns,
            }
        )

    proposal["merged_tables"] = merged_tables
    proposal["schema_draft"] = schema_draft
    proposal["review_items"] = review_items
    return proposal


def build_normalization_actions(raw_tables: list[dict[str, Any]]) -> list[dict[str, Any]]:
    actions = []
    for raw_table in raw_tables:
        for column in raw_table["columns"]:
            actions.append(
                {
                    "table_name": raw_table["table_name"],
                    "display_name": raw_table["display_name"],
                    "source_column": column["original_name"],
                    "normalized_column": column["db_name"],
                    "actions": ["trim_whitespace", "normalize_column_name"],
                }
            )
    return actions


def build_heuristic_proposal(
    dataset_name: str,
    raw_tables: list[dict[str, Any]],
    feedback: str | None = None,
) -> dict[str, Any]:
    groups = cluster_source_tables(raw_tables)
    table_component_map: dict[str, str] = {}
    column_mappings: list[dict[str, Any]] = []

    for index, component in enumerate(groups, start=1):
        component_id = f"component_{index}"
        for table in component:
            table_component_map[table["table_name"]] = component_id
        column_mappings.extend(build_component_mappings(component_id, component))

    proposal = {
        "dataset_name": dataset_name,
        "summary": "LLM-ready draft generated from source file structure and value profiles.",
        "raw_tables": raw_tables,
        "column_mappings": column_mappings,
        "table_component_map": table_component_map,
        "merged_tables": [],
        "schema_draft": [],
        "review_items": [],
        "normalization_actions": build_normalization_actions(raw_tables),
        "normalization_rules": [
            "trim whitespace from text cells",
            "normalize column names to snake_case for DB columns",
            "preserve source lineage columns for approved tables",
        ],
        "notes": [
            "Review items marked as uncertain before approval.",
            "Raw and merged tables will both be stored after approval.",
        ],
        "user_decisions": [],
    }
    rebuild_proposal_views(proposal)
    if feedback:
        apply_feedback_heuristics(proposal, feedback)
    return proposal


def feedback_targets(feedback: str) -> list[str]:
    return [token.lower() for token in re.findall(r"[A-Za-z0-9_一-龠ぁ-んァ-ヶ\.]+", feedback)]


def mapping_matches_feedback(mapping: dict[str, Any], feedback: str) -> bool:
    lowered = feedback.lower()
    targets = feedback_targets(feedback)
    haystacks = {
        mapping["canonical_name"].lower(),
        *[match["source_column"].lower() for match in mapping["matches"]],
        *[match["display_name"].lower() for match in mapping["matches"]],
        *[match["source_table"].lower() for match in mapping["matches"]],
    }
    return any(target in candidate for target in targets for candidate in haystacks) or any(
        candidate in lowered for candidate in haystacks
    )


def decision_from_feedback(feedback: str) -> str | None:
    lowered = feedback.lower()
    if "統合しない" in feedback or "まとめない" in feedback or "do not merge" in lowered:
        return "keep_separate"
    if "要確認" in feedback or "確認" in feedback or "review" in lowered:
        return "uncertain"
    if "統合して" in feedback or "merge" in lowered:
        return "merge"
    return None


def apply_feedback_heuristics(proposal: dict[str, Any], feedback: str) -> dict[str, Any]:
    decision = decision_from_feedback(feedback)
    if not decision:
        proposal.setdefault("notes", []).append(f"User feedback recorded: {feedback}")
        return proposal

    decisions_applied = []
    for mapping in proposal["column_mappings"]:
        if not mapping_matches_feedback(mapping, feedback):
            continue
        mapping["decision"] = decision
        mapping["merge_recommended"] = decision == "merge"
        mapping["review_status"] = "ready" if decision != "uncertain" else "needs_review"
        mapping["rationale"] = f"user feedback: {feedback}"
        decisions_applied.append(
            {
                "canonical_name": mapping["canonical_name"],
                "source_columns": [
                    {
                        "source_table": match["source_table"],
                        "source_column": match["source_column"],
                    }
                    for match in mapping["matches"]
                ],
                "decision": decision,
                "reason": feedback,
            }
        )

    if decisions_applied:
        existing = proposal.setdefault("user_decisions", [])
        existing.extend(decisions_applied)
        proposal["notes"].append(f"Applied user feedback to {len(decisions_applied)} mapping(s).")
        rebuild_proposal_views(proposal)
    else:
        proposal["notes"].append(f"Feedback saved but no matching columns were identified automatically: {feedback}")
    return proposal


def revise_proposal(existing_proposal: dict[str, Any], feedback: str) -> dict[str, Any]:
    proposal = copy.deepcopy(existing_proposal)
    return apply_feedback_heuristics(proposal, feedback)


def _openai_json_response(
    settings: Settings,
    system_prompt: str,
    user_payload: dict[str, Any],
) -> dict[str, Any] | None:
    openai_settings = effective_openai_settings(settings)
    if not openai_settings.api_key or OpenAI is None:
        return None
    client_options: dict[str, Any] = {"api_key": openai_settings.api_key}
    if openai_settings.endpoint:
        client_options["base_url"] = openai_settings.endpoint
    client = OpenAI(**client_options)
    try:
        response = client.responses.create(
            model=openai_settings.model,
            input=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": json.dumps(user_payload, ensure_ascii=False)},
            ],
        )
    except OpenAIError:
        return None
    text = getattr(response, "output_text", "").strip()
    if not text:
        return None
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        start = text.find("{")
        end = text.rfind("}")
        if start >= 0 and end > start:
            return json.loads(text[start : end + 1])
    return None


def generate_proposal(
    settings: Settings,
    dataset_name: str,
    raw_tables: list[dict[str, Any]],
    feedback: str | None = None,
) -> dict[str, Any]:
    fallback = build_heuristic_proposal(dataset_name, raw_tables, feedback)
    payload = {
        "dataset_name": dataset_name,
        "feedback": feedback,
        "raw_tables": raw_tables,
        "expected_shape": {
            "dataset_name": "string",
            "summary": "string",
            "raw_tables": [],
            "merged_tables": [],
            "schema_draft": [],
            "column_mappings": [],
            "review_items": [],
            "normalization_actions": [],
            "normalization_rules": [],
            "notes": [],
        },
    }
    system_prompt = (
        "You are designing a pre-ingestion data proposal for a dataset builder. "
        "Return valid JSON only. Preserve raw_tables exactly as provided. "
        "Generate merged_tables, schema_draft, column_mappings, review_items, "
        "normalization_actions, normalization_rules, and notes. "
        "Favor conservative merge recommendations when ambiguous."
    )
    response = _openai_json_response(settings, system_prompt, payload)
    if not response:
        return fallback
    response["raw_tables"] = raw_tables
    response.setdefault("table_component_map", fallback["table_component_map"])
    response.setdefault("user_decisions", [])
    response.setdefault("column_mappings", fallback["column_mappings"])
    response.setdefault("normalization_actions", fallback["normalization_actions"])
    response.setdefault("normalization_rules", fallback["normalization_rules"])
    response.setdefault("notes", fallback["notes"])
    rebuild_proposal_views(response)
    if feedback:
        response.setdefault("notes", []).append(f"User feedback considered: {feedback}")
    return response
