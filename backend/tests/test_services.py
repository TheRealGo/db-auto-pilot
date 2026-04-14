from __future__ import annotations

import pandas as pd

import app.services.materialization as materialization_module
from app.services.materialization import (
    MaterializationExecutionError,
    MaterializationError,
    MaterializationTimeoutError,
    build_deterministic_materialization_code,
    build_materialization_retry_guidance,
    execute_materialization_code,
    validate_generated_code,
    validate_materialization_proposal_payload,
)
from app.services.proposals import (
    build_comparison_candidates,
    build_observations,
    generate_proposal,
    parse_feedback_overrides,
    run_observation_request,
)
from app.services.querying import schema_prompt


class DummySettings:
    pass


def test_generate_proposal_runs_multi_step_loop(monkeypatch) -> None:
    raw_tables = [
        {
            "table_name": "raw_sales",
            "display_name": "sales.csv / sheet1",
            "source_filename": "sales.csv",
            "sheet_name": "sheet1",
            "row_count": 2,
            "columns": [
                {
                    "original_name": "customer_name",
                    "normalized_name": "customer_name",
                    "db_name": "customer_name",
                    "logical_type": "text",
                },
                {
                    "original_name": "amount",
                    "normalized_name": "amount",
                    "db_name": "amount",
                    "logical_type": "number",
                },
            ],
        },
        {
            "table_name": "raw_sales_jp",
            "display_name": "sales_jp.csv / sheet1",
            "source_filename": "sales_jp.csv",
            "sheet_name": "sheet1",
            "row_count": 2,
            "columns": [
                {
                    "original_name": "顧客名",
                    "normalized_name": "顧客名",
                    "db_name": "顧客名",
                    "logical_type": "text",
                },
                {
                    "original_name": "売上",
                    "normalized_name": "売上",
                    "db_name": "売上",
                    "logical_type": "number",
                },
            ],
        },
    ]
    dataframe_map = {
        "raw_sales": pd.DataFrame(
            {
                "_row_index": [1, 2],
                "_source_file": ["sales.csv", "sales.csv"],
                "_source_sheet": ["sheet1", "sheet1"],
                "customer_name": ["Alice", "Bob"],
                "amount": [100, 120],
            }
        ),
        "raw_sales_jp": pd.DataFrame(
            {
                "_row_index": [1, 2],
                "_source_file": ["sales_jp.csv", "sales_jp.csv"],
                "_source_sheet": ["sheet1", "sheet1"],
                "顧客名": ["Alice", "Bob"],
                "売上": [100, 120],
            }
        ),
    }

    responses = iter(
        [
            {
                "mode": "observe",
                "notes": ["Need a row sample before deciding."],
                "observation_requests": [
                    {
                        "tool": "sample_rows",
                        "args": {"table_name": "raw_sales", "columns": ["customer_name"], "limit": 2},
                        "reason": "Compare row values.",
                    }
                ],
            },
            {
                "mode": "finalize",
                "notes": ["Enough evidence gathered."],
                "final_proposal": {
                    "summary": "customer_name and 顧客名 should merge.",
                    "decisions": [
                        {
                            "candidate_id": "cand_001",
                            "action": "merge",
                            "canonical_name": "customer_name",
                            "reason": "Names and sample values align.",
                        }
                    ],
                    "normalization_plan": [],
                    "questions_for_user": [],
                    "notes": ["Merged likely customer columns."],
                },
            },
        ]
    )

    monkeypatch.setattr("app.services.proposals.ensure_openai_available", lambda settings: None)
    monkeypatch.setattr("app.services.proposals._openai_json_response", lambda settings, system_prompt, payload: next(responses))

    proposal = generate_proposal(DummySettings(), "sales", raw_tables, dataframe_map)

    assert proposal["agent_steps"]
    assert proposal["agent_steps"][0]["mode"] == "observe"
    assert proposal["summary"] == "customer_name and 顧客名 should merge."
    assert proposal["merge_decisions"]
    assert proposal["canonical_proposal"]["overview"]["merged_component_count"] >= 1
    assert proposal["canonical_proposal"]["candidates"][0]["canonical_name"] == "customer_name"


def test_generate_proposal_applies_feedback_override(monkeypatch) -> None:
    raw_tables = [
        {
            "table_name": "raw_sales",
            "display_name": "sales.csv / sheet1",
            "source_filename": "sales.csv",
            "sheet_name": "sheet1",
            "row_count": 2,
            "columns": [{"original_name": "customer_name", "normalized_name": "customer_name", "db_name": "customer_name", "logical_type": "text"}],
        },
        {
            "table_name": "raw_sales_jp",
            "display_name": "sales_jp.csv / sheet1",
            "source_filename": "sales_jp.csv",
            "sheet_name": "sheet1",
            "row_count": 2,
            "columns": [{"original_name": "顧客名", "normalized_name": "顧客名", "db_name": "顧客名", "logical_type": "text"}],
        },
    ]
    dataframe_map = {
        "raw_sales": pd.DataFrame({"customer_name": ["Alice", "Bob"]}),
        "raw_sales_jp": pd.DataFrame({"顧客名": ["Alice", "Bob"]}),
    }
    responses = iter(
        [
            {
                "mode": "finalize",
                "notes": [],
                "final_proposal": {
                    "summary": "merge them",
                    "decisions": [{"candidate_id": "cand_001", "action": "merge", "canonical_name": "customer_name", "reason": "looks same"}],
                    "normalization_plan": [],
                    "questions_for_user": [],
                    "notes": [],
                },
            }
        ]
    )

    monkeypatch.setattr("app.services.proposals.ensure_openai_available", lambda settings: None)
    monkeypatch.setattr("app.services.proposals._openai_json_response", lambda settings, system_prompt, payload: next(responses))

    proposal = generate_proposal(
        DummySettings(),
        "sales",
        raw_tables,
        dataframe_map,
        feedback="customer_name と 顧客名 は統合しないでください",
    )

    assert proposal["review_items"][0]["severity"] == "blocking"
    assert proposal["review_items"][0]["override_applied"] is True
    assert proposal["canonical_proposal"]["overview"]["blocking_review_count"] == 1


def test_build_comparison_candidates_uses_value_overlap_signal() -> None:
    raw_tables = [
        {
            "table_name": "raw_sales",
            "display_name": "sales.csv / sheet1",
            "source_filename": "sales.csv",
            "sheet_name": "sheet1",
            "row_count": 2,
            "columns": [
                {
                    "original_name": "client",
                    "normalized_name": "client",
                    "normalized_candidates": ["client"],
                    "db_name": "client",
                    "logical_type": "text",
                }
            ],
        },
        {
            "table_name": "raw_customers",
            "display_name": "customers.csv / sheet1",
            "source_filename": "customers.csv",
            "sheet_name": "sheet1",
            "row_count": 2,
            "columns": [
                {
                    "original_name": "account_name",
                    "normalized_name": "account_name",
                    "normalized_candidates": ["account_name", "accountname"],
                    "db_name": "account_name",
                    "logical_type": "text",
                }
            ],
        },
    ]
    dataframe_map = {
        "raw_sales": pd.DataFrame({"client": ["Acme", "Globex"]}),
        "raw_customers": pd.DataFrame({"account_name": ["ACME", "Globex"]}),
    }

    observations = build_observations(raw_tables, dataframe_map)
    candidates = build_comparison_candidates(observations)

    assert candidates
    assert candidates[0]["value_overlap"] > 0
    assert "sample values overlap" in candidates[0]["reasoning"]


def test_parse_feedback_overrides_extracts_keep_separate_and_canonical_name() -> None:
    observations = [
        {
            "table_name": "raw_a",
            "display_name": "A",
            "source_filename": "a.csv",
            "sheet_name": "sheet1",
            "row_count": 2,
            "columns": [
                {
                    "source_table": "raw_a",
                    "source_display_name": "A",
                    "source_column": "customer_name",
                    "display_name": "customer_name",
                    "normalized_name": "customer_name",
                    "normalized_candidates": ["customer_name"],
                    "logical_type": "text",
                    "tokens": ["customer", "name"],
                    "profile": {"unique_samples": ["Alice"], "distinct_ratio": 1.0, "numeric_ratio": 0.0, "datetime_ratio": 0.0, "sample_values": ["Alice"]},
                }
            ],
        },
        {
            "table_name": "raw_b",
            "display_name": "B",
            "source_filename": "b.csv",
            "sheet_name": "sheet1",
            "row_count": 2,
            "columns": [
                {
                    "source_table": "raw_b",
                    "source_display_name": "B",
                    "source_column": "顧客名",
                    "display_name": "顧客名",
                    "normalized_name": "顧客名",
                    "normalized_candidates": ["顧客名"],
                    "logical_type": "text",
                    "tokens": ["顧客"],
                    "profile": {"unique_samples": ["Alice"], "distinct_ratio": 1.0, "numeric_ratio": 0.0, "datetime_ratio": 0.0, "sample_values": ["Alice"]},
                }
            ],
        },
    ]
    comparison_candidates = [
        {
            "candidate_id": "cand_001",
            "left": {"source_table": "raw_a", "source_column": "customer_name"},
            "right": {"source_table": "raw_b", "source_column": "顧客名"},
        }
    ]

    keep_separate = parse_feedback_overrides("customer_name と 顧客名 は統合しないでください", observations, comparison_candidates)
    rename = parse_feedback_overrides('customer_name と 顧客名 を merge して canonical は customer_master', observations, comparison_candidates)

    assert keep_separate["overrides"][0]["type"] == "keep_separate"
    assert rename["overrides"][0]["type"] == "force_merge"
    assert rename["overrides"][0]["canonical_name"] == "customer_master"


def test_run_observation_request_supports_table_pair_and_group_compare() -> None:
    raw_tables = [
        {
            "table_name": "raw_sales",
            "display_name": "sales.csv / sheet1",
            "source_filename": "sales.csv",
            "sheet_name": "sheet1",
            "row_count": 2,
            "columns": [
                {"original_name": "customer_name", "normalized_name": "customer_name", "db_name": "customer_name", "logical_type": "text"},
                {"original_name": "department", "normalized_name": "department", "db_name": "department", "logical_type": "text"},
            ],
        },
        {
            "table_name": "raw_customers",
            "display_name": "customers.csv / sheet1",
            "source_filename": "customers.csv",
            "sheet_name": "sheet1",
            "row_count": 2,
            "columns": [
                {"original_name": "顧客名", "normalized_name": "顧客名", "db_name": "顧客名", "logical_type": "text"},
                {"original_name": "部署", "normalized_name": "部署", "db_name": "部署", "logical_type": "text"},
            ],
        },
    ]
    dataframe_map = {
        "raw_sales": pd.DataFrame({"customer_name": ["Alice", "Bob"], "department": ["Ops", "Sales"]}),
        "raw_customers": pd.DataFrame({"顧客名": ["Alice", "Bob"], "部署": ["Ops", "Sales"]}),
    }
    observations = build_observations(raw_tables, dataframe_map)
    comparison_candidates = build_comparison_candidates(observations)

    table_pair = run_observation_request(
        {"tool": "table_pair_overlap", "args": {"left_table": "raw_sales", "right_table": "raw_customers"}},
        observations,
        comparison_candidates,
        dataframe_map,
    )
    group_compare = run_observation_request(
        {
            "tool": "column_group_compare",
            "args": {
                "left_group": [{"source_table": "raw_sales", "source_column": "customer_name"}],
                "right_group": [{"source_table": "raw_customers", "source_column": "顧客名"}],
            },
        },
        observations,
        comparison_candidates,
        dataframe_map,
    )

    assert table_pair["top_pairs"]
    assert group_compare["group_score"] > 0


def test_validate_generated_code_rejects_dangerous_import() -> None:
    code = "import os\nresult = {'merged_tables': [], 'lineage_items': [], 'execution_notes': []}"
    try:
        validate_generated_code(code)
    except Exception as exc:
        assert "violated safety rules" in str(exc)
    else:  # pragma: no cover
        raise AssertionError("dangerous import should have been rejected")


def test_validate_generated_code_rejects_introspection_calls() -> None:
    code = "value = globals()\nresult = {'merged_tables': [], 'lineage_items': [], 'execution_notes': []}"
    try:
        validate_generated_code(code)
    except Exception as exc:
        assert "call:globals" in str(exc)
    else:  # pragma: no cover
        raise AssertionError("introspection call should have been rejected")


def test_execute_materialization_code_rejects_missing_provenance() -> None:
    code = """
result = {
    "merged_tables": [
        {
            "component_id": "component_1",
            "display_name": "Merged Table 1",
            "physical_name": "merged_table_1",
            "dataframe": pd.DataFrame({"customer_name": ["Alice"]}),
        }
    ],
    "lineage_items": [],
    "execution_notes": [],
}
    """.strip()
    source_frames = {
        "raw_sales": pd.DataFrame(
            {
                "_row_index": [1],
                "_source_file": ["sales.csv"],
                "_source_sheet": ["sheet1"],
                "customer_name": ["Alice"],
            }
        )
    }
    plan = {"components": [{"component_id": "component_1"}]}
    try:
        execute_materialization_code(code, source_frames, plan, {"status": "passed", "imports": [], "violations": []})
    except MaterializationExecutionError as exc:
        assert "provenance columns" in str(exc)
    else:  # pragma: no cover
        raise AssertionError("missing provenance should have been rejected")


def test_execute_materialization_code_applies_repairs() -> None:
    code = """
result = {
    "merged_tables": [
        {
            "component_id": "component_1",
            "dataframe": pd.DataFrame(
                {
                    "customer_name": ["Alice"],
                    "_source_row_index": [1],
                    "_source_sheet": ["sheet1"],
                    "_source_file": ["sales.csv"],
                    "_source_table": ["raw_sales"],
                }
            ),
        }
    ],
    "lineage_items": [{"table_name": "Merged Table 1", "column_name": "customer_name"}],
}
    """.strip()
    source_frames = {
        "raw_sales": pd.DataFrame(
            {
                "_row_index": [1],
                "_source_file": ["sales.csv"],
                "_source_sheet": ["sheet1"],
                "customer_name": ["Alice"],
            }
        )
    }
    plan = {
        "components": [
            {
                "component_id": "component_1",
                "display_name": "Merged Table 1",
                "physical_name": "merged_table_1",
            }
        ]
    }

    result = execute_materialization_code(code, source_frames, plan, {"status": "passed", "imports": [], "violations": []})

    assert result["repair_summary"]["status"] == "repaired"
    assert "filled_display_name" in result["repair_summary"]["applied_repairs"]
    assert "filled_physical_name" in result["repair_summary"]["applied_repairs"]


def test_validate_materialization_proposal_payload_rejects_unknown_column() -> None:
    payload = {
        "summary": "proposal",
        "normalization_decisions": [
            {
                "component_id": "component_1",
                "column_name": "missing_column",
                "actions": ["normalize_text"],
                "config": {},
                "reason": "normalize",
            }
        ],
        "transformation_notes": [],
        "risk_notes": [],
        "expected_outputs": [],
        "quality_expectations": [],
    }
    plan = {
        "components": [
            {
                "component_id": "component_1",
                "columns": [{"name": "customer_name", "logical_type": "text"}],
            }
        ]
    }

    try:
        validate_materialization_proposal_payload(payload, plan)
    except MaterializationError as exc:
        assert "unknown column" in str(exc)
    else:  # pragma: no cover
        raise AssertionError("unknown column should have been rejected")


def test_execute_materialization_code_applies_normalization_and_quality_summary() -> None:
    code = """
result = {
    "merged_tables": [
        {
            "component_id": "component_1",
            "display_name": "Merged Table 1",
            "physical_name": "merged_table_1",
            "dataframe": pd.DataFrame(
                {
                    "customer_name": [" Alice  ", "BOB"],
                    "amount": ["1,200", "80%"],
                    "event_date": ["2024/01/02", "bad-date"],
                    "_source_row_index": [1, 2],
                    "_source_sheet": ["sheet1", "sheet1"],
                    "_source_file": ["sales.csv", "sales.csv"],
                    "_source_table": ["raw_sales", "raw_sales"],
                }
            ),
        }
    ],
    "lineage_items": [
        {"table_name": "Merged Table 1", "column_name": "customer_name", "source_columns": [], "status": "merge"},
        {"table_name": "Merged Table 1", "column_name": "amount", "source_columns": [], "status": "merge"},
        {"table_name": "Merged Table 1", "column_name": "event_date", "source_columns": [], "status": "merge"},
    ],
    "execution_notes": [],
}
    """.strip()
    source_frames = {
        "raw_sales": pd.DataFrame(
            {
                "_row_index": [1, 2],
                "_source_file": ["sales.csv", "sales.csv"],
                "_source_sheet": ["sheet1", "sheet1"],
                "customer_name": [" Alice  ", "BOB"],
                "amount": ["1,200", "80%"],
                "event_date": ["2024/01/02", "bad-date"],
            }
        )
    }
    plan = {
        "components": [
            {
                "component_id": "component_1",
                "display_name": "Merged Table 1",
                "physical_name": "merged_table_1",
                "columns": [
                    {"name": "customer_name", "logical_type": "text"},
                    {"name": "amount", "logical_type": "number"},
                    {"name": "event_date", "logical_type": "date"},
                ],
            }
        ]
    }
    decisions = [
        {
            "component_id": "component_1",
            "column_name": "customer_name",
            "actions": ["normalize_text", "lowercase"],
            "config": {},
            "reason": "normalize text",
        },
        {
            "component_id": "component_1",
            "column_name": "amount",
            "actions": ["strip_non_numeric", "parse_number"],
            "config": {"percent_mode": "auto"},
            "reason": "normalize number",
        },
        {
            "component_id": "component_1",
            "column_name": "event_date",
            "actions": ["parse_date"],
            "config": {},
            "reason": "normalize date",
        },
    ]

    result = execute_materialization_code(
        code,
        source_frames,
        plan,
        {"status": "passed", "imports": [], "violations": []},
        decisions,
    )

    dataframe = result["merged_tables"][0]["dataframe"]
    assert dataframe["customer_name"].tolist() == ["alice", "bob"]
    assert dataframe["amount"].tolist() == [1200.0, 0.8]
    assert str(dataframe["event_date"].iloc[0]).startswith("2024-01-02")
    assert result["quality_summary"]["status"] == "completed"
    assert result["warnings"]
    assert any(
        column_summary["warning_details"]
        for column_summary in result["quality_summary"]["table_summaries"][0]["column_summaries"]
    )


def test_build_materialization_retry_guidance_extracts_quality_issues() -> None:
    run_result = {
        "error_stage": "execution",
        "error": "bad parse",
        "guard_summary": {"violations": [{"type": "import", "value": "os"}]},
        "warnings": ["Merged Table 1.amount: low_numeric_parse_ratio"],
        "quality_summary": {
            "warning_count": 1,
            "table_summaries": [
                {
                    "component_id": "component_1",
                    "display_name": "Merged Table 1",
                    "column_summaries": [
                        {
                            "column_name": "amount",
                            "actions": ["parse_number"],
                            "warnings": ["low_numeric_parse_ratio"],
                        }
                    ],
                }
            ],
        },
    }

    guidance = build_materialization_retry_guidance(
        run_result,
        {"summary": "merge sales data"},
        previous_run_id="run_123",
    )

    assert guidance["reason"] == "failed_run_retry"
    assert guidance["previous_run_id"] == "run_123"
    assert guidance["previous_error_stage"] == "execution"
    assert guidance["guard_violations"] == ["import:os"]
    assert guidance["quality_warnings"]
    assert guidance["column_issues"][0]["column_name"] == "amount"
    assert guidance["column_patches"][0]["component_id"] == "component_1"
    assert "strip_non_numeric" in guidance["column_patches"][0]["suggested_actions"]
    assert guidance["column_patches"][0]["warning_details"][0]["code"] == "low_numeric_parse_ratio"
    assert guidance["proposal_summary"] == "merge sales data"


def test_validate_materialization_proposal_payload_fills_suggested_mapping() -> None:
    payload = {
        "summary": "proposal",
        "normalization_decisions": [
            {
                "component_id": "component_1",
                "column_name": "customer_name",
                "actions": ["normalize_text", "map_values"],
                "config": {},
                "reason": "normalize aliases",
            }
        ],
        "transformation_notes": [],
        "risk_notes": [],
        "expected_outputs": [],
        "quality_expectations": [],
    }
    plan = {
        "components": [
            {
                "component_id": "component_1",
                "columns": [
                    {
                        "name": "customer_name",
                        "logical_type": "text",
                        "suggested_value_mapping": {" Alice  ": "Alice", "ALICE": "Alice"},
                    }
                ],
            }
        ]
    }

    validated = validate_materialization_proposal_payload(payload, plan)

    assert validated["normalization_decisions"][0]["config"]["mapping"][" Alice  "] == "Alice"


def test_schema_prompt_includes_query_hints_and_provenance() -> None:
    tables = [
        {
            "mode": "merged",
            "table_name": "merged_sales",
            "display_name": "Merged Sales",
            "schema": {
                "columns": [
                    {"name": "department", "logical_type": "text"},
                    {"name": "amount", "logical_type": "number"},
                    {"name": "event_date", "logical_type": "datetime"},
                    {"name": "_source_file", "logical_type": "text"},
                ],
                "lineage": [
                    {
                        "column_name": "department",
                        "source_columns": [{"source_table": "raw_sales", "source_column": "dept"}],
                        "status": "merge",
                    }
                ],
            },
        }
    ]

    payload = schema_prompt(tables)

    assert payload[0]["query_hints"]["metric_columns"] == ["amount"]
    assert payload[0]["query_hints"]["datetime_columns"] == ["event_date"]
    assert payload[0]["columns"][-1]["is_provenance"] is True


def test_build_deterministic_materialization_code_executes_plan() -> None:
    plan = {
        "components": [
            {
                "component_id": "component_1",
                "display_name": "Merged Table 1",
                "physical_name": "merged_table_1",
                "source_tables": ["raw_sales"],
                "columns": [
                    {
                        "name": "customer_name",
                        "status": "merge",
                        "source_columns": [
                            {
                                "source_table": "raw_sales",
                                "source_column": "customer_name",
                                "display_name": "Customer Name",
                            }
                        ],
                    }
                ],
            }
        ]
    }
    source_frames = {
        "raw_sales": pd.DataFrame(
            {
                "_row_index": [1, 2],
                "_source_file": ["sales.csv", "sales.csv"],
                "_source_sheet": ["sheet1", "sheet1"],
                "customer_name": ["Alice", "Bob"],
            }
        )
    }

    code = build_deterministic_materialization_code(plan)
    result = execute_materialization_code(code, source_frames, plan, validate_generated_code(code))

    dataframe = result["merged_tables"][0]["dataframe"]
    assert dataframe["customer_name"].tolist() == ["Alice", "Bob"]
    assert dataframe["_source_table"].tolist() == ["raw_sales", "raw_sales"]


def test_execute_materialization_code_times_out(monkeypatch) -> None:
    monkeypatch.setattr(materialization_module, "MATERIALIZATION_TIMEOUT_SECONDS", 1)
    code = "while True:\n    pass"
    source_frames = {
        "raw_sales": pd.DataFrame(
            {
                "_row_index": [1],
                "_source_file": ["sales.csv"],
                "_source_sheet": ["sheet1"],
                "customer_name": ["Alice"],
            }
        )
    }
    plan = {"components": []}

    try:
        execute_materialization_code(code, source_frames, plan, {"status": "passed", "imports": [], "violations": []})
    except MaterializationTimeoutError as exc:
        assert "timeout" in str(exc).lower()
    else:  # pragma: no cover
        raise AssertionError("timeout should have been raised")
