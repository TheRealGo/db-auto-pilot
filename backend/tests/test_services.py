from __future__ import annotations

import sqlite3
import tempfile
import unittest
from pathlib import Path

import pandas as pd

from app.repository import MetadataRepository
from app.services.ingestion import infer_logical_type, normalize_column_names, read_tabular_file
from app.services.materialization import MaterializationExecutionError, MaterializationGuardError, _publish_staging_table, draft_materialization_plan, materialize_frames, summarize_materialization_plan_changes, validate_materialization_plan_references
from app.services.proposals import ProposalGenerationError, build_canonical_proposal, build_llm_prompt, build_union_groups, summarize_proposal_changes, validate_proposal_contract, validate_proposal_references
from app.services.querying import QueryGenerationError, fallback_query, run_query, validate_select_sql


class ServiceTests(unittest.TestCase):
    def test_normalizes_duplicate_columns(self) -> None:
        self.assertEqual(normalize_column_names(["部署", "部署", "売上 金額"]), ["部署", "部署_2", "売上_金額"])

    def test_infers_basic_types(self) -> None:
        self.assertEqual(infer_logical_type(pd.Series([1, 2, 3])), "integer")
        self.assertEqual(infer_logical_type(pd.Series(["営業", "開発", "営業"])), "category")

    def test_read_tabular_file_rejects_empty_csv(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "empty.csv"
            path.write_text("", encoding="utf-8")
            with self.assertRaisesRegex(ValueError, "Unable to parse"):
                read_tabular_file(path)

            header_only = Path(tmp) / "header-only.csv"
            header_only.write_text("department,sales\n", encoding="utf-8")
            with self.assertRaisesRegex(ValueError, "no data rows"):
                read_tabular_file(header_only)

    def test_materializes_union_with_provenance(self) -> None:
        frames = {
            "sales_a": pd.DataFrame({"department": ["営業"], "sales": [100]}),
            "sales_b": pd.DataFrame({"dept": ["開発"], "amount": [250]}),
        }
        proposal = build_canonical_proposal(frames)
        plan = draft_materialization_plan(proposal)
        with tempfile.TemporaryDirectory() as tmp:
            result = materialize_frames(Path(tmp) / "app.db", "dataset-1", frames, plan)
            self.assertEqual(result["rows"], 2)
            self.assertIn("_source_table", result["columns"])
            self.assertEqual(result["mode"], "merged")

    def test_materialization_plan_includes_review_information(self) -> None:
        frames = {
            "left": pd.DataFrame({"abc": [1]}),
            "right": pd.DataFrame({"abd": [2]}),
        }
        proposal = build_canonical_proposal(frames)
        plan = draft_materialization_plan(proposal, "confirm weak mapping")

        self.assertEqual(plan["expected_output"]["estimated_rows"], 2)
        self.assertIn("_source_table", plan["expected_output"]["columns"])
        self.assertTrue(plan["quality_expectations"])
        self.assertTrue(plan["risk_notes"])
        self.assertIn("confirm weak mapping", plan["feedback"])

    def test_summarizes_materialization_plan_revision_changes(self) -> None:
        frames = {"sales": pd.DataFrame({"department": ["ops"], "sales": [10]})}
        proposal = build_canonical_proposal(frames)
        first = draft_materialization_plan(proposal, "first")
        second = draft_materialization_plan(proposal, "second")
        second["retry"] = {"failed_materialization_run_id": "run-1"}

        self.assertEqual(summarize_materialization_plan_changes(None, first), ["Initial materialization proposal generated."])
        changes = summarize_materialization_plan_changes(first, second)
        self.assertIn("Materialization feedback changed.", changes)
        self.assertIn("Added retry evidence from a failed materialization run.", changes)

    def test_materialization_result_includes_lineage_and_quality_warnings(self) -> None:
        frames = {
            "sales_a": pd.DataFrame({"部署": ["営業"], "売上": [100]}),
            "sales_b": pd.DataFrame({"部門": ["開発"], "売上金額": [250], "備考": ["重点"]}),
        }
        proposal = build_canonical_proposal(frames)
        plan = draft_materialization_plan(proposal)
        with tempfile.TemporaryDirectory() as tmp:
            result = materialize_frames(Path(tmp) / "app.db", "dataset-jp", frames, plan)
        self.assertTrue(any(item["source_column"] == "部署" for item in result["lineage"]))
        self.assertTrue(any(item["source_column"] == "部門" for item in result["lineage"]))
        self.assertTrue(any(warning["code"] == "missing_source_column" and warning["output_column"] == "備考" for warning in result["quality_warnings"]))

    def test_materialization_rejects_row_and_column_limit_before_publish(self) -> None:
        frames = {"sales": pd.DataFrame({"department": ["ops", "dev"], "sales": [10, 20]})}
        plan = draft_materialization_plan(build_canonical_proposal(frames))
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "app.db"
            with self.assertRaisesRegex(MaterializationExecutionError, "row limit"):
                materialize_frames(db_path, "dataset-rows", frames, plan, max_rows=1)
            with sqlite3.connect(db_path) as conn:
                self.assertFalse(conn.execute("select 1 from sqlite_master where name like 'dataset_dataset_rows%'").fetchone())

        wide_frames = {"wide": pd.DataFrame({"a": [1], "b": [2], "c": [3]})}
        wide_plan = {"canonical_columns": [], "cleansing_policy": []}
        with tempfile.TemporaryDirectory() as tmp:
            with self.assertRaisesRegex(MaterializationExecutionError, "column limit"):
                materialize_frames(Path(tmp) / "app.db", "dataset-cols", wide_frames, wide_plan, max_columns=4)

    def test_publish_staging_table_rolls_back_when_backup_name_collides(self) -> None:
        with sqlite3.connect(":memory:") as conn:
            conn.execute("create table dataset_1(value text)")
            conn.execute("insert into dataset_1 values ('old')")
            conn.execute("create table dataset_1_staging(value text)")
            conn.execute("insert into dataset_1_staging values ('new')")
            conn.execute("create table dataset_1_backup_collision(value text)")
            conn.commit()

            with self.assertRaises(sqlite3.DatabaseError):
                _publish_staging_table(conn, "dataset_1_staging", "dataset_1", "dataset_1_backup_collision")

            current = conn.execute("select value from dataset_1").fetchall()
            staging = conn.execute("select value from dataset_1_staging").fetchall()
            self.assertEqual(current, [("old",)])
            self.assertEqual(staging, [("new",)])

    def test_publish_staging_table_replaces_existing_table_atomically(self) -> None:
        with sqlite3.connect(":memory:") as conn:
            conn.execute("create table dataset_1(value text)")
            conn.execute("insert into dataset_1 values ('old')")
            conn.execute("create table dataset_1_staging(value text)")
            conn.execute("insert into dataset_1_staging values ('new')")
            conn.commit()

            _publish_staging_table(conn, "dataset_1_staging", "dataset_1", "dataset_1_backup")

            self.assertEqual(conn.execute("select value from dataset_1").fetchall(), [("new",)])
            self.assertFalse(conn.execute("select 1 from sqlite_master where name = 'dataset_1_backup'").fetchone())

    def test_materialization_plan_validation_rejects_invented_references(self) -> None:
        frames = {"sales": pd.DataFrame({"department": ["ops"], "sales": [10]})}
        plan = draft_materialization_plan(build_canonical_proposal(frames))
        validate_materialization_plan_references(plan, frames)
        plan["canonical_columns"][0]["members"][0]["table"] = "invented"
        with self.assertRaises(MaterializationGuardError):
            validate_materialization_plan_references(plan, frames)

    def test_materialization_rejects_output_column_collisions(self) -> None:
        frames = {"sales": pd.DataFrame({"department": ["ops"], "dept": ["ops"], "sales": [10]})}
        duplicate_plan = {
            "canonical_columns": [
                {"canonical_name": "Customer ID", "members": [{"table": "sales", "column": "department"}]},
                {"canonical_name": "customer_id", "members": [{"table": "sales", "column": "dept"}]},
            ],
            "cleansing_policy": [],
        }
        with self.assertRaisesRegex(MaterializationGuardError, "duplicate output column"):
            validate_materialization_plan_references(duplicate_plan, frames)

        reserved_plan = {
            "canonical_columns": [
                {"canonical_name": "_source_table", "members": [{"table": "sales", "column": "department"}]},
            ],
            "cleansing_policy": [],
        }
        with self.assertRaisesRegex(MaterializationGuardError, "reserved provenance"):
            validate_materialization_plan_references(reserved_plan, frames)

    def test_materialization_rejects_source_provenance_column_collisions(self) -> None:
        frames = {"sales": pd.DataFrame({"_source_table": ["legacy"], "sales": [10]})}
        with tempfile.TemporaryDirectory() as tmp:
            with self.assertRaisesRegex(MaterializationGuardError, "reserved provenance"):
                materialize_frames(Path(tmp) / "app.db", "dataset-reserved", frames, {"canonical_columns": [], "cleansing_policy": []})

    def test_proposal_validation_rejects_invented_references(self) -> None:
        frames = {"sales": pd.DataFrame({"department": ["ops"], "sales": [10]})}
        proposal = build_canonical_proposal(frames)
        validate_proposal_references(proposal, frames)
        proposal["canonical_columns"][0]["members"][0]["column"] = "invented"
        with self.assertRaises(ProposalGenerationError):
            validate_proposal_references(proposal, frames)

    def test_proposal_contract_rejects_invalid_llm_shape(self) -> None:
        frames = {"sales": pd.DataFrame({"department": ["ops"], "sales": [10]})}
        proposal = build_canonical_proposal(frames)

        malformed_columns = dict(proposal)
        malformed_columns["canonical_columns"] = "not a list"
        with self.assertRaisesRegex(ProposalGenerationError, "canonical_columns"):
            validate_proposal_contract(malformed_columns)

        bad_confidence = build_canonical_proposal(frames)
        bad_confidence["canonical_columns"][0]["confidence"] = 1.5
        with self.assertRaisesRegex(ProposalGenerationError, "confidence"):
            validate_proposal_contract(bad_confidence)

        bad_cleansing = build_canonical_proposal(frames)
        bad_cleansing["cleansing_policy"].append({"table": "sales", "column": "department", "action": "execute_python"})
        with self.assertRaisesRegex(ProposalGenerationError, "cleansing_policy"):
            validate_proposal_references(bad_cleansing, frames)

    def test_deterministic_proposal_marks_low_confidence_merges_for_review(self) -> None:
        frames = {
            "left": pd.DataFrame({"abc": [1]}),
            "right": pd.DataFrame({"abd": [2]}),
            "strong": pd.DataFrame({"department": ["ops"]}),
            "strong_2": pd.DataFrame({"dept": ["ops"]}),
        }
        groups = build_union_groups(frames)
        weak = next(group for group in groups if {member["column"] for member in group["members"]} == {"abc", "abd"})
        strong = next(group for group in groups if {member["column"] for member in group["members"]} == {"department", "dept"})

        self.assertEqual(weak["decision"], "review")
        self.assertTrue(weak["review_required"])
        self.assertLess(weak["confidence"], 0.8)
        self.assertEqual(strong["decision"], "merge")
        self.assertFalse(strong["review_required"])

    def test_summarizes_proposal_revision_changes(self) -> None:
        frames = {"sales": pd.DataFrame({"department": ["ops"], "sales": [10]})}
        first = build_canonical_proposal(frames, feedback="first")
        second = build_canonical_proposal(frames, feedback="second")
        second["canonical_columns"][0]["decision"] = "review"
        second["canonical_columns"][0]["review_required"] = True

        initial = summarize_proposal_changes(None, first)
        changes = summarize_proposal_changes(first, second)

        self.assertEqual(initial, ["Initial proposal generated."])
        self.assertIn("User feedback changed.", changes)
        self.assertTrue(any("Changed decision" in change for change in changes))
        self.assertTrue(any("review_required" in change for change in changes))

    def test_llm_prompt_uses_metadata_only_privacy_contract(self) -> None:
        frames = {
            "customers": pd.DataFrame(
                {
                    "customer_name": ["SECRET_CUSTOMER"],
                    "sales": [100],
                }
            )
        }
        proposal = build_canonical_proposal(frames, feedback="use customer_name as the customer label")
        prompt = build_llm_prompt(proposal)
        serialized = str(prompt)
        self.assertIn("metadata_only", serialized)
        self.assertIn("raw rows", serialized)
        self.assertIn("sample values", serialized)
        self.assertIn("customer_name", serialized)
        self.assertNotIn("SECRET_CUSTOMER", serialized)

    def test_query_guard_blocks_mutations(self) -> None:
        with self.assertRaises(QueryGenerationError):
            validate_select_sql("drop table users")
        with self.assertRaises(QueryGenerationError):
            validate_select_sql("select * from x; delete from x")
        with self.assertRaises(QueryGenerationError):
            validate_select_sql("select * from x;")
        with self.assertRaises(QueryGenerationError):
            validate_select_sql("select * from x -- comment")

    def test_query_guard_allows_with_cte_and_preserves_existing_limit(self) -> None:
        self.assertEqual(validate_select_sql("WITH x AS (SELECT 1) SELECT * FROM x", default_limit=10), "WITH x AS (SELECT 1) SELECT * FROM x limit 10")
        self.assertEqual(validate_select_sql("select * from x limit 5", default_limit=10), "select * from x limit 5")
        with self.assertRaises(QueryGenerationError):
            validate_select_sql("select * from x;")
        with self.assertRaises(QueryGenerationError):
            validate_select_sql("select * from x -- trailing comment")
        with self.assertRaises(QueryGenerationError):
            validate_select_sql("SeLeCt * FrOm x AtTaCh")

    def test_fallback_sales_by_department_query(self) -> None:
        with sqlite3.connect(":memory:") as conn:
            conn.execute("create table dataset_1(department text, sales real)")
            conn.executemany("insert into dataset_1 values (?, ?)", [("営業", 100), ("営業", 50), ("開発", 25)])
            sql, _ = fallback_query("部署別の売上合計を見たい", conn, "dataset_1")
            rows = run_query(conn, sql)
            self.assertEqual(rows[0]["department"], "営業")
            self.assertEqual(rows[0]["total"], 150)

    def test_query_guard_allows_with_and_preserves_existing_limit(self) -> None:
        with_sql = validate_select_sql("with src as (select * from dataset_1) select * from src")
        self.assertEqual(with_sql, "with src as (select * from dataset_1) select * from src limit 1000")
        limited = validate_select_sql("select * from dataset_1 limit 25")
        self.assertEqual(limited, "select * from dataset_1 limit 25")

    def test_query_guard_conservative_keyword_literal_false_positive(self) -> None:
        with self.assertRaises(QueryGenerationError):
            validate_select_sql('select "drop"')

    def test_repository_migrates_legacy_query_history_schema(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "legacy.db"
            with sqlite3.connect(db_path) as conn:
                conn.execute(
                    """
                    create table query_history(
                        id text primary key,
                        dataset_id text,
                        target_mode text,
                        question text not null,
                        sql_text text not null,
                        explanation text not null,
                        result_json text not null,
                        created_at text not null
                    )
                    """
                )
                conn.execute(
                    """
                    insert into query_history(id, dataset_id, target_mode, question, sql_text, explanation, result_json, created_at)
                    values ('hist-1', 'ds-1', 'merged', 'q', 'select 1', 'old', '[{"a": 1}]', '2026-01-01T00:00:00+00:00')
                    """
                )
            repo = MetadataRepository(db_path)
            entries = repo.list_query_history()
            self.assertEqual(entries[0]["sql"], "select 1")
            self.assertEqual(entries[0]["row_count"], 1)
            self.assertEqual(entries[0]["result_preview"], [{"a": 1}])
            repo.add_query_history("ds-1", "new", "select 2", 0)

    def test_repository_backs_up_existing_database_before_migration_once(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "legacy.db"
            with sqlite3.connect(db_path) as conn:
                conn.execute("create table datasets(id text primary key, name text not null, status text not null, created_at text not null, updated_at text not null)")
                conn.execute("insert into datasets(id, name, status, created_at, updated_at) values ('ds-1', 'sales', 'uploaded', '2026-01-01T00:00:00+00:00', '2026-01-01T00:00:00+00:00')")
                conn.execute("pragma user_version = 0")

            MetadataRepository(db_path)
            backups = sorted(Path(tmp).glob("legacy.pre-migration-v0-*.db.bak"))
            self.assertEqual(len(backups), 1)
            with sqlite3.connect(db_path) as conn:
                self.assertEqual(conn.execute("pragma user_version").fetchone()[0], 2)
                columns = {row[1] for row in conn.execute("pragma table_info(datasets)").fetchall()}
            self.assertIn("proposal_json", columns)

            MetadataRepository(db_path)
            self.assertEqual(len(list(Path(tmp).glob("legacy.pre-migration-v0-*.db.bak"))), 1)

    def test_repository_migrates_v1_query_history_context_columns_to_v2(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "v1.db"
            with sqlite3.connect(db_path) as conn:
                conn.execute(
                    """
                    create table query_history(
                        id text primary key,
                        dataset_id text,
                        question text not null,
                        sql text not null,
                        row_count integer not null,
                        created_at text not null,
                        explanation text not null default '',
                        columns_json text not null default '[]',
                        result_preview_json text not null default '[]',
                        materialized_table text,
                        materialization_run_id text,
                        target_mode text not null default 'merged'
                    )
                    """
                )
                conn.execute("pragma user_version = 1")

            MetadataRepository(db_path)
            backups = sorted(Path(tmp).glob("v1.pre-migration-v1-*.db.bak"))
            self.assertEqual(len(backups), 1)
            with sqlite3.connect(db_path) as conn:
                self.assertEqual(conn.execute("pragma user_version").fetchone()[0], 2)
                columns = {row[1] for row in conn.execute("pragma table_info(query_history)").fetchall()}
            self.assertIn("proposal_id", columns)
            self.assertIn("proposal_version", columns)
            self.assertIn("materialization_proposal_id", columns)
            self.assertIn("materialization_proposal_version", columns)

    def test_repository_migrates_legacy_materialization_proposals_schema(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "legacy.db"
            with sqlite3.connect(db_path) as conn:
                conn.execute(
                    """
                    create table materialization_proposals(
                        id text primary key,
                        dataset_id text not null,
                        proposal_id text not null,
                        status text not null,
                        source_run_id text not null,
                        materialization_json text not null,
                        created_at text not null
                    )
                    """
                )
                conn.execute(
                    """
                    insert into materialization_proposals(
                        id, dataset_id, proposal_id, status, source_run_id, materialization_json, created_at
                    )
                    values ('mat-1', 'ds-1', 'prop-1', 'proposed', 'run-1', '{"canonical_columns": []}', '2026-01-01T00:00:00+00:00')
                    """
                )
            repo = MetadataRepository(db_path)
            latest = repo.latest_materialization_proposal("ds-1")
            self.assertEqual(latest["version"], 1)
            self.assertEqual(latest["plan"], {"canonical_columns": []})
            proposal_id = repo.save_materialization_proposal("ds-1", {"canonical_columns": [{"canonical_name": "x"}]}, "new")
            self.assertTrue(proposal_id)
            latest = repo.latest_materialization_proposal("ds-1")
            self.assertEqual(latest["version"], 2)
            self.assertEqual(latest["feedback"], "new")

    def test_repository_normalizes_and_enforces_version_uniqueness(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "versions.db"
            with sqlite3.connect(db_path) as conn:
                conn.execute(
                    """
                    create table datasets(
                        id text primary key,
                        name text not null,
                        status text not null,
                        created_at text not null,
                        updated_at text not null,
                        materialized_table text,
                        proposal_json text,
                        materialization_json text
                    )
                    """
                )
                conn.execute(
                    "insert into datasets(id, name, status, created_at, updated_at) values ('ds-1', 'sales', 'uploaded', '2026-01-01T00:00:00+00:00', '2026-01-01T00:00:00+00:00')"
                )
                conn.execute(
                    """
                    create table proposals(
                        id text primary key,
                        dataset_id text not null,
                        version integer not null,
                        feedback text not null default '',
                        proposal_json text not null,
                        created_at text not null
                    )
                    """
                )
                conn.execute(
                    "insert into proposals(id, dataset_id, version, feedback, proposal_json, created_at) values ('prop-1', 'ds-1', 1, '', '{}', '2026-01-01T00:00:00+00:00')"
                )
                conn.execute(
                    "insert into proposals(id, dataset_id, version, feedback, proposal_json, created_at) values ('prop-2', 'ds-1', 1, '', '{}', '2026-01-01T00:00:01+00:00')"
                )
                conn.execute(
                    """
                    create table materialization_proposals(
                        id text primary key,
                        dataset_id text not null,
                        version integer not null,
                        feedback text not null default '',
                        plan_json text not null,
                        created_at text not null
                    )
                    """
                )
                conn.execute(
                    "insert into materialization_proposals(id, dataset_id, version, feedback, plan_json, created_at) values ('mat-1', 'ds-1', 1, '', '{}', '2026-01-01T00:00:00+00:00')"
                )
                conn.execute(
                    "insert into materialization_proposals(id, dataset_id, version, feedback, plan_json, created_at) values ('mat-2', 'ds-1', 1, '', '{}', '2026-01-01T00:00:01+00:00')"
                )

            repo = MetadataRepository(db_path)
            self.assertEqual(repo.latest_proposal("ds-1")["version"], 2)
            self.assertEqual(repo.latest_materialization_proposal("ds-1")["version"], 2)

            with sqlite3.connect(db_path) as conn:
                with self.assertRaises(sqlite3.IntegrityError):
                    conn.execute(
                        "insert into proposals(id, dataset_id, version, feedback, proposal_json, created_at) values ('prop-dup', 'ds-1', 2, '', '{}', '2026-01-01T00:00:02+00:00')"
                    )
                with self.assertRaises(sqlite3.IntegrityError):
                    conn.execute(
                        "insert into materialization_proposals(id, dataset_id, version, feedback, plan_json, created_at) values ('mat-dup', 'ds-1', 2, '', '{}', '2026-01-01T00:00:02+00:00')"
                    )

            repo.save_proposal("ds-1", {"canonical_columns": []})
            repo.save_materialization_proposal("ds-1", {"canonical_columns": []})
            self.assertEqual(repo.latest_proposal("ds-1")["version"], 3)
            self.assertEqual(repo.latest_materialization_proposal("ds-1")["version"], 3)

    def test_repository_enforces_foreign_keys(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            repo = MetadataRepository(Path(tmp) / "app.db")
            with self.assertRaises(sqlite3.IntegrityError):
                repo.add_source_file(
                    dataset_id="missing-dataset",
                    filename="sales.csv",
                    sheet_name=None,
                    table_name="sales",
                    file_path=Path(tmp) / "sales.csv",
                    rows=1,
                    columns=["department"],
                    profile={},
                )

    def test_repository_writes_legacy_proposal_status_column(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "legacy-proposals.db"
            with sqlite3.connect(db_path) as conn:
                conn.execute(
                    """
                    create table datasets(
                        id text primary key,
                        name text not null,
                        status text not null,
                        approved_proposal_id text,
                        created_at text not null,
                        updated_at text not null
                    )
                    """
                )
                conn.execute(
                    "insert into datasets(id, name, status, created_at, updated_at) values ('ds-1', 'sales', 'uploaded', '2026-01-01T00:00:00+00:00', '2026-01-01T00:00:00+00:00')"
                )
                conn.execute(
                    """
                    create table proposals(
                        id text primary key,
                        dataset_id text not null,
                        version integer not null,
                        status text not null,
                        feedback text,
                        proposal_json text not null,
                        created_at text not null
                    )
                    """
                )
            repo = MetadataRepository(db_path)
            proposal_id = repo.save_proposal("ds-1", {"canonical_columns": []}, status="proposed")
            with sqlite3.connect(db_path) as conn:
                row = conn.execute("select status, version from proposals where id = ?", (proposal_id,)).fetchone()
                dataset_columns = {row[1] for row in conn.execute("pragma table_info(datasets)").fetchall()}
                dataset = conn.execute("select proposal_json, materialized_table, materialization_json from datasets where id = 'ds-1'").fetchone()
            self.assertEqual(row, ("proposed", 1))
            self.assertIn("proposal_json", dataset_columns)
            self.assertEqual(dataset[0], '{"canonical_columns": []}')

    def test_repository_migrates_legacy_approval_decisions_schema(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "legacy.db"
            with sqlite3.connect(db_path) as conn:
                conn.execute(
                    """
                    create table approval_decisions(
                        id text primary key,
                        dataset_id text not null,
                        proposal_id text not null,
                        canonical_name text not null,
                        decision_json text not null,
                        created_at text not null
                    )
                    """
                )
                conn.execute(
                    """
                    insert into approval_decisions(id, dataset_id, proposal_id, canonical_name, decision_json, created_at)
                    values ('dec-1', 'ds-1', 'prop-1', 'legacy_decision', '{"ok": true}', '2026-01-01T00:00:00+00:00')
                    """
                )
            repo = MetadataRepository(db_path)
            entries = repo.list_approval_decisions("ds-1")
            self.assertEqual(entries[0]["decision_type"], "legacy_decision")
            self.assertEqual(entries[0]["payload"], {"ok": True})
            repo.add_approval_decision("ds-1", "prop-2", "proposal_approved", "ok", {"proposal_version": 2})
            entries = repo.list_approval_decisions("ds-1")
            self.assertEqual(entries[0]["decision_type"], "proposal_approved")


if __name__ == "__main__":
    unittest.main()
