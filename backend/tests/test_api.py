from __future__ import annotations

import io
import os
import sqlite3
import stat
import tempfile
import unittest
from pathlib import Path

import pandas as pd
from fastapi.testclient import TestClient

from app.version import APP_VERSION, SCHEMA_VERSION


class ApiTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        root = Path(self.tmp.name)
        os.environ["DB_AUTO_PILOT_DATA_DIR"] = str(root)
        os.environ["DB_AUTO_PILOT_DATABASE_PATH"] = str(root / "app.db")
        os.environ["DB_AUTO_PILOT_UPLOADS_DIR"] = str(root / "uploads")
        os.environ["DB_AUTO_PILOT_APP_SETTINGS_PATH"] = str(root / "settings.json")
        os.environ["DB_AUTO_PILOT_LLM_ENABLED"] = "false"
        os.environ["DB_AUTO_PILOT_MAX_UPLOAD_MB"] = "1"
        os.environ["DB_AUTO_PILOT_MAX_MATERIALIZATION_ROWS"] = "1000"
        os.environ["DB_AUTO_PILOT_MAX_MATERIALIZATION_COLUMNS"] = "50"
        os.environ["DB_AUTO_PILOT_CORS_ALLOW_ORIGINS"] = "http://allowed.example"
        from app.config import get_settings

        get_settings.cache_clear()
        from app.main import create_app

        self.client = TestClient(create_app())

    def tearDown(self) -> None:
        self.tmp.cleanup()

    def test_settings_exposes_llm_data_policy(self) -> None:
        response = self.client.get("/settings")
        self.assertEqual(response.status_code, 200, response.text)
        self.assertEqual(response.json()["llm_data_policy"], "metadata_only")
        self.assertFalse(response.json()["llm_enabled"])
        self.assertEqual(response.json()["max_upload_mb"], 1)
        self.assertEqual(response.json()["max_materialization_rows"], 1000)
        self.assertEqual(response.json()["max_materialization_columns"], 50)
        self.assertEqual(response.json()["query_row_limit"], 500)
        self.assertEqual(response.json()["cors_allow_origins"], ["http://allowed.example"])

    def test_settings_update_persists_without_returning_secret(self) -> None:
        update = self.client.put(
            "/settings",
            json={"openai_api_key": "sk-test-secret", "openai_model": "gpt-test", "llm_enabled": True},
        )
        self.assertEqual(update.status_code, 200, update.text)
        body = update.json()
        self.assertEqual(body["openai_model"], "gpt-test")
        self.assertTrue(body["llm_enabled"])
        self.assertTrue(body["openai_api_key_configured"])
        self.assertNotIn("sk-test-secret", str(body))

        fetched = self.client.get("/settings").json()
        self.assertEqual(fetched["openai_model"], "gpt-test")
        self.assertTrue(fetched["openai_api_key_configured"])
        self.assertNotIn("sk-test-secret", str(fetched))
        mode = stat.S_IMODE((Path(self.tmp.name) / "settings.json").stat().st_mode)
        self.assertEqual(mode, 0o600)

        cleared = self.client.put("/settings", json={"clear_openai_api_key": True, "llm_enabled": True})
        self.assertEqual(cleared.status_code, 200, cleared.text)
        self.assertFalse(cleared.json()["openai_api_key_configured"])
        self.assertFalse(cleared.json()["llm_enabled"])

    def test_request_id_header_is_returned(self) -> None:
        generated = self.client.get("/health")
        self.assertEqual(generated.status_code, 200)
        self.assertTrue(generated.headers.get("x-request-id"))

        supplied = self.client.get("/health", headers={"x-request-id": "trace-123"})
        self.assertEqual(supplied.headers["x-request-id"], "trace-123")

    def test_cors_uses_configured_origin_without_credentials(self) -> None:
        response = self.client.options(
            "/health",
            headers={
                "origin": "http://allowed.example",
                "access-control-request-method": "GET",
            },
        )
        self.assertEqual(response.status_code, 200, response.text)
        self.assertEqual(response.headers["access-control-allow-origin"], "http://allowed.example")
        self.assertNotIn("access-control-allow-credentials", response.headers)

    def test_diagnostics_exposes_non_secret_runtime_state(self) -> None:
        response = self.client.get("/diagnostics")
        self.assertEqual(response.status_code, 200, response.text)
        body = response.json()
        self.assertEqual(body["status"], "ok")
        self.assertEqual(body["schema_version"], 2)
        self.assertEqual(body["database_user_version"], 2)
        self.assertTrue(body["database_ready"])
        self.assertTrue(body["uploads_dir_ready"])
        self.assertEqual(body["database_integrity"], "ok")
        self.assertEqual(body["foreign_key_violations"], 0)
        self.assertEqual(body["migration_backup_count"], 0)
        self.assertIsNone(body["latest_migration_backup"])
        self.assertEqual(body["counts"]["datasets"], 0)
        self.assertEqual(body["settings"]["max_upload_mb"], 1)
        serialized = str(body)
        self.assertNotIn(str(Path(self.tmp.name)), serialized)

    def test_diagnostics_degrades_on_foreign_key_violations(self) -> None:
        with sqlite3.connect(Path(self.tmp.name) / "app.db") as conn:
            conn.execute("pragma foreign_keys = off")
            conn.execute(
                """
                insert into source_files(id, dataset_id, filename, sheet_name, table_name, file_path, rows, columns_json, profile_json)
                values ('src-orphan', 'missing-dataset', 'bad.csv', null, 'bad', 'bad.csv', 1, '[]', '{}')
                """
            )

        response = self.client.get("/diagnostics")
        self.assertEqual(response.status_code, 200, response.text)
        body = response.json()
        self.assertEqual(body["status"], "degraded")
        self.assertGreater(body["foreign_key_violations"], 0)

    def test_diagnostics_exposes_migration_backup_summary(self) -> None:
        backup = Path(self.tmp.name) / "app.pre-migration-v1-20260101000000-abcdef12.db.bak"
        backup.write_text("backup", encoding="utf-8")
        response = self.client.get("/diagnostics")
        self.assertEqual(response.status_code, 200, response.text)
        body = response.json()
        self.assertEqual(body["migration_backup_count"], 1)
        self.assertEqual(body["latest_migration_backup"], backup.name)

    def test_upload_propose_approve_query_history_and_analytics(self) -> None:
        csv = "部署,売上\n営業,100\n営業,50\n開発,25\n"
        upload = self.client.post(
            "/datasets/upload",
            files=[("files", ("sales.csv", csv.encode("utf-8"), "text/csv"))],
            data={"name": "sales"},
        )
        self.assertEqual(upload.status_code, 200, upload.text)
        dataset_id = upload.json()["id"]

        proposal = self.client.post(f"/datasets/{dataset_id}/proposal", json={"feedback": "部署と売上を使う"})
        self.assertEqual(proposal.status_code, 200, proposal.text)
        proposal_body = proposal.json()
        self.assertIn("canonical_columns", proposal_body["proposal"])
        self.assertTrue(proposal_body["proposal_id"])
        self.assertEqual(proposal_body["version"], 1)

        approval = self.client.post(
            f"/datasets/{dataset_id}/approve",
            json={"proposal_id": proposal_body["proposal_id"], "proposal_version": proposal_body["version"], "notes": "承認"},
        )
        self.assertEqual(approval.status_code, 200, approval.text)
        self.assertEqual(approval.json()["rows"], 3)

        query = self.client.post("/query", json={"dataset_id": dataset_id, "question": "部署別の売上合計を見たい"})
        self.assertEqual(query.status_code, 200, query.text)
        self.assertIn("select", query.json()["sql"].lower())
        self.assertGreaterEqual(len(query.json()["rows"]), 1)

        history = self.client.get("/query/history")
        self.assertEqual(history.status_code, 200)
        self.assertEqual(len(history.json()), 1)
        history_entry = history.json()[0]
        self.assertEqual(history_entry["explanation"], query.json()["explanation"])
        self.assertEqual(history_entry["columns"], query.json()["columns"])
        self.assertEqual(history_entry["result_preview"], query.json()["rows"][:20])
        self.assertTrue(history_entry["materialized_table"].startswith("dataset_"))
        self.assertTrue(history_entry["materialization_run_id"])
        self.assertEqual(history_entry["target_mode"], "merged")
        self.assertEqual(history_entry["proposal_id"], proposal_body["proposal_id"])
        self.assertEqual(history_entry["proposal_version"], proposal_body["version"])

        detail = self.client.get(f"/datasets/{dataset_id}").json()
        self.assertIn("quality_warnings", detail["materialization"])
        self.assertIn("lineage", detail["materialization"])
        self.assertEqual(detail["materialization_runs"][0]["status"], "materialized")

        analytics = self.client.get(f"/datasets/{dataset_id}/analytics")
        self.assertEqual(analytics.status_code, 200, analytics.text)
        self.assertEqual(analytics.json()["materialized_table"], approval.json()["materialized_table"])
        self.assertEqual(analytics.json()["materialization_run_id"], detail["materialization_runs"][0]["id"])
        analytics_table = analytics.json()["tables"][0]
        self.assertEqual(analytics_table["row_count"], 3)
        self.assertEqual(analytics_table["recommended_charts"][0]["type"], "bar")
        self.assertEqual(analytics_table["recommended_charts"][0]["dimension"], "部署")

        export = self.client.get(f"/datasets/{dataset_id}/export")
        self.assertEqual(export.status_code, 200, export.text)
        exported = export.json()
        self.assertEqual(exported["runtime_provenance"]["app_version"], APP_VERSION)
        self.assertEqual(exported["runtime_provenance"]["schema_version"], SCHEMA_VERSION)
        self.assertEqual(exported["runtime_provenance"]["database_user_version"], SCHEMA_VERSION)
        self.assertEqual(exported["runtime_provenance"]["database_integrity"], "ok")
        self.assertEqual(exported["runtime_provenance"]["foreign_key_violations"], 0)
        self.assertEqual(exported["runtime_provenance"]["migration_backup_count"], 0)
        self.assertEqual(exported["runtime_provenance"]["settings"]["llm_data_policy"], "metadata_only")
        self.assertNotIn(str(Path(self.tmp.name)), str(exported["runtime_provenance"]))
        self.assertEqual(exported["dataset"]["id"], dataset_id)
        self.assertEqual(exported["approvals"][0]["proposal_id"], proposal_body["proposal_id"])
        self.assertEqual(exported["query_history"][0]["id"], history_entry["id"])
        self.assertFalse(exported["query_history_previews_included"])
        self.assertEqual(exported["query_history"][0]["result_preview"], [])

        preview_export = self.client.get(f"/datasets/{dataset_id}/export?include_query_previews=true")
        self.assertEqual(preview_export.status_code, 200, preview_export.text)
        preview_exported = preview_export.json()
        self.assertTrue(preview_exported["query_history_previews_included"])
        self.assertGreater(len(preview_exported["query_history"][0]["result_preview"]), 0)
        self.assertLessEqual(len(preview_exported["query_history"][0]["result_preview"]), 5)
        self.assertEqual(exported["analytics"]["materialized_table"], approval.json()["materialized_table"])

    def test_dataset_query_history_is_scoped_to_requested_dataset(self) -> None:
        dataset_ids = []
        for name, row in [("sales-a", "ops,10"), ("sales-b", "dev,20")]:
            dataset = self.client.post(
                "/datasets/upload",
                files=[("files", ("sales.csv", f"department,sales\n{row}\n".encode("utf-8"), "text/csv"))],
                data={"name": name},
            ).json()
            proposal = self.client.post(f"/datasets/{dataset['id']}/proposal", json={"feedback": ""}).json()
            self.client.post(
                f"/datasets/{dataset['id']}/approve",
                json={"proposal_id": proposal["proposal_id"], "proposal_version": proposal["version"], "notes": "ok"},
            )
            query = self.client.post("/query", json={"dataset_id": dataset["id"], "question": f"preview {name}"})
            self.assertEqual(query.status_code, 200, query.text)
            dataset_ids.append(dataset["id"])

        first_history = self.client.get(f"/datasets/{dataset_ids[0]}/query-history")
        self.assertEqual(first_history.status_code, 200, first_history.text)
        self.assertEqual({entry["dataset_id"] for entry in first_history.json()}, {dataset_ids[0]})
        second_history = self.client.get(f"/datasets/{dataset_ids[1]}/query-history")
        self.assertEqual({entry["dataset_id"] for entry in second_history.json()}, {dataset_ids[1]})

    def test_history_list_limits_are_bounded(self) -> None:
        dataset = self.client.post(
            "/datasets/upload",
            files=[("files", ("sales.csv", b"department,sales\nops,10\n", "text/csv"))],
            data={"name": "sales"},
        ).json()
        for path in [
            "/query/history?limit=201",
            f"/datasets/{dataset['id']}/query-history?limit=201",
            f"/datasets/{dataset['id']}/proposals?limit=101",
            f"/datasets/{dataset['id']}/materialization-proposals?limit=101",
        ]:
            with self.subTest(path=path):
                response = self.client.get(path)
                self.assertEqual(response.status_code, 422, response.text)

    def test_export_redacts_source_profile_cell_values(self) -> None:
        dataset = self.client.post(
            "/datasets/upload",
            files=[("files", ("customers.csv", b"customer_name,sales\nSECRET_CUSTOMER,10\n", "text/csv"))],
            data={"name": "customers"},
        ).json()

        detail = self.client.get(f"/datasets/{dataset['id']}").json()
        self.assertIn("SECRET_CUSTOMER", str(detail["sources"][0]["profile"]))

        export = self.client.get(f"/datasets/{dataset['id']}/export")
        self.assertEqual(export.status_code, 200, export.text)
        exported_profile = export.json()["dataset"]["sources"][0]["profile"]
        self.assertNotIn("SECRET_CUSTOMER", str(exported_profile))
        self.assertTrue(exported_profile["columns"]["customer_name"]["cell_values_redacted"])

    def test_approve_rejects_invented_proposal_references(self) -> None:
        dataset = self.client.post(
            "/datasets/upload",
            files=[("files", ("sales.csv", b"department,sales\nops,10\n", "text/csv"))],
            data={"name": "sales"},
        ).json()
        proposal_response = self.client.post(f"/datasets/{dataset['id']}/proposal", json={"feedback": ""}).json()
        proposal = proposal_response["proposal"]
        proposal["canonical_columns"][0]["members"][0]["column"] = "invented"
        response = self.client.post(
            f"/datasets/{dataset['id']}/approve",
            json={
                "proposal_id": proposal_response["proposal_id"],
                "proposal_version": proposal_response["version"],
                "proposal": proposal,
                "notes": "bad",
            },
        )
        self.assertEqual(response.status_code, 400)
        self.assertIn("must match", response.text)

    def test_approve_rejects_missing_or_stale_proposal_version(self) -> None:
        dataset = self.client.post(
            "/datasets/upload",
            files=[("files", ("sales.csv", b"department,sales\nops,10\n", "text/csv"))],
            data={"name": "sales"},
        ).json()
        first = self.client.post(f"/datasets/{dataset['id']}/proposal", json={"feedback": "first"}).json()
        second = self.client.post(f"/datasets/{dataset['id']}/proposal", json={"feedback": "second"}).json()

        missing = self.client.post(f"/datasets/{dataset['id']}/approve", json={"notes": "missing ids"})
        self.assertEqual(missing.status_code, 400)
        self.assertIn("proposal_id", missing.text)

        stale = self.client.post(
            f"/datasets/{dataset['id']}/approve",
            json={"proposal_id": first["proposal_id"], "proposal_version": first["version"], "notes": "stale"},
        )
        self.assertEqual(stale.status_code, 400)
        self.assertIn("stale", stale.text)

        current = self.client.post(
            f"/datasets/{dataset['id']}/approve",
            json={"proposal_id": second["proposal_id"], "proposal_version": second["version"], "notes": "current"},
        )
        self.assertEqual(current.status_code, 200, current.text)

    def test_proposal_history_exposes_revision_change_summary(self) -> None:
        dataset = self.client.post(
            "/datasets/upload",
            files=[("files", ("sales.csv", b"department,sales\nops,10\n", "text/csv"))],
            data={"name": "sales"},
        ).json()
        first = self.client.post(f"/datasets/{dataset['id']}/proposal", json={"feedback": "first"}).json()
        second = self.client.post(f"/datasets/{dataset['id']}/proposal", json={"feedback": "second"}).json()

        self.assertEqual(first["proposal"]["change_summary"], ["Initial proposal generated."])
        self.assertIn("User feedback changed.", second["proposal"]["change_summary"])

        history = self.client.get(f"/datasets/{dataset['id']}/proposals")
        self.assertEqual(history.status_code, 200, history.text)
        entries = history.json()
        self.assertEqual([entry["version"] for entry in entries], [2, 1])
        self.assertEqual(entries[0]["id"], second["proposal_id"])
        self.assertIn("User feedback changed.", entries[0]["proposal"]["change_summary"])
        self.assertEqual(entries[1]["id"], first["proposal_id"])

        export = self.client.get(f"/datasets/{dataset['id']}/export")
        self.assertEqual(export.status_code, 200, export.text)
        self.assertEqual([entry["version"] for entry in export.json()["proposal_history"]], [2, 1])

    def test_review_required_proposal_approval_requires_notes(self) -> None:
        dataset = self.client.post(
            "/datasets/upload",
            files=[
                ("files", ("left.csv", b"abc,value\nx,1\n", "text/csv")),
                ("files", ("right.csv", b"abd,value\ny,2\n", "text/csv")),
            ],
            data={"name": "weak-merge"},
        ).json()
        proposal = self.client.post(f"/datasets/{dataset['id']}/proposal", json={"feedback": ""}).json()
        self.assertTrue(any(group["review_required"] for group in proposal["proposal"]["canonical_columns"]))

        missing_notes = self.client.post(
            f"/datasets/{dataset['id']}/proposal/approve",
            json={"proposal_id": proposal["proposal_id"], "proposal_version": proposal["version"]},
        )
        self.assertEqual(missing_notes.status_code, 400)
        self.assertIn("review_required", missing_notes.text)

        acknowledged = self.client.post(
            f"/datasets/{dataset['id']}/proposal/approve",
            json={
                "proposal_id": proposal["proposal_id"],
                "proposal_version": proposal["version"],
                "notes": "Reviewed weak abc/abd similarity and accept this mapping.",
            },
        )
        self.assertEqual(acknowledged.status_code, 200, acknowledged.text)

    def test_materialization_approve_rejects_invented_plan_references(self) -> None:
        dataset = self.client.post(
            "/datasets/upload",
            files=[("files", ("sales.csv", b"department,sales\nops,10\n", "text/csv"))],
            data={"name": "sales"},
        ).json()
        proposal = self.client.post(f"/datasets/{dataset['id']}/proposal", json={"feedback": ""}).json()
        self.client.post(
            f"/datasets/{dataset['id']}/approve",
            json={"proposal_id": proposal["proposal_id"], "proposal_version": proposal["version"], "notes": "ok"},
        )
        plan_response = self.client.post(f"/datasets/{dataset['id']}/materialization-proposal", json={"feedback": ""}).json()
        plan = plan_response["plan"]
        plan["canonical_columns"][0]["members"][0]["table"] = "invented"
        response = self.client.post(
            f"/datasets/{dataset['id']}/materialization-approve",
            json={
                "materialization_proposal_id": plan_response["materialization_proposal_id"],
                "materialization_proposal_version": plan_response["version"],
                "plan": plan,
                "notes": "bad",
            },
        )
        self.assertEqual(response.status_code, 400)
        self.assertIn("must match", response.text)

    def test_materialization_rejects_row_limit_before_publishing_dataset(self) -> None:
        rows = "department,sales\n" + "".join(f"ops,{i}\n" for i in range(1001))
        dataset = self.client.post(
            "/datasets/upload",
            files=[("files", ("large_materialization.csv", rows.encode("utf-8"), "text/csv"))],
            data={"name": "too-large-materialization"},
        ).json()
        proposal = self.client.post(f"/datasets/{dataset['id']}/proposal", json={"feedback": ""}).json()
        response = self.client.post(
            f"/datasets/{dataset['id']}/approve",
            json={"proposal_id": proposal["proposal_id"], "proposal_version": proposal["version"], "notes": "too big"},
        )
        self.assertEqual(response.status_code, 400)
        self.assertIn("row limit", response.text)
        detail = self.client.get(f"/datasets/{dataset['id']}").json()
        self.assertIsNone(detail["materialized_table"])
        self.assertEqual(detail["materialization_runs"][0]["status"], "failed")
        self.assertIn("row limit", detail["materialization_runs"][0]["error"])
        self.assertIn("retry_guidance", detail["materialization_runs"][0]["plan"])

    def test_materialization_retry_proposal_uses_failed_run_evidence(self) -> None:
        rows = "department,sales\n" + "".join(f"ops,{i}\n" for i in range(1001))
        dataset = self.client.post(
            "/datasets/upload",
            files=[("files", ("large_materialization.csv", rows.encode("utf-8"), "text/csv"))],
            data={"name": "retry-materialization"},
        ).json()
        proposal = self.client.post(f"/datasets/{dataset['id']}/proposal", json={"feedback": ""}).json()
        integration = self.client.post(
            f"/datasets/{dataset['id']}/proposal/approve",
            json={"proposal_id": proposal["proposal_id"], "proposal_version": proposal["version"], "notes": "approve integration"},
        )
        self.assertEqual(integration.status_code, 200, integration.text)
        plan = self.client.post(f"/datasets/{dataset['id']}/materialization-proposal", json={"feedback": "first plan"}).json()
        failed = self.client.post(
            f"/datasets/{dataset['id']}/materialization-proposal/{plan['materialization_proposal_id']}/approve",
            json={"materialization_proposal_version": plan["version"], "notes": "too large"},
        )
        self.assertEqual(failed.status_code, 400)

        retry = self.client.post(
            f"/datasets/{dataset['id']}/materialization-proposal/{plan['materialization_proposal_id']}/retry",
            json={"feedback": "raise the row limit or filter rows"},
        )
        self.assertEqual(retry.status_code, 200, retry.text)
        body = retry.json()
        self.assertEqual(body["version"], plan["version"] + 1)
        retry_meta = body["plan"]["retry"]
        self.assertEqual(retry_meta["source_materialization_proposal_id"], plan["materialization_proposal_id"])
        self.assertIn("row limit", retry_meta["failed_error"])
        self.assertIn("retry_guidance", retry_meta)

    def test_materialization_proposal_requires_approved_integration_proposal(self) -> None:
        dataset = self.client.post(
            "/datasets/upload",
            files=[("files", ("sales.csv", b"department,sales\nops,10\n", "text/csv"))],
            data={"name": "sales"},
        ).json()
        self.client.post(f"/datasets/{dataset['id']}/proposal", json={"feedback": ""})
        response = self.client.post(f"/datasets/{dataset['id']}/materialization-proposal", json={"feedback": ""})
        self.assertEqual(response.status_code, 400)
        self.assertIn("Approve", response.text)

    def test_materialization_approve_rejects_missing_or_stale_plan_version(self) -> None:
        dataset = self.client.post(
            "/datasets/upload",
            files=[("files", ("sales.csv", b"department,sales\nops,10\n", "text/csv"))],
            data={"name": "sales"},
        ).json()
        proposal = self.client.post(f"/datasets/{dataset['id']}/proposal", json={"feedback": ""}).json()
        self.client.post(
            f"/datasets/{dataset['id']}/approve",
            json={"proposal_id": proposal["proposal_id"], "proposal_version": proposal["version"], "notes": "ok"},
        )
        first = self.client.post(f"/datasets/{dataset['id']}/materialization-proposal", json={"feedback": "first"}).json()
        second = self.client.post(f"/datasets/{dataset['id']}/materialization-proposal", json={"feedback": "second"}).json()

        missing = self.client.post(f"/datasets/{dataset['id']}/materialization-approve", json={"notes": "missing"})
        self.assertEqual(missing.status_code, 400)
        self.assertIn("materialization_proposal_id", missing.text)

        stale = self.client.post(
            f"/datasets/{dataset['id']}/materialization-approve",
            json={
                "materialization_proposal_id": first["materialization_proposal_id"],
                "materialization_proposal_version": first["version"],
                "notes": "stale",
            },
        )
        self.assertEqual(stale.status_code, 400)
        self.assertIn("stale", stale.text)

        current = self.client.post(
            f"/datasets/{dataset['id']}/materialization-proposal/{second['materialization_proposal_id']}/approve",
            json={"materialization_proposal_version": second["version"], "notes": "current"},
        )
        self.assertEqual(current.status_code, 200, current.text)

    def test_materialization_proposal_history_exposes_revision_change_summary(self) -> None:
        dataset = self.client.post(
            "/datasets/upload",
            files=[("files", ("sales.csv", b"department,sales\nops,10\n", "text/csv"))],
            data={"name": "sales"},
        ).json()
        proposal = self.client.post(f"/datasets/{dataset['id']}/proposal", json={"feedback": ""}).json()
        self.client.post(
            f"/datasets/{dataset['id']}/proposal/approve",
            json={"proposal_id": proposal["proposal_id"], "proposal_version": proposal["version"], "notes": "ok"},
        )
        first = self.client.post(f"/datasets/{dataset['id']}/materialization-proposal", json={"feedback": "first"}).json()
        second = self.client.post(f"/datasets/{dataset['id']}/materialization-proposal", json={"feedback": "second"}).json()

        self.assertEqual(first["plan"]["change_summary"], ["Initial materialization proposal generated."])
        self.assertIn("Materialization feedback changed.", second["plan"]["change_summary"])

        history = self.client.get(f"/datasets/{dataset['id']}/materialization-proposals")
        self.assertEqual(history.status_code, 200, history.text)
        entries = history.json()
        self.assertEqual([entry["version"] for entry in entries], [2, 1])
        self.assertEqual(entries[0]["id"], second["materialization_proposal_id"])
        self.assertIn("Materialization feedback changed.", entries[0]["plan"]["change_summary"])

        export = self.client.get(f"/datasets/{dataset['id']}/export")
        self.assertEqual(export.status_code, 200, export.text)
        self.assertEqual([entry["version"] for entry in export.json()["materialization_proposal_history"]], [2, 1])

    def test_two_step_integration_and_materialization_approval_flow(self) -> None:
        dataset = self.client.post(
            "/datasets/upload",
            files=[("files", ("sales.csv", b"department,sales\nops,10\n", "text/csv"))],
            data={"name": "sales"},
        ).json()
        proposal = self.client.post(f"/datasets/{dataset['id']}/proposal", json={"feedback": ""}).json()
        integration = self.client.post(
            f"/datasets/{dataset['id']}/proposal/approve",
            json={"proposal_id": proposal["proposal_id"], "proposal_version": proposal["version"], "notes": "approve integration"},
        )
        self.assertEqual(integration.status_code, 200, integration.text)
        self.assertEqual(integration.json()["status"], "approved")
        self.assertIsNone(integration.json()["materialized_table"])

        materialization = self.client.post(f"/datasets/{dataset['id']}/materialization-proposal", json={"feedback": ""})
        self.assertEqual(materialization.status_code, 200, materialization.text)
        plan = materialization.json()
        approval = self.client.post(
            f"/datasets/{dataset['id']}/materialization-proposal/{plan['materialization_proposal_id']}/approve",
            json={"materialization_proposal_version": plan["version"], "notes": "approve materialization"},
        )
        self.assertEqual(approval.status_code, 200, approval.text)
        self.assertEqual(approval.json()["rows"], 1)
        query = self.client.post("/query", json={"dataset_id": dataset["id"], "question": "count rows"})
        self.assertEqual(query.status_code, 200, query.text)
        history = self.client.get(f"/datasets/{dataset['id']}/query-history").json()[0]
        self.assertEqual(history["proposal_id"], proposal["proposal_id"])
        self.assertEqual(history["proposal_version"], proposal["version"])
        self.assertEqual(history["materialization_proposal_id"], plan["materialization_proposal_id"])
        self.assertEqual(history["materialization_proposal_version"], plan["version"])
        timeline = self.client.get(f"/datasets/{dataset['id']}/approvals")
        self.assertEqual(timeline.status_code, 200, timeline.text)
        self.assertEqual([entry["decision_type"] for entry in timeline.json()], ["materialization_approved", "proposal_approved"])
        self.assertEqual(timeline.json()[0]["payload"]["materialization_proposal_version"], plan["version"])

    def test_blocks_mutating_sql(self) -> None:
        dataset = self.client.post(
            "/datasets/upload",
            files=[("files", ("sales.csv", b"department,sales\nops,10\n", "text/csv"))],
            data={"name": "sales"},
        ).json()
        proposal = self.client.post(f"/datasets/{dataset['id']}/proposal", json={"feedback": ""}).json()
        self.client.post(
            f"/datasets/{dataset['id']}/approve",
            json={"proposal_id": proposal["proposal_id"], "proposal_version": proposal["version"], "notes": "ok"},
        )
        response = self.client.post("/query", json={"dataset_id": dataset["id"], "question": "bad", "sql": "delete from datasets"})
        self.assertEqual(response.status_code, 400)

    def test_query_requires_materialized_requested_dataset(self) -> None:
        approved_csv = "部署,売上\n営業,100\n"
        approved = self.client.post(
            "/datasets/upload",
            files=[("files", ("approved.csv", approved_csv.encode("utf-8"), "text/csv"))],
            data={"name": "approved"},
        ).json()
        proposal = self.client.post(f"/datasets/{approved['id']}/proposal", json={"feedback": ""}).json()
        self.client.post(
            f"/datasets/{approved['id']}/approve",
            json={"proposal_id": proposal["proposal_id"], "proposal_version": proposal["version"], "notes": "ok"},
        )

        draft = self.client.post(
            "/datasets/upload",
            files=[("files", ("draft.csv", "部署,売上\n開発,999\n".encode("utf-8"), "text/csv"))],
            data={"name": "draft"},
        ).json()

        response = self.client.post("/query", json={"dataset_id": draft["id"], "question": "部署別の売上合計を見たい"})
        self.assertEqual(response.status_code, 400)
        self.assertIn("materialized", response.text)

        missing = self.client.post("/query", json={"dataset_id": "missing", "question": "部署別の売上合計を見たい"})
        self.assertEqual(missing.status_code, 404)

    def test_user_sql_is_scoped_to_requested_dataset_table(self) -> None:
        csv = "department,sales\nops,10\n"
        dataset = self.client.post(
            "/datasets/upload",
            files=[("files", ("sales.csv", csv.encode("utf-8"), "text/csv"))],
            data={"name": "sales"},
        ).json()
        proposal = self.client.post(f"/datasets/{dataset['id']}/proposal", json={"feedback": ""}).json()
        materialized = self.client.post(
            f"/datasets/{dataset['id']}/approve",
            json={"proposal_id": proposal["proposal_id"], "proposal_version": proposal["version"], "notes": "ok"},
        ).json()

        metadata = self.client.post("/query", json={"dataset_id": dataset["id"], "question": "meta", "sql": "select * from datasets"})
        self.assertEqual(metadata.status_code, 400)

        scoped = self.client.post(
            "/query",
            json={
                "dataset_id": dataset["id"],
                "question": "preview",
                "sql": f"select * from {materialized['materialized_table']}",
            },
        )
        self.assertEqual(scoped.status_code, 200, scoped.text)

        empty = self.client.post(
            "/query",
            json={
                "dataset_id": dataset["id"],
                "question": "empty",
                "sql": f"select department, sales from {materialized['materialized_table']} where sales > 9999",
            },
        )
        self.assertEqual(empty.status_code, 200, empty.text)
        self.assertEqual(empty.json()["rows"], [])
        self.assertEqual(empty.json()["columns"], ["department", "sales"])

    def test_query_rejects_limits_above_configured_cap(self) -> None:
        dataset = self.client.post(
            "/datasets/upload",
            files=[("files", ("sales.csv", b"department,sales\nops,10\n", "text/csv"))],
            data={"name": "sales"},
        ).json()
        proposal = self.client.post(f"/datasets/{dataset['id']}/proposal", json={"feedback": ""}).json()
        self.client.post(
            f"/datasets/{dataset['id']}/approve",
            json={"proposal_id": proposal["proposal_id"], "proposal_version": proposal["version"], "notes": "ok"},
        )

        response = self.client.post("/query", json={"dataset_id": dataset["id"], "question": "preview", "limit": 501})
        self.assertEqual(response.status_code, 400)
        self.assertIn("configured maximum", response.text)

    def test_user_sql_existing_limit_cannot_bypass_request_row_cap(self) -> None:
        csv = "department,sales\n" + "".join(f"ops,{index}\n" for index in range(201))
        dataset = self.client.post(
            "/datasets/upload",
            files=[("files", ("sales.csv", csv.encode("utf-8"), "text/csv"))],
            data={"name": "sales"},
        ).json()
        proposal = self.client.post(f"/datasets/{dataset['id']}/proposal", json={"feedback": ""}).json()
        materialized = self.client.post(
            f"/datasets/{dataset['id']}/approve",
            json={"proposal_id": proposal["proposal_id"], "proposal_version": proposal["version"], "notes": "ok"},
        ).json()

        response = self.client.post(
            "/query",
            json={
                "dataset_id": dataset["id"],
                "question": "too many",
                "sql": f"select * from {materialized['materialized_table']} limit 201",
            },
        )
        self.assertEqual(response.status_code, 400)
        self.assertIn("more than 200 rows", response.text)

    def test_invalid_upload_does_not_create_orphan_dataset(self) -> None:
        response = self.client.post(
            "/datasets/upload",
            files=[("files", ("bad.txt", b"not tabular", "text/plain"))],
            data={"name": "bad"},
        )
        self.assertEqual(response.status_code, 400)
        self.assertEqual(self.client.get("/datasets").json(), [])

    def test_empty_upload_does_not_create_orphan_dataset(self) -> None:
        for filename, content in [("empty.csv", b""), ("header_only.csv", b"department,sales\n")]:
            with self.subTest(filename=filename):
                response = self.client.post(
                    "/datasets/upload",
                    files=[("files", (filename, content, "text/csv"))],
                    data={"name": filename},
                )
                self.assertEqual(response.status_code, 400)
                self.assertEqual(self.client.get("/datasets").json(), [])

    def test_duplicate_upload_filenames_keep_distinct_sources(self) -> None:
        response = self.client.post(
            "/datasets/upload",
            files=[
                ("files", ("sales.csv", b"department,sales\nops,10\n", "text/csv")),
                ("files", ("sales.csv", b"department,sales\ndev,20\n", "text/csv")),
            ],
            data={"name": "duplicate-names"},
        )
        self.assertEqual(response.status_code, 200, response.text)
        detail = response.json()
        self.assertEqual(detail["source_count"], 2)
        table_names = {source["table_name"] for source in detail["sources"]}
        self.assertEqual(len(table_names), 2)

        proposal = self.client.post(f"/datasets/{detail['id']}/proposal", json={"feedback": ""})
        self.assertEqual(proposal.status_code, 200, proposal.text)
        self.assertEqual(len(proposal.json()["proposal"]["source_tables"]), 2)

    def test_excel_multisheet_upload_preserves_sheet_sources(self) -> None:
        workbook = io.BytesIO()
        with pd.ExcelWriter(workbook, engine="openpyxl") as writer:
            pd.DataFrame({"department": ["ops"], "sales": [10]}).to_excel(writer, sheet_name="sales", index=False)
            pd.DataFrame({"department": ["dev"], "cost": [5]}).to_excel(writer, sheet_name="costs", index=False)
        response = self.client.post(
            "/datasets/upload",
            files=[("files", ("book.xlsx", workbook.getvalue(), "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"))],
            data={"name": "excel-book"},
        )
        self.assertEqual(response.status_code, 200, response.text)
        detail = response.json()
        self.assertEqual(detail["source_count"], 2)
        self.assertEqual({source["sheet_name"] for source in detail["sources"]}, {"sales", "costs"})
        self.assertEqual({source["rows"] for source in detail["sources"]}, {1})

        proposal = self.client.post(f"/datasets/{detail['id']}/proposal", json={"feedback": ""})
        self.assertEqual(proposal.status_code, 200, proposal.text)
        self.assertEqual(len(proposal.json()["proposal"]["source_tables"]), 2)

    def test_oversized_upload_does_not_create_orphan_dataset(self) -> None:
        response = self.client.post(
            "/datasets/upload",
            files=[("files", ("large.csv", b"a\n" + b"x\n" * (1024 * 1024), "text/csv"))],
            data={"name": "large"},
        )
        self.assertEqual(response.status_code, 400)
        self.assertIn("exceeds 1 MB limit", response.text)
        self.assertEqual(self.client.get("/datasets").json(), [])

    def test_mixed_valid_invalid_upload_rolls_back_dataset_creation(self) -> None:
        response = self.client.post(
            "/datasets/upload",
            files=[
                ("files", ("good.csv", b"department,sales\nops,10\n", "text/csv")),
                ("files", ("bad.txt", b"not tabular", "text/plain")),
            ],
            data={"name": "mixed"},
        )
        self.assertEqual(response.status_code, 400)
        self.assertEqual(self.client.get("/datasets").json(), [])


if __name__ == "__main__":
    unittest.main()
