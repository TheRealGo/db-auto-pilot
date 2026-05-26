from __future__ import annotations

import subprocess
import shutil
import sqlite3
import sys
import tempfile
import unittest
import zipfile
import json
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "backend"))

from app.version import APP_VERSION, SCHEMA_VERSION


class BackupRestoreTests(unittest.TestCase):
    def test_backup_and_restore_database_with_uploads(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            database = root / "data" / "app.db"
            uploads = root / "data" / "uploads"
            output = root / "backups"
            database.parent.mkdir(parents=True)
            uploads.mkdir(parents=True)
            with sqlite3.connect(database) as conn:
                conn.execute("create table sample(value text)")
                conn.execute("insert into sample values ('sqlite-content')")
            (uploads / "dataset-1").mkdir()
            (uploads / "dataset-1" / "sales.csv").write_text("department,sales\nops,10\n", encoding="utf-8")

            backup = subprocess.run(
                [
                    sys.executable,
                    str(ROOT / "scripts" / "backup_data.py"),
                    "--database-path",
                    str(database),
                    "--uploads-dir",
                    str(uploads),
                    "--output-dir",
                    str(output),
                    "--label",
                    "test",
                ],
                check=True,
                capture_output=True,
                text=True,
            )
            archive = Path(backup.stdout.strip())
            self.assertTrue(archive.exists())
            with zipfile.ZipFile(archive) as zipped:
                manifest = json.loads(zipped.read("manifest.json").decode("utf-8"))
            metadata = manifest["runtime_metadata"]
            self.assertEqual(metadata["app_version"], APP_VERSION)
            self.assertEqual(metadata["schema_version"], SCHEMA_VERSION)
            self.assertEqual(metadata["database_user_version"], 0)
            self.assertEqual(metadata["database_integrity"], "ok")
            self.assertEqual(metadata["foreign_key_violations"], 0)

            database.unlink()
            shutil.rmtree(uploads)

            restore = subprocess.run(
                [
                    sys.executable,
                    str(ROOT / "scripts" / "restore_data.py"),
                    str(archive),
                    "--database-path",
                    str(database),
                    "--uploads-dir",
                    str(uploads),
                ],
                check=True,
                capture_output=True,
                text=True,
            )
            self.assertIn(
                f"Backup metadata: app={APP_VERSION} schema={SCHEMA_VERSION} db_user_version=0 integrity=ok fk_violations=0",
                restore.stdout,
            )

            with sqlite3.connect(database) as conn:
                self.assertEqual(conn.execute("select value from sample").fetchone()[0], "sqlite-content")
            self.assertIn("ops,10", (uploads / "dataset-1" / "sales.csv").read_text(encoding="utf-8"))

    def test_restore_refuses_to_overwrite_without_force(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            database = root / "source" / "app.db"
            uploads = root / "source" / "uploads"
            output = root / "backups"
            database.parent.mkdir(parents=True)
            uploads.mkdir(parents=True)
            with sqlite3.connect(database) as conn:
                conn.execute("create table sample(value text)")
            backup = subprocess.run(
                [
                    sys.executable,
                    str(ROOT / "scripts" / "backup_data.py"),
                    "--database-path",
                    str(database),
                    "--uploads-dir",
                    str(uploads),
                    "--output-dir",
                    str(output),
                ],
                check=True,
                capture_output=True,
                text=True,
            )

            target_db = root / "target" / "app.db"
            target_uploads = root / "target" / "uploads"
            target_db.parent.mkdir(parents=True)
            target_uploads.mkdir(parents=True)
            target_db.write_bytes(b"existing")
            refused = subprocess.run(
                [
                    sys.executable,
                    str(ROOT / "scripts" / "restore_data.py"),
                    backup.stdout.strip(),
                    "--database-path",
                    str(target_db),
                    "--uploads-dir",
                    str(target_uploads),
                ],
                capture_output=True,
                text=True,
            )
            self.assertNotEqual(refused.returncode, 0)
            self.assertEqual(target_db.read_bytes(), b"existing")


if __name__ == "__main__":
    unittest.main()
