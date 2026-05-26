from __future__ import annotations

import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "backend"))

from app.version import APP_VERSION, SCHEMA_VERSION
from scripts import release_check


class ReleaseCheckTests(unittest.TestCase):
    def test_release_check_writes_pass_report(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            report_path = Path(tmp) / "release-evidence.json"
            with patch.object(release_check.subprocess, "run", return_value=subprocess.CompletedProcess(["ok"], 0)):
                result = release_check.main(["--report-path", str(report_path)])

            self.assertEqual(result, 0)
            report = json.loads(report_path.read_text(encoding="utf-8"))
            self.assertEqual(report["status"], "passed")
            self.assertEqual(report["runtime_metadata"]["app_version"], APP_VERSION)
            self.assertEqual(report["runtime_metadata"]["schema_version"], SCHEMA_VERSION)
            self.assertEqual([step["name"] for step in report["steps"]], ["backend_unittest", "root_unittest", "py_compile", "frontend_build", "diff_check"])
            self.assertTrue(all(step["returncode"] == 0 for step in report["steps"]))

    def test_release_check_uses_npm_ci_when_lockfile_exists(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            report_path = Path(tmp) / "release-evidence.json"
            with patch.object(release_check.subprocess, "run", return_value=subprocess.CompletedProcess(["ok"], 0)):
                result = release_check.main(["--install-frontend", "--report-path", str(report_path)])

            self.assertEqual(result, 0)
            report = json.loads(report_path.read_text(encoding="utf-8"))
            install_step = next(step for step in report["steps"] if step["name"] == "frontend_install")
            self.assertEqual(install_step["command"], ["npm", "ci"])

    def test_release_check_writes_failure_report(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            report_path = Path(tmp) / "release-evidence.json"
            calls = [
                subprocess.CompletedProcess(["ok"], 0),
                subprocess.CompletedProcess(["fail"], 7),
            ]
            with patch.object(release_check.subprocess, "run", side_effect=calls):
                result = release_check.main(["--report-path", str(report_path)])

            self.assertEqual(result, 7)
            report = json.loads(report_path.read_text(encoding="utf-8"))
            self.assertEqual(report["status"], "failed")
            self.assertEqual(report["runtime_metadata"]["app_version"], APP_VERSION)
            self.assertEqual(report["runtime_metadata"]["schema_version"], SCHEMA_VERSION)
            self.assertEqual(report["failed_step"], "root_unittest")
            self.assertEqual(report["failed_returncode"], 7)


if __name__ == "__main__":
    unittest.main()
