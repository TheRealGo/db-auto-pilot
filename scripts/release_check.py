from __future__ import annotations

import argparse
import json
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "backend"))

from app.version import APP_VERSION, SCHEMA_VERSION


def run(command: list[str], cwd: Path = ROOT) -> dict[str, Any]:
    print(f"$ {' '.join(command)}", flush=True)
    started = time.monotonic()
    completed = subprocess.run(command, cwd=cwd, check=False)
    result = {
        "command": command,
        "cwd": str(cwd.relative_to(ROOT) if cwd != ROOT else "."),
        "returncode": completed.returncode,
        "duration_seconds": round(time.monotonic() - started, 3),
    }
    if completed.returncode != 0:
        raise subprocess.CalledProcessError(completed.returncode, command)
    return result


def python_files() -> list[str]:
    paths: list[str] = []
    for base in [ROOT / "backend" / "app", ROOT / "backend" / "tests", ROOT / "scripts", ROOT / "tests"]:
        paths.extend(str(path.relative_to(ROOT)) for path in sorted(base.rglob("*.py")))
    return paths


def frontend_install_command() -> list[str]:
    if (ROOT / "frontend" / "package-lock.json").exists():
        return ["npm", "ci"]
    return ["npm", "install"]


def write_report(path: Path, report: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Run db-auto-pilot release verification checks.")
    parser.add_argument(
        "--install-frontend",
        action="store_true",
        help="Run npm install in frontend before the production build.",
    )
    parser.add_argument(
        "--report-path",
        type=Path,
        help="Write machine-readable release evidence JSON to this path.",
    )
    args = parser.parse_args(argv)

    report: dict[str, Any] = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "status": "running",
        "runtime_metadata": {
            "app_version": APP_VERSION,
            "schema_version": SCHEMA_VERSION,
        },
        "install_frontend": bool(args.install_frontend),
        "steps": [],
    }
    steps: list[tuple[str, list[str], Path]] = [
        ("backend_unittest", [sys.executable, "-m", "unittest", "discover", "-s", "backend/tests"], ROOT),
        ("root_unittest", [sys.executable, "-m", "unittest", "discover", "-s", "tests"], ROOT),
        ("py_compile", [sys.executable, "-m", "py_compile", *python_files()], ROOT),
    ]
    if args.install_frontend:
        steps.append(("frontend_install", frontend_install_command(), ROOT / "frontend"))
    steps.extend(
        [
            ("frontend_build", ["npm", "run", "build"], ROOT / "frontend"),
            ("diff_check", ["git", "diff", "--check"], ROOT),
        ]
    )

    try:
        for name, command, cwd in steps:
            result = run(command, cwd)
            result["name"] = name
            report["steps"].append(result)
    except subprocess.CalledProcessError as exc:
        report["status"] = "failed"
        report["failed_step"] = name
        report["failed_returncode"] = exc.returncode
        if args.report_path:
            write_report(args.report_path, report)
        return exc.returncode
    report["status"] = "passed"
    if args.report_path:
        write_report(args.report_path, report)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
