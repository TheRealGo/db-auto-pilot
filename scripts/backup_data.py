from __future__ import annotations

import argparse
import hashlib
import json
import sqlite3
import sys
import zipfile
from datetime import datetime, timezone
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "backend"))

from app.version import APP_VERSION, SCHEMA_VERSION


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def add_file(archive: zipfile.ZipFile, path: Path, arcname: str, files: list[dict[str, object]]) -> None:
    archive.write(path, arcname)
    files.append({"path": arcname, "size": path.stat().st_size, "sha256": sha256(path)})


def database_metadata(database_path: Path) -> dict[str, object]:
    with sqlite3.connect(database_path) as conn:
        user_version = int(conn.execute("pragma user_version").fetchone()[0])
        integrity_row = conn.execute("pragma integrity_check").fetchone()
        integrity = str(integrity_row[0]) if integrity_row else "unknown"
        foreign_key_violations = len(conn.execute("pragma foreign_key_check").fetchall())
    return {
        "app_version": APP_VERSION,
        "schema_version": SCHEMA_VERSION,
        "database_user_version": user_version,
        "database_integrity": integrity,
        "foreign_key_violations": foreign_key_violations,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Create a db-auto-pilot data backup archive.")
    parser.add_argument("--database-path", type=Path, default=ROOT / "data" / "db_auto_pilot.db")
    parser.add_argument("--uploads-dir", type=Path, default=ROOT / "data" / "uploads")
    parser.add_argument("--output-dir", type=Path, default=ROOT / "backups")
    parser.add_argument("--label", default="")
    args = parser.parse_args()

    if not args.database_path.exists():
        raise SystemExit(f"Database does not exist: {args.database_path}")

    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")
    suffix = f"-{args.label}" if args.label else ""
    args.output_dir.mkdir(parents=True, exist_ok=True)
    archive_path = args.output_dir / f"db-auto-pilot-backup-{timestamp}{suffix}.zip"
    files: list[dict[str, object]] = []
    manifest = {
        "format": "db-auto-pilot-backup-v1",
        "created_at": datetime.now(timezone.utc).isoformat(),
        "database_archive_path": f"database/{args.database_path.name}",
        "uploads_archive_prefix": "uploads/",
        "runtime_metadata": database_metadata(args.database_path),
        "files": files,
    }

    with zipfile.ZipFile(archive_path, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        add_file(archive, args.database_path, manifest["database_archive_path"], files)
        if args.uploads_dir.exists():
            for path in sorted(item for item in args.uploads_dir.rglob("*") if item.is_file()):
                add_file(archive, path, f"uploads/{path.relative_to(args.uploads_dir).as_posix()}", files)
        archive.writestr("manifest.json", json.dumps(manifest, ensure_ascii=False, indent=2))

    print(archive_path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
