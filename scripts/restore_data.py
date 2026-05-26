from __future__ import annotations

import argparse
import hashlib
import json
import shutil
import tempfile
import zipfile
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def safe_extract(archive: zipfile.ZipFile, target: Path) -> None:
    target = target.resolve()
    for member in archive.infolist():
        destination = (target / member.filename).resolve()
        if target not in destination.parents and destination != target:
            raise ValueError(f"Unsafe archive member path: {member.filename}")
    archive.extractall(target)


def main() -> int:
    parser = argparse.ArgumentParser(description="Restore a db-auto-pilot data backup archive.")
    parser.add_argument("archive", type=Path)
    parser.add_argument("--database-path", type=Path, default=ROOT / "data" / "db_auto_pilot.db")
    parser.add_argument("--uploads-dir", type=Path, default=ROOT / "data" / "uploads")
    parser.add_argument("--force", action="store_true", help="Overwrite existing database/uploads targets.")
    args = parser.parse_args()

    if not args.archive.exists():
        raise SystemExit(f"Backup archive does not exist: {args.archive}")
    if not args.force and (args.database_path.exists() or args.uploads_dir.exists()):
        raise SystemExit("Refusing to overwrite existing database/uploads without --force")

    with tempfile.TemporaryDirectory() as tmp:
        extract_dir = Path(tmp)
        with zipfile.ZipFile(args.archive) as archive:
            safe_extract(archive, extract_dir)
        manifest = json.loads((extract_dir / "manifest.json").read_text(encoding="utf-8"))
        if manifest.get("format") != "db-auto-pilot-backup-v1":
            raise SystemExit("Unsupported backup archive format")
        runtime_metadata = manifest.get("runtime_metadata") or {}

        for entry in manifest.get("files", []):
            restored = extract_dir / entry["path"]
            if sha256(restored) != entry["sha256"]:
                raise SystemExit(f"Checksum mismatch for {entry['path']}")

        database_source = extract_dir / manifest["database_archive_path"]
        uploads_source = extract_dir / manifest.get("uploads_archive_prefix", "uploads")

        args.database_path.parent.mkdir(parents=True, exist_ok=True)
        if args.force and args.database_path.exists():
            args.database_path.unlink()
        shutil.copy2(database_source, args.database_path)

        if args.force and args.uploads_dir.exists():
            shutil.rmtree(args.uploads_dir)
        args.uploads_dir.mkdir(parents=True, exist_ok=True)
        if uploads_source.exists():
            for source in sorted(item for item in uploads_source.rglob("*") if item.is_file()):
                relative = source.relative_to(uploads_source)
                target = args.uploads_dir / relative
                target.parent.mkdir(parents=True, exist_ok=True)
                shutil.copy2(source, target)

    if runtime_metadata:
        print(
            "Backup metadata: "
            f"app={runtime_metadata.get('app_version', 'unknown')} "
            f"schema={runtime_metadata.get('schema_version', 'unknown')} "
            f"db_user_version={runtime_metadata.get('database_user_version', 'unknown')} "
            f"integrity={runtime_metadata.get('database_integrity', 'unknown')} "
            f"fk_violations={runtime_metadata.get('foreign_key_violations', 'unknown')}"
        )
    print(f"Restored {args.archive}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
