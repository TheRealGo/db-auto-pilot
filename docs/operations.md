# Operations Notes

## Local Runtime

Backend:

```sh
cd backend
cp .env.example .env
python -m pip install -r requirements.lock.txt
uvicorn app.main:app --host 127.0.0.1 --port 8765
```

Frontend:

```sh
cd frontend
npm ci
npm run dev
```

The frontend development server proxies API calls to `http://127.0.0.1:8765`.
`backend/requirements.txt` remains the short direct-dependency list for review, while `backend/requirements.lock.txt` records the pinned environment used by release checks.

## Data Handling

- Uploaded files and SQLite databases are local runtime data and must not be committed.
- LLM usage is disabled by default in `.env.example`.
- LLM proposal calls use a `metadata_only` data policy: prompts may include table names, column names, row counts, candidate references, similarity scores, cleansing references, and user feedback text.
- LLM proposal prompts must not include uploaded files, raw rows, sample values, or cell values. Keep this invariant covered by tests before enabling model calls in production.
- Uploads larger than `DB_AUTO_PILOT_MAX_UPLOAD_MB` are rejected before dataset metadata is created. The current default is 50 MB.
- Materialization rejects outputs above `DB_AUTO_PILOT_MAX_MATERIALIZATION_ROWS` or `DB_AUTO_PILOT_MAX_MATERIALIZATION_COLUMNS` before publishing a SQLite table. Current defaults are 100,000 rows and 200 columns.
- Query requests are capped by `DB_AUTO_PILOT_QUERY_ROW_LIMIT` and the request `limit`; existing SQL `LIMIT` clauses cannot return more rows than the request cap. The current configured default is 500 rows.
- History endpoints bound list sizes at the API boundary: query history max 200 entries per request, proposal and materialization proposal history max 100 entries per request.
- `backend/.env.example` lists the supported local runtime knobs for storage paths, LLM policy/model, upload limits, materialization limits, and query row limits.
- `DB_AUTO_PILOT_CORS_ALLOW_ORIGINS` is a comma-separated allowlist for browser clients. Defaults are the Vite local development origins; wildcard credentialed CORS is not enabled.
- `GET /settings` returns non-secret operational limits (`max_upload_mb`, materialization row/column caps, query row cap, and CORS allowlist) so support and release checks can confirm the active runtime policy.
- When settings are saved through the API, the local settings file is written with owner-only permissions (`0600`) because it may contain an OpenAI API key.
- A persisted OpenAI API key can be cleared through `PUT /settings` with `clear_openai_api_key=true` or from the frontend settings panel. Environment-provided keys must be removed from the environment or `.env` file.
- `GET /diagnostics` returns non-secret runtime diagnostics: app/schema version, SQLite `user_version`, readiness booleans, SQLite `integrity_check`, foreign-key violation count, migration backup count/latest filename, metadata table counts, and the same non-secret settings payload. It intentionally omits filesystem paths and secret values.
- The frontend sidebar exposes settings and diagnostics without secret values. Operators can set the OpenAI model, toggle LLM proposal use, register an API key, and confirm active upload/query/materialization limits.
- The dataset workspace displays source column profiles, proposal/materialization proposal revisions, materialization runs, lineage, quality warnings, governed SQL/results/history, and analytics from the API payloads.
- Every API response includes `x-request-id`. Supplying an `x-request-id` request header preserves it; otherwise the backend generates one. Access logs include request id, method, path, status, and duration, but do not log request or response bodies.
- Materialization success and failure attempts are recorded in `materialization_runs`. Failed attempts keep the existing/pending dataset table unchanged and include error text plus retry guidance in dataset detail responses.
- A failed materialization can seed a new retry proposal with `POST /datasets/{dataset_id}/materialization-proposal/{materialization_proposal_id}/retry`; the retry plan records the failed run id, error, retry guidance, and user feedback.
- `GET /datasets/{dataset_id}/proposals` returns proposal versions with feedback and `change_summary` so approvers can review natural-language revision effects before approval.
- `GET /datasets/{dataset_id}/materialization-proposals` returns materialization plan versions with feedback and `change_summary`, including retry-plan evidence when generated from a failed run.
- `GET /datasets/{dataset_id}/export` returns a JSON evidence bundle containing dataset metadata, non-secret runtime provenance, proposal history, materialization proposal history, approvals, query history, and analytics context for audit/support review. Runtime provenance includes app/schema/SQLite `user_version`, readiness booleans, integrity/foreign-key summaries, migration backup summary, metadata counts, and non-secret settings. Exported source profiles redact sampled cell values and top-value lists by default. Exported query history omits result preview rows unless `include_query_previews=true` is explicitly supplied; that opt-in preview is still capped at 5 rows per entry. Use local dataset/query detail APIs only when an operator explicitly needs raw profiling examples or larger result previews.
- Back up `DB_AUTO_PILOT_DATABASE_PATH` and `DB_AUTO_PILOT_UPLOADS_DIR` together; metadata without uploaded source files is not sufficient for audit or rematerialization. Use `python scripts/backup_data.py --database-path <db> --uploads-dir <uploads> --output-dir <backup-dir>` to create a manifest/checksum zip with app/schema/database integrity metadata, and `python scripts/restore_data.py <archive> --database-path <db> --uploads-dir <uploads>` to restore into empty targets. Restore prints the backup runtime metadata so operators can confirm schema/user_version/integrity before smoke checks. Use `--force` only when intentionally replacing existing local runtime data.

## Release Gate

Use [release-checklist.md](release-checklist.md) as the release acceptance checklist.

Run these before release:

```sh
python scripts/release_check.py --install-frontend
```

The script runs backend unittest suites, Python compile checks, frontend production build, and `git diff --check`. On a prepared local workspace, omit `--install-frontend` to avoid reinstalling Node dependencies. When installation is requested and `frontend/package-lock.json` is present, the script uses `npm ci` for lockfile-backed reproducibility.

For auditable release evidence, write the machine-readable report:

```sh
python scripts/release_check.py --report-path release-evidence.json
```

The report records UTC generation time, app/schema runtime metadata, pass/fail status, each check command, working directory, return code, and duration. CI writes this report and uploads it as a `release-evidence` artifact.
Local `release-evidence*.json` files are ignored by git because they are generated build evidence, not source.

## Rollback

This first-cycle implementation is local-file based. Rollback is a file-level operation:

Before the repository applies schema migrations to an existing SQLite file, it writes a sibling backup named like:

```text
db_auto_pilot.pre-migration-v0-20260526000000-abcdef12.db.bak
```

The repository then records the applied application schema level in SQLite `pragma user_version`. Current schema version is `2`; a database already at that version is not backed up again on normal startup.

1. Stop the backend process.
2. Restore the previous SQLite database from the matching `*.pre-migration-*.db.bak` file or from the external backup set.
3. Restore the matching upload directory from the same backup point.
4. Restart the backend.
5. Run a smoke query against the restored dataset and verify query history timestamps.

Future production cycles should replace the ad hoc migration path with numbered migration files, dry-run checks, and point-in-time backup/restore tests.
