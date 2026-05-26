# db-auto-pilot

db-auto-pilot is intended to turn multiple messy Excel/CSV files into an approved SQLite database that can be searched in natural language and inspected with result tables, generated SQL, explanations, history, and lightweight analytics.

## Current Repository State

This checkout is an active productization baseline, not a release-ready build. Backend and frontend source/package metadata have been restored, the current API covers the upload -> proposal -> versioned approval -> materialization -> governed query -> history -> analytics path, and SQLite metadata now includes proposal versions, approval decisions, materialization runs, dataset table metadata, lineage, and retained-schema query-history compatibility. The repository also retains built frontend assets under `frontend/dist`, SQLite metadata databases under `data/` and `backend/data/`, and Python bytecode caches under `backend/app/__pycache__` and `backend/tests/__pycache__`.

The remaining SQLite schema and built frontend indicate the intended workflow:

1. Upload one or more Excel/CSV files into a dataset.
2. Generate an LLM-assisted integration proposal.
3. Revise or approve the proposal with natural-language feedback. Approval requires the latest `proposal_id` and `proposal_version`, so stale or tampered approvals are rejected. Proposal revision history and change summaries are available in the UI and evidence export.
4. Generate and approve a materialization proposal. Materialization proposal versions and retry-plan change summaries are retained for review.
5. Create SQLite tables with lineage and quality metadata.
6. Ask natural-language questions, optionally edit governed read-only SQL, view SQL/explanations/results, and retain query history.
7. Review source profiles, lineage, quality warnings, diagnostics, settings, analytics, and timeline/history for the dataset.

## Product Specification

The first requirements and acceptance criteria are in [docs/product-requirements.md](docs/product-requirements.md).

The productization roadmap, release gates, and first implementation-cycle QA change units are in [docs/qa-release-plan.md](docs/qa-release-plan.md).

Operational setup, data-handling notes, and release/rollback checks are in [docs/operations.md](docs/operations.md).

Release acceptance steps are in [docs/release-checklist.md](docs/release-checklist.md).

## Verification

Use the repository-standard release check. The local `backend/.venv` currently contains the backend dependencies:

```sh
./backend/.venv/bin/python scripts/release_check.py
```

Use `--install-frontend` on a clean checkout where `frontend/node_modules` is not present.

Do not use `pytest` as the default runner for this project.
