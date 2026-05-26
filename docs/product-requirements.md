# Product Requirements and Acceptance Criteria

## Goal

Build a production-ready desktop/web application that helps users consolidate multiple Excel/CSV files into a governed SQLite database, with LLM-assisted column meaning analysis, integration/cleansing proposals, explicit human approval, natural-language SQL search, query history, and simple analysis/visualization.

## Users

- Business analyst: uploads operational spreadsheets, validates merge logic, asks natural-language questions, exports evidence.
- Data steward: reviews lineage, cleansing policy, approval history, and release quality before operational use.
- Operator/developer: configures LLM access, storage paths, logs, checks, and recovery procedures.

## Core Workflow

1. Dataset creation and upload
   - User creates a named dataset and uploads multiple `.csv`, `.xls`, or `.xlsx` files.
   - The system records source filename, type, sheets, row/column counts, detected headers, inferred types, and parse warnings.
   - The UI displays uploaded source profiles, including inferred logical type, null ratio, and distinct count for each column.
   - Unsupported, oversized, encrypted, empty, or malformed files fail with actionable errors and do not corrupt existing datasets.

2. Source profiling and column meaning analysis
   - The system profiles every table/sheet/column: names, examples, null rate, distinct count, numeric/date/category candidates, and quality warnings.
   - LLM analysis proposes business meaning, likely synonyms, join keys, units, date formats, identifier columns, and confidence.
   - LLM output must be constrained by observed source metadata and must not invent files, tables, columns, or values.

3. Integration proposal
   - The system proposes merge/keep-separate/review decisions for candidate columns and tables.
   - Each proposal includes canonical names, source-column lineage, confidence, rationale, open questions, cleansing requirements, and risks.
   - Low-confidence or lossy transformations require explicit review status rather than silent approval.

4. Natural-language refinement and approval
   - User can provide natural-language feedback such as "treat customer code and client id as the same key" or "keep regional sales separate."
   - The system creates a new proposal version, preserves previous versions, and explains changes.
   - Proposal revision history must be visible before approval and included in evidence export.
   - Approval records who approved what proposal, when, and with the final decision payload.

5. Materialization proposal and SQLite creation
   - After integration approval, the system proposes the executable materialization plan: output tables, normalization/cleansing actions, expected outputs, quality expectations, and risk notes.
   - Materialization proposal revisions preserve prior versions and summarize plan changes, retry evidence, and user feedback.
   - User approval triggers SQLite table creation in a controlled execution path.
   - Created tables include stable physical names, schema metadata, lineage, quality warnings, and provenance columns sufficient to trace source rows/columns.

6. Natural-language SQL search
   - User asks questions against approved/materialized tables.
   - Advanced users can edit and rerun governed `SELECT` SQL through the same scoped query guard.
   - The system returns SQL, explanation, result rows, target table/mode, warnings, and execution errors where applicable.
   - Only read-only SQLite `SELECT` or `WITH ... SELECT` statements are allowed.
   - SQL must be scoped to the selected dataset's approved materialized table; metadata tables, other datasets, and table-free ad hoc expressions are rejected.
- Query history stores question, SQL, explanation, result summary/payload, target mode, proposal/materialization proposal context, and timestamp.

7. Analysis and visualization
   - The system computes simple table analytics: row counts, column counts, numeric summaries, and top categorical values.
   - The UI displays summaries and simple visual bars/charts without requiring the user to write SQL.
   - Analytics must clearly identify the table/mode and refresh after rematerialization.

8. Operations and release readiness
   - The system has deterministic local fallbacks where LLM access is unavailable, but fallbacks must be labeled.
   - Configuration, diagnostics, logs, storage paths, migrations, backups, and error handling are documented and visible through non-secret UI/API surfaces where practical.
   - CI must run unit tests, compile checks, frontend build checks, and whitespace checks before release.

## API Contract Initial Draft

The current built frontend and bytecode indicate these endpoints should be restored and covered by contract tests:

- `GET /datasets`
- `POST /datasets` with file upload payload
- `GET /datasets/{dataset_id}`
- `POST /datasets/{dataset_id}/proposal`
- `POST /datasets/{dataset_id}/proposal/revise`
- `GET /datasets/{dataset_id}/proposals`
- `POST /datasets/{dataset_id}/approve`
- `POST /datasets/{dataset_id}/materialization-proposal`
- `GET /datasets/{dataset_id}/materialization-proposals`
- `POST /datasets/{dataset_id}/materialization-proposal/{materialization_proposal_id}/retry`
- `POST /datasets/{dataset_id}/materialization-proposal/{materialization_proposal_id}/approve`
- `POST /datasets/{dataset_id}/query`
- `GET /datasets/{dataset_id}/query-history`
- `GET /datasets/{dataset_id}/analytics?target_mode=merged`
- `GET /datasets/{dataset_id}/export`

## Acceptance Criteria

### Upload and Profiling

- Given two valid CSV files and one valid Excel workbook, when the user uploads them, then a dataset is created with all source files, sheet metadata, row/column profiles, and parse warnings persisted.
- Given a malformed, empty, encrypted, unsupported, or oversized file, when uploaded, then the API returns a clear validation error and existing dataset state is unchanged.
- Given duplicate column names or hidden/blank Excel sheets, when profiled, then the system records disambiguated names and warnings instead of silently dropping data.

### LLM Proposal Quality

- Given profiled sources, when proposal generation runs with LLM enabled, then every proposed merge/cleanse decision follows the proposal contract and references only observed source files, tables, columns, and candidate IDs.
- Given LLM proposal generation is enabled, then the prompt follows the `metadata_only` policy and excludes uploaded files, raw rows, sample values, and cell values.
- Given insufficient evidence for a merge, then the proposal marks the item `review` with `review_required=true` or keeps it separate; approval requires explicit notes and must not finalize a low-confidence lossy merge silently.
- Given no LLM API key, then deterministic local fallback produces a limited proposal or clear "not enough candidates" error that is visible to the user.

### Refinement and Approval

- Given user feedback on an existing proposal, when revision runs, then a new version is stored with feedback, previous proposal context, changed decisions, and unchanged lineage.
- Given multiple proposal versions, then the UI and evidence export show revision history and `change_summary` for each version.
- Given a proposal approval request with a mismatched proposal ID or stale version, then approval is rejected.
- Given approval succeeds, then dataset status changes to awaiting materialization approval and the approved proposal ID is persisted.

### Materialization

- Given an approved proposal, when materialization proposal generation runs, then output includes normalization decisions, deterministic plan, expected outputs, quality expectations, and risk notes.
- Given multiple materialization proposal versions or retry proposals, then the UI and evidence export show version history, feedback, change summaries, and retry context.
- Given generated materialization code, then execution is sandboxed/guarded, blocks unsafe imports/statements, enforces time/resource limits, and validates returned table/lineage structures.
- Given materialization succeeds, then SQLite tables, schema metadata, approval decisions, lineage, run result, and quality warnings are persisted atomically.
- Given materialization fails, then the failure stage, error, retry guidance, and previous run context are visible; no partial tables are exposed as approved.

### Natural-Language Query

- Given approved tables, when a user asks a supported question, then the response includes SQL, explanation, result rows, and a history entry.
- Given generated SQL is not read-only `SELECT` or `WITH ... SELECT`, then it is rejected before execution.
- Given user-provided or generated SQL references metadata tables, another dataset table, or no selected dataset table, then it is rejected before execution.
- Given no LLM API key, then local fallback can answer count, grouped sum, average, and preview style questions and labels the fallback in the explanation.
- Given an unapproved dataset, then query and analytics endpoints reject access with a clear approval-required message.

### Analytics and History

- Given materialized tables, then analytics returns row count, column count, numeric summaries, and category summaries for each table.
- Given rematerialization, then stale query history is cleared or explicitly versioned so users cannot confuse results across table versions.
- Given query/history/timeline display, then every entry includes timestamp, status, and enough identifiers to audit which proposal/materialization run produced it.
- Given materialization has completed or failed, then the UI displays persisted lineage, quality warnings, and materialization run evidence without requiring direct database access.
- Given a dataset, then evidence export returns dataset detail, non-secret runtime provenance, proposal history, materialization proposal history, approvals, query history, and analytics context as JSON without requiring direct database access; raw sampled cell values and query result previews are redacted by default unless an operator explicitly opts into a bounded preview.

### Release Gates

- `python3 -m unittest discover -s tests` passes.
- `python3 -m py_compile` passes for restored backend source.
- Frontend package metadata is restored and production build passes.
- `git diff --check` passes.
- A smoke test covers upload -> proposal -> approval -> materialization -> query -> analytics.
