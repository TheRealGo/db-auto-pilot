# QA Release Plan and Productization Roadmap

## Current Release Assessment

Status: not releasable.

Evidence:

- Backend source has been restored for the main workflow and now includes dataset/source metadata, proposal versions, approval decisions, materialization success/failure runs, dataset table metadata, lineage tables, query history audit fields, retained SQLite compatibility, and stale proposal approval protection. It is still simplified relative to the full production roadmap.
- Frontend source and package metadata have been restored for Vite/React and wired to the current proposal/version approval contract, but still need broader UX review against the full product requirements.
- SQLite metadata schema exists in `data/db_auto_pilot.db` and `backend/data/db_auto_pilot.db`, but both observed metadata databases have zero datasets.
- Built frontend and bytecode show a coherent intended workflow. The restored source now has a CI workflow, additive bootstrap/migration handling, privacy guardrails, and materialization staging-swap rollback tests, but still lacks production-grade migration versioning, release packaging, and full operational rollback runbooks.

## Productization Roadmap

### Phase 1: Source Restoration and Contract Baseline

- Restore maintainable backend source for config, schemas, repository, ingestion, proposals, materialization, querying, and API app, reconciling the simplified restored source with the richer retained bytecode/schema contract.
- Restore and review frontend source, package metadata, Tauri configuration, and build scripts.
- Add API contract tests for dataset lifecycle endpoints.
- Add DB migration/bootstrap logic for the existing metadata schema.
- Define sample fixtures for CSV, multi-sheet Excel, malformed files, and conflicting column semantics.

Exit criteria:

- Backend imports and app startup work from source.
- Unit tests run with `python3 -m unittest discover -s tests`.
- Frontend production build runs from source.
- README setup path works on a clean checkout.

### Phase 2: Integration Intelligence and Human Approval

- Implement deterministic source profiling and candidate generation before LLM calls.
- Add constrained LLM proposal loop with schema validation, evidence references, and local fallback.
- Implement proposal versioning, natural-language revision, approval decisions, and stale approval protection.
- Add UI states for open questions, confidence, risks, lineage, proposal revision history, and materialization proposal revision history.

Exit criteria:

- Proposal outputs are reproducible enough for test fixtures.
- LLM output validation rejects invented or unsupported references.
- Human approval path is auditable.
- Proposal and materialization proposal versions expose `change_summary` evidence in API, UI, and export payloads.

### Phase 3: Safe Materialization and Data Quality

- Implement materialization proposal generation with explicit cleansing policy.
- Guard generated code or replace it with a deterministic transformation DSL where practical.
- Enforce table/column/row limits, timeout, safe imports, and atomic SQLite writes with tested staging-table rollback behavior.
- Persist lineage, quality warnings, run results, and retry guidance.
- Reject materialization plans that normalize multiple canonical groups to the same output column or collide with reserved provenance columns.

Exit criteria:

- Failed materialization cannot expose partial approved tables.
- Failed materialization records error and retry guidance in materialization run history.
- Successful materialization can be traced from output columns back to source columns.
- Retry proposal uses prior failure evidence through `POST /datasets/{dataset_id}/materialization-proposal/{materialization_proposal_id}/retry`.

### Phase 4: Natural-Language Query, Analytics, and Audit UX

- Implement read-only SQL generation and validation.
- Enforce query table scope with SQLite authorization so generated and user-provided SQL can read only the selected approved materialized table.
- Add local fallback query intents for no-LLM mode.
- Store query history with SQL, explanation, result payload, target mode, and run/proposal version context.
- Implement analytics summaries and lightweight visualizations.
- Keep evidence export aligned with audit surfaces: proposal history, materialization proposal history, approvals, query history, and analytics context.
- Add UI smoke coverage for the full happy path and key failure states.

Exit criteria:

- Query endpoint cannot execute writes or multi-statement SQL.
- Query endpoint rejects metadata-table access, cross-dataset table access, and table-free SQL before execution.
- History and analytics refresh correctly after rematerialization.
- User can inspect result, SQL, explanation, and lineage without developer tools.

### Phase 5: Operational Hardening

- Add CI, packaging, logging, backup/export, configuration docs, and support diagnostics.
- Add privacy/security review for uploaded files and LLM data sharing.
- Add performance budgets for file size, row count, column count, and query latency.
- Add release checklist and rollback plan.

Exit criteria:

- CI gates are required for release.
- Operators can configure storage/LLM settings and collect logs.
- Data handling and LLM transmission behavior are documented and testable.

## First Implementation-Cycle QA Change Units

### CU-001: Restore Repository Buildability

- Owner/DRI: backend/frontend implementation DRI.
- Scope: restore source files and package/test metadata without changing product behavior beyond making the existing workflow maintainable.
- Reviewers: product/QA, backend/data, frontend/API owner.
- Required checks: `python3 -m unittest discover -s tests`, `python3 -m py_compile`, frontend build, `git diff --check`.
- Acceptance: clean checkout can install, start backend, build frontend, and run tests from source.

### CU-002: API Contract and Persistence Baseline

- Owner/DRI: backend/data.
- Scope: dataset, proposal, approval, materialization, query, history, and analytics endpoint contracts plus SQLite metadata repository behavior.
- Reviewers: product/QA, frontend/API owner.
- Required checks: unittest API tests with temporary SQLite database.
- Acceptance: all endpoints return stable response shapes and approval-gated endpoints reject invalid states.

### CU-003: Upload/Profile Fixtures

- Owner/DRI: backend/data.
- Scope: CSV/Excel parsing, source metadata, column profiling, error handling, and fixture set.
- Reviewers: product/QA.
- Required checks: unittest service tests for valid/malformed/edge-case files.
- Acceptance: profiling evidence is persisted and invalid files do not corrupt dataset state.

### CU-004: LLM Proposal Validation

- Owner/DRI: backend/data plus LLM integration owner.
- Scope: candidate generation, proposal schema validation, no-invention guardrails, local fallback, and revision behavior.
- Reviewers: product/QA.
- Required checks: unittest tests with stubbed LLM responses for valid, invalid, low-confidence, and no-key cases.
- Acceptance: invalid LLM references are rejected and low-confidence merges require review.

### CU-005: Safe Materialization

- Owner/DRI: backend/data.
- Scope: materialization proposal, code/plan guardrails, atomic SQLite writes, lineage, quality warnings, and retry context.
- Reviewers: product/QA, security-minded reviewer.
- Required checks: unittest tests for safe success, blocked unsafe code, timeout/resource failure, and rollback.
- Acceptance: generated transformations cannot perform unsafe imports or expose partial failed outputs.

### CU-006: Query and Analytics UX Contract

- Owner/DRI: frontend/API plus backend/data.
- Scope: natural-language query UI/API, SQL/explanation/results/history display, analytics summaries, empty/error/loading states.
- Reviewers: product/QA.
- Required checks: API unittest tests, frontend build, manual smoke path.
- Acceptance: approved dataset supports question -> SQL -> result -> history -> analytics; unapproved dataset is blocked clearly.

### CU-007: Release Operations

- Owner/DRI: release owner.
- Scope: setup docs, environment variables, CI, logging, data directory policy, backup/restore, privacy notes, release checklist.
- Reviewers: product/QA, operator.
- Required checks: clean-checkout setup dry run and CI pass.
- Acceptance: a new developer/operator can run and verify the product without hidden local state.

## Mandatory Verification Matrix

- Unit: repository, ingestion, proposal validation, materialization guard/validation, query SQL validation, analytics summaries.
- Contract: every endpoint listed in `docs/product-requirements.md`.
- Integration: upload multiple files -> proposal -> revision -> approval -> materialization -> query -> analytics.
- Security: SQL write rejection, generated-code import rejection, path traversal upload rejection, LLM no-invention validation.
- Recovery: failed upload, failed LLM call, invalid LLM JSON, failed materialization, retry after failed materialization.
- Release: clean checkout setup, backend start, frontend build, test command, diff whitespace check.

## Residual Risks

- Python bytecode is useful evidence but not a maintainable or sufficient source of truth.
- Built frontend may be stale relative to bytecode/API behavior.
- Generated materialization code is inherently high-risk unless tightly sandboxed or replaced by a constrained transformation plan.
- LLM use may transmit sensitive spreadsheet data; privacy controls and redaction policy are required before production use.
- Existing SQLite schema lacks explicit migration/version metadata in the observed database.
- CI and test source now exist, but regression risk remains high until clean-checkout setup, packaging, migration versioning, and broader fixture coverage are completed.
