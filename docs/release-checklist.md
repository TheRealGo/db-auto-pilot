# Release Checklist

Use this checklist before tagging or distributing a build.

## Scope

- Confirm the change has a clear owner and review notes.
- Confirm affected surfaces: backend API, SQLite schema/data, frontend UI, operations docs, CI/release scripts.
- Confirm any LLM prompt or uploaded-data handling change preserves the `metadata_only` privacy contract unless explicitly approved.

## Verification

Run:

```sh
python scripts/release_check.py --install-frontend --report-path release-evidence.json
```

The generated report must show `status: passed`.

For workflow changes, run a manual smoke:

1. Upload at least one CSV fixture.
2. Generate an integration proposal.
3. Approve integration and materialization.
4. Ask a natural-language question.
5. Check SQL, result rows, history, analytics, lineage/quality, approvals, diagnostics, and evidence export.

## Data And Migration

- Back up the SQLite database and uploads directory together before testing on retained data.
- Check `GET /diagnostics` for expected app/schema/database versions.
- Verify failed upload/materialization paths do not publish partial approved state.
- Confirm generated local evidence files are not committed.

## Rollback

- Identify the previous working artifact or commit.
- Identify the matching database/upload backup set.
- Document whether rollback is file restore, database restore, or forward-fix.
- Re-run a smoke query after rollback.

## Acceptance

- Required checks passed.
- Reviewers accepted or documented residual risks.
- Release evidence report is attached to the release or CI artifact.
- Operations notes are updated when runtime behavior or recovery steps changed.
