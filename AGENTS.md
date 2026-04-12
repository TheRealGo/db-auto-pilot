# Repository Guidelines

## Project Structure & Module Organization
This repository is currently documentation-first. The root contains [README.md](/Users/therealgo/PythonProjects/db-auto-pilot/README.md) for the product overview and [Spec.md](/Users/therealgo/PythonProjects/db-auto-pilot/Spec.md) for feature requirements. There is no `src/` or `tests/` directory yet, so new code should establish a clear layout early:

- `src/db_auto_pilot/` for application code
- `tests/` for automated tests
- `docs/` for design notes or architecture decisions

Keep modules focused by responsibility, such as `ingest/`, `query/`, and `visualization/`.

## Build, Test, and Development Commands
No build system or test runner is configured yet. Until one is added, contributors should keep setup explicit in the PR. When Python packaging is introduced, prefer standard commands and document them in `README.md`, for example:

- `python -m pytest` to run tests
- `python -m db_auto_pilot` to run the app locally

Do not add undocumented helper scripts.

## Coding Style & Naming Conventions
Use Python 3 with 4-space indentation and PEP 8 naming:

- `snake_case` for modules, functions, and variables
- `PascalCase` for classes
- short, descriptive filenames such as `sql_generator.py`

Add type hints to new public functions. Keep functions small and side effects isolated, especially around LLM calls and database writes. If formatters or linters are added, prefer widely used tools such as `ruff` and `black`, and commit their config with the code that depends on them.

## Testing Guidelines
There is no test suite yet, but new features should include tests where practical. Use `tests/test_<module>.py` naming and mirror the package layout. Prioritize coverage for:

- Excel or file-ingestion edge cases
- SQL generation guardrails
- database read/write flows

Document any manual verification steps in the PR when automation is not yet available.

## Commit & Pull Request Guidelines
The current history uses short, imperative commit messages such as `Initial commit`. Continue with concise subjects like `Add ingestion service skeleton` or `Document query flow`.

Pull requests should include:

- a brief summary of the change
- linked issue or requirement reference when available
- test evidence or manual verification notes
- screenshots only for UI work

## Configuration & Security
Do not commit sample spreadsheets with sensitive data, API keys, or database credentials. Keep environment-specific settings in ignored local config files, and document required variables in `README.md`.
