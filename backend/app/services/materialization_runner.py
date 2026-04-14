from __future__ import annotations

import base64
import builtins
import json
import pickle
import resource
import sys
from typing import Any

import pandas as pd

from app.services.materialization import (
    ALLOWED_IMPORTS,
    BANNED_IMPORT_PREFIXES,
    MaterializationExecutionError,
    MaterializationGuardError,
)


def _deserialize_frame(serialized_frame: str) -> pd.DataFrame:
    return pickle.loads(base64.b64decode(serialized_frame.encode("ascii")))


def _serialize_frame(frame: pd.DataFrame) -> str:
    return base64.b64encode(pickle.dumps(frame)).decode("ascii")


def _guarded_import(name: str, globals_dict: dict[str, Any] | None = None, locals_dict: dict[str, Any] | None = None, fromlist: tuple[str, ...] = (), level: int = 0) -> Any:
    root = name.split(".")[0]
    if root in BANNED_IMPORT_PREFIXES or root not in ALLOWED_IMPORTS:
        raise MaterializationGuardError(
            f"Import not allowed in generated materialization code: {name}",
            {"status": "failed", "imports": [name], "violations": [{"type": "import", "value": name}]},
        )
    return builtins.__import__(name, globals_dict, locals_dict, fromlist, level)


def _safe_builtins() -> dict[str, Any]:
    allowed = {
        "abs",
        "all",
        "any",
        "dict",
        "enumerate",
        "filter",
        "float",
        "int",
        "isinstance",
        "len",
        "list",
        "max",
        "min",
        "next",
        "print",
        "range",
        "round",
        "set",
        "sorted",
        "str",
        "sum",
        "tuple",
        "zip",
    }
    safe = {name: getattr(builtins, name) for name in allowed}
    safe["__import__"] = _guarded_import
    return safe


def _apply_limits(memory_limit_bytes: int, cpu_limit_seconds: int) -> None:
    try:
        resource.setrlimit(resource.RLIMIT_CPU, (cpu_limit_seconds, cpu_limit_seconds + 1))
    except Exception:
        pass
    for limit_name in ("RLIMIT_AS", "RLIMIT_DATA"):
        limit = getattr(resource, limit_name, None)
        if limit is None:
            continue
        try:
            resource.setrlimit(limit, (memory_limit_bytes, memory_limit_bytes))
        except Exception:
            pass


def _print(payload: dict[str, Any]) -> None:
    sys.stdout.write(json.dumps(payload, ensure_ascii=False))


def main() -> int:
    try:
        payload = json.loads(sys.stdin.read())
        generated_code = payload["generated_code"]
        plan = payload["plan"]
        memory_limit_bytes = int(payload.get("memory_limit_bytes", 0))
        cpu_limit_seconds = int(payload.get("cpu_limit_seconds", 0))
        source_frames = {
            name: _deserialize_frame(frame)
            for name, frame in payload["source_frames"].items()
        }
        if memory_limit_bytes > 0 and cpu_limit_seconds > 0:
            _apply_limits(memory_limit_bytes, cpu_limit_seconds)

        namespace: dict[str, Any] = {
            "__builtins__": _safe_builtins(),
            "source_frames": source_frames,
            "PLAN": plan,
            "pd": pd,
            "json": json,
        }
        exec(generated_code, namespace, namespace)
        result = namespace.get("result")
        if not isinstance(result, dict):
            raise MaterializationExecutionError("Generated materialization code did not return a result dictionary.")
        merged_tables = []
        for item in result.get("merged_tables", []):
            if not isinstance(item, dict) or "dataframe" not in item:
                raise MaterializationExecutionError("Generated materialization code returned an invalid merged table.")
            dataframe = item["dataframe"]
            if not isinstance(dataframe, pd.DataFrame):
                raise MaterializationExecutionError("Generated materialization code returned a non-DataFrame merged table.")
            merged_tables.append(
                {
                    **{key: value for key, value in item.items() if key != "dataframe"},
                    "dataframe": _serialize_frame(dataframe),
                }
            )
        _print(
            {
                "status": "ok",
                "merged_tables": merged_tables,
                "lineage_items": result.get("lineage_items", []),
                "execution_notes": result.get("execution_notes", []),
            }
        )
        return 0
    except MaterializationGuardError as exc:
        _print(
            {
                "status": "error",
                "error_stage": "guard",
                "error": str(exc),
                "guard_summary": exc.summary,
            }
        )
        return 0
    except MaterializationExecutionError as exc:
        _print(
            {
                "status": "error",
                "error_stage": "execution",
                "error": str(exc),
            }
        )
        return 0
    except Exception as exc:
        _print(
            {
                "status": "error",
                "error_stage": "execution",
                "error": f"Materialization runner failed: {exc}",
            }
        )
        return 0


if __name__ == "__main__":
    raise SystemExit(main())
