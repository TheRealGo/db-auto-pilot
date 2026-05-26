from __future__ import annotations

import re
import shutil
from collections import Counter
from pathlib import Path
from typing import Any

import pandas as pd
from pandas.errors import EmptyDataError, ParserError
from fastapi import UploadFile


def slugify(value: str, fallback: str = "value") -> str:
    slug = re.sub(r"[^\w]+", "_", value.strip().lower(), flags=re.UNICODE).strip("_")
    if not slug:
        slug = fallback
    if slug[0].isdigit():
        slug = f"c_{slug}"
    return slug


def normalize_column_name(name: Any, index: int = 0) -> str:
    return slugify(str(name or ""), fallback=f"column_{index + 1}")


def normalize_column_names(columns: list[Any]) -> list[str]:
    counts: Counter[str] = Counter()
    normalized: list[str] = []
    for index, column in enumerate(columns):
        base = normalize_column_name(column, index)
        counts[base] += 1
        normalized.append(base if counts[base] == 1 else f"{base}_{counts[base]}")
    return normalized


def infer_logical_type(series: pd.Series) -> str:
    non_null = series.dropna()
    if non_null.empty:
        return "empty"
    if pd.api.types.is_bool_dtype(series):
        return "boolean"
    if pd.api.types.is_integer_dtype(series):
        return "integer"
    if pd.api.types.is_float_dtype(series):
        return "number"
    parsed_numeric = pd.to_numeric(non_null, errors="coerce")
    if parsed_numeric.notna().mean() >= 0.9:
        return "number"
    text_values = non_null.astype(str)
    if text_values.str.contains(r"[-/年年月日:]").mean() >= 0.5:
        parsed_dates = pd.to_datetime(non_null, errors="coerce")
        if parsed_dates.notna().mean() >= 0.8:
            return "date"
    unique_ratio = non_null.astype(str).nunique(dropna=True) / max(len(non_null), 1)
    return "category" if unique_ratio <= 0.8 else "text"


def sanitize_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    cleaned = df.copy()
    cleaned.columns = normalize_column_names(list(cleaned.columns))
    cleaned = cleaned.dropna(axis=0, how="all")
    for column in cleaned.columns:
        if pd.api.types.is_object_dtype(cleaned[column]):
            cleaned[column] = cleaned[column].map(lambda v: v.strip() if isinstance(v, str) else v)
    return cleaned


def validate_non_empty_dataframe(df: pd.DataFrame, label: str) -> pd.DataFrame:
    if len(df.columns) == 0:
        raise ValueError(f"{label} has no columns")
    if len(df) == 0:
        raise ValueError(f"{label} has no data rows")
    return df


def series_profile(series: pd.Series) -> dict[str, Any]:
    non_null = series.dropna()
    top_values = (
        non_null.astype(str).value_counts().head(5).rename_axis("value").reset_index(name="count").to_dict(orient="records")
        if not non_null.empty
        else []
    )
    return {
        "logical_type": infer_logical_type(series),
        "null_count": int(series.isna().sum()),
        "null_ratio": float(series.isna().mean()) if len(series) else 0.0,
        "distinct_count": int(non_null.nunique(dropna=True)),
        "sample_values": [str(v) for v in non_null.head(5).tolist()],
        "top_values": top_values,
    }


def file_profile(df: pd.DataFrame) -> dict[str, Any]:
    return {
        "rows": int(len(df)),
        "columns": {
            column: series_profile(df[column])
            for column in df.columns
        },
    }


def read_tabular_file(path: Path) -> dict[str, pd.DataFrame]:
    suffix = path.suffix.lower()
    try:
        if suffix == ".csv":
            frame = validate_non_empty_dataframe(sanitize_dataframe(pd.read_csv(path)), path.name)
            return {"default": frame}
        if suffix in {".xlsx", ".xlsm", ".xls"}:
            sheets = pd.read_excel(path, sheet_name=None)
            frames = {
                str(name): validate_non_empty_dataframe(sanitize_dataframe(frame), f"{path.name}/{name}")
                for name, frame in sheets.items()
            }
            if not frames:
                raise ValueError(f"{path.name} has no worksheets")
            return frames
    except (EmptyDataError, ParserError) as exc:
        raise ValueError(f"Unable to parse tabular file {path.name}: {exc}") from exc
    raise ValueError(f"Unsupported file type: {path.suffix}")


def get_sheet_count(path: Path) -> int:
    return len(read_tabular_file(path))


def save_upload(upload: UploadFile, uploads_dir: Path, dataset_id: str) -> Path:
    suffix = Path(upload.filename or "upload.csv").suffix
    target_dir = uploads_dir / dataset_id
    target_dir.mkdir(parents=True, exist_ok=True)
    target = target_dir / f"{slugify(Path(upload.filename or 'upload').stem)}{suffix}"
    with target.open("wb") as fh:
        shutil.copyfileobj(upload.file, fh)
    return target


def load_dataframe_map(sources: list[dict[str, Any]]) -> dict[str, pd.DataFrame]:
    frames: dict[str, pd.DataFrame] = {}
    for source in sources:
        source_frames = read_tabular_file(Path(source["file_path"]))
        sheet_name = source.get("sheet_name") or "default"
        frame = source_frames[sheet_name]
        frames[source["table_name"]] = frame
    return frames


def sqlite_type(series: pd.Series) -> str:
    logical = infer_logical_type(series)
    if logical in {"integer", "boolean"}:
        return "integer"
    if logical == "number":
        return "real"
    return "text"


def create_sqlite_table_from_dataframe(conn: Any, table_name: str, df: pd.DataFrame) -> None:
    df.to_sql(table_name, conn, if_exists="replace", index=False)
