from __future__ import annotations

import re
import shutil
from collections import Counter
from pathlib import Path
from typing import Any

import pandas as pd
from fastapi import UploadFile


def slugify(value: str) -> str:
    text = re.sub(r"[^0-9a-zA-Z]+", "_", value.strip().lower())
    text = re.sub(r"_+", "_", text).strip("_")
    return text or "item"


def normalize_column_name(value: str) -> str:
    return slugify(value)


def normalize_column_names(values: list[str]) -> list[str]:
    counts: Counter[str] = Counter()
    normalized_names: list[str] = []
    for value in values:
        base_name = normalize_column_name(value)
        counts[base_name] += 1
        if counts[base_name] == 1:
            normalized_names.append(base_name)
        else:
            normalized_names.append(f"{base_name}_{counts[base_name]}")
    return normalized_names


def infer_logical_type(series: pd.Series) -> str:
    if pd.api.types.is_bool_dtype(series):
        return "boolean"
    if pd.api.types.is_integer_dtype(series):
        return "integer"
    if pd.api.types.is_float_dtype(series):
        return "number"
    if pd.api.types.is_datetime64_any_dtype(series):
        return "datetime"
    return "text"


def distinct_ratio(series: pd.Series) -> float:
    non_null = series.dropna()
    if non_null.empty:
        return 0.0
    return round(float(non_null.nunique()) / float(len(non_null)), 3)


def null_ratio(series: pd.Series) -> float:
    if len(series) == 0:
        return 0.0
    return round(float(series.isna().sum()) / float(len(series)), 3)


def normalized_candidates(column_name: str) -> list[str]:
    normalized = normalize_column_name(column_name)
    compact = normalized.replace("_", "")
    candidates = [normalized]
    if compact != normalized:
        candidates.append(compact)
    return candidates


def sanitize_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    cleaned = df.copy()
    cleaned.columns = [str(column).strip() for column in cleaned.columns]
    for column in cleaned.columns:
        if cleaned[column].dtype == "object":
            cleaned[column] = cleaned[column].map(
                lambda value: value.strip() if isinstance(value, str) else value
            )
    cleaned = cleaned.where(pd.notnull(cleaned), None)
    return cleaned


def read_tabular_file(path: Path) -> list[tuple[str, pd.DataFrame]]:
    if path.suffix.lower() == ".csv":
        return [("sheet1", sanitize_dataframe(pd.read_csv(path)))]
    sheets = pd.read_excel(path, sheet_name=None)
    return [(sheet_name, sanitize_dataframe(df)) for sheet_name, df in sheets.items()]


def get_sheet_count(path: Path) -> int:
    return len(read_tabular_file(path))


def save_upload(upload: UploadFile, destination: Path) -> None:
    destination.parent.mkdir(parents=True, exist_ok=True)
    with destination.open("wb") as handle:
        shutil.copyfileobj(upload.file, handle)


def file_profile(dataset_id: str, file_record: dict[str, Any]) -> list[dict[str, Any]]:
    path = Path(file_record["stored_path"])
    tables: list[dict[str, Any]] = []
    for sheet_name, df in read_tabular_file(path):
        display_name = f"{file_record['filename']} / {sheet_name}"
        db_table_name = (
            f"raw_{dataset_id.replace('-', '')[:8]}_"
            f"{slugify(Path(file_record['filename']).stem)}_{slugify(sheet_name)}"
        )[:55]
        columns = []
        normalized_names = normalize_column_names([str(column) for column in df.columns])
        for column, normalized_name in zip(df.columns, normalized_names, strict=True):
            series = df[column]
            sample_values = [value for value in series.head(3).tolist() if value is not None]
            columns.append(
                {
                    "original_name": str(column),
                    "normalized_name": normalized_name,
                    "db_name": normalized_name,
                    "logical_type": infer_logical_type(series),
                    "sample_values": sample_values,
                    "null_ratio": null_ratio(series),
                    "distinct_ratio": distinct_ratio(series),
                    "normalized_candidates": normalized_candidates(str(column)),
                }
            )
        tables.append(
            {
                "table_name": db_table_name,
                "display_name": display_name,
                "dataset_id": dataset_id,
                "source_file_id": file_record["id"],
                "source_filename": file_record["filename"],
                "sheet_name": sheet_name,
                "row_count": int(len(df.index)),
                "columns": columns,
            }
        )
    return tables


def load_dataframe_map(dataset_id: str, file_records: list[dict[str, Any]]) -> dict[str, pd.DataFrame]:
    dataframes: dict[str, pd.DataFrame] = {}
    for file_record in file_records:
        path = Path(file_record["stored_path"])
        for sheet_name, df in read_tabular_file(path):
            db_table_name = (
                f"raw_{dataset_id.replace('-', '')[:8]}_"
                f"{slugify(Path(file_record['filename']).stem)}_{slugify(sheet_name)}"
            )[:55]
            renamed = df.copy()
            renamed.columns = normalize_column_names([str(column) for column in renamed.columns])
            renamed.insert(0, "_source_sheet", sheet_name)
            renamed.insert(0, "_source_file", file_record["filename"])
            renamed.insert(0, "_row_index", range(1, len(renamed.index) + 1))
            dataframes[db_table_name] = renamed
    return dataframes


def sqlite_type(series: pd.Series) -> str:
    if pd.api.types.is_integer_dtype(series):
        return "INTEGER"
    if pd.api.types.is_float_dtype(series):
        return "REAL"
    return "TEXT"


def create_sqlite_table_from_dataframe(conn: Any, table_name: str, df: pd.DataFrame) -> None:
    conn.execute(f'DROP TABLE IF EXISTS "{table_name}"')
    columns_sql = []
    for column in df.columns:
        columns_sql.append(f'"{column}" {sqlite_type(df[column])}')
    conn.execute(f'CREATE TABLE "{table_name}" ({", ".join(columns_sql)})')
    placeholders = ", ".join(["?"] * len(df.columns))
    column_names = ", ".join(f'"{column}"' for column in df.columns)
    rows = df.where(pd.notnull(df), None).values.tolist()
    conn.executemany(
        f'INSERT INTO "{table_name}" ({column_names}) VALUES ({placeholders})',
        rows,
    )
