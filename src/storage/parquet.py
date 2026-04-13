from __future__ import annotations

import shutil
from pathlib import Path

import pandas as pd


def _ensure_parquet_engine() -> None:
    try:
        import pyarrow  # noqa: F401
        return
    except ModuleNotFoundError as first_error:
        try:
            import fastparquet  # noqa: F401
            return
        except ModuleNotFoundError as second_error:
            raise RuntimeError(
                "Parquet support requires 'pyarrow' or 'fastparquet'. "
                "Install dependencies from requirements.txt before running ingestion."
            ) from second_error
        raise RuntimeError(
            "Parquet support requires 'pyarrow' or 'fastparquet'. "
            "Install dependencies from requirements.txt before running ingestion."
        ) from first_error


def normalize_date_columns(df: pd.DataFrame, columns: list[str]) -> pd.DataFrame:
    output = df.copy()
    for column in columns:
        if column in output.columns:
            output[column] = pd.to_datetime(output[column], format="%Y%m%d", errors="coerce")
    return output


class ParquetDataStore:
    def __init__(self, root: Path) -> None:
        self.root = root
        self.root.mkdir(parents=True, exist_ok=True)

    def overwrite_table(self, table_name: str, df: pd.DataFrame) -> list[str]:
        _ensure_parquet_engine()
        table_root = self.root / table_name
        table_root.mkdir(parents=True, exist_ok=True)
        output_path = table_root / "data.parquet"
        df.to_parquet(output_path, index=False)
        return [str(output_path.relative_to(self.root))]

    def read_table(self, table_name: str, columns: list[str] | None = None) -> pd.DataFrame:
        table_root = self.root / table_name
        if not table_root.exists():
            raise FileNotFoundError(f"Table '{table_name}' does not exist under {self.root}.")
        return pd.read_parquet(table_root, columns=columns)

    def list_partition_files(self, table_name: str) -> list[Path]:
        table_root = self.root / table_name
        if not table_root.exists():
            raise FileNotFoundError(f"Table '{table_name}' does not exist under {self.root}.")
        return sorted(table_root.glob("year=*/month=*/data.parquet"))

    def clear_table(self, table_name: str) -> None:
        table_root = self.root / table_name
        if table_root.exists():
            shutil.rmtree(table_root)

    def write_partition_file(self, table_name: str, relative_partition_path: Path, df: pd.DataFrame) -> str:
        _ensure_parquet_engine()
        output_path = self.root / table_name / relative_partition_path
        output_path.parent.mkdir(parents=True, exist_ok=True)
        df.to_parquet(output_path, index=False)
        return str(output_path.relative_to(self.root))

    def upsert_by_month(
        self,
        table_name: str,
        df: pd.DataFrame,
        *,
        partition_col: str,
        primary_keys: list[str],
    ) -> list[str]:
        _ensure_parquet_engine()
        if df.empty:
            return []

        table_root = self.root / table_name
        table_root.mkdir(parents=True, exist_ok=True)

        if partition_col not in df.columns:
            raise KeyError(f"Partition column '{partition_col}' is missing from dataframe.")

        working = df.copy()
        working[partition_col] = pd.to_datetime(working[partition_col], errors="coerce")

        updated_paths: list[str] = []
        grouped = working.groupby([working[partition_col].dt.year, working[partition_col].dt.month], dropna=True)
        for (year, month), partition_df in grouped:
            partition_path = table_root / f"year={int(year):04d}" / f"month={int(month):02d}" / "data.parquet"
            partition_path.parent.mkdir(parents=True, exist_ok=True)

            combined = partition_df.copy()
            if partition_path.exists():
                existing = pd.read_parquet(partition_path)
                combined = pd.concat([existing, combined], ignore_index=True)

            combined = combined.drop_duplicates(subset=primary_keys, keep="last")
            combined = combined.sort_values(primary_keys).reset_index(drop=True)
            combined.to_parquet(partition_path, index=False)
            updated_paths.append(str(partition_path.relative_to(self.root)))
        return updated_paths

    def replace_by_month(
        self,
        table_name: str,
        df: pd.DataFrame,
        *,
        partition_col: str,
        primary_keys: list[str],
    ) -> list[str]:
        self.clear_table(table_name)
        return self.upsert_by_month(
            table_name,
            df,
            partition_col=partition_col,
            primary_keys=primary_keys,
        )
