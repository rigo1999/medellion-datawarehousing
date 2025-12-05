"""Silver Layer implementation for data cleaning and transformation."""

import pandas as pd
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable, Optional

from ..utils.data_utils import load_csv, save_csv


class SilverLayer:
    """Silver Layer for cleaned and transformed data.

    The Silver layer contains data that has been cleaned, validated,
    and transformed. Data quality rules are applied, duplicates are
    removed, and data types are standardized.
    """

    def __init__(self, data_path: str = "data/processed"):
        """Initialize the Silver Layer.

        Args:
            data_path: Base path for storing silver layer data.
        """
        self.data_path = Path(data_path)
        self.data_path.mkdir(parents=True, exist_ok=True)

    def transform(
        self,
        df: pd.DataFrame,
        table_name: str,
        transformations: Optional[list[Callable[[pd.DataFrame], pd.DataFrame]]] = None,
        deduplicate: bool = True,
        drop_nulls: bool = False,
        null_columns: Optional[list[str]] = None,
    ) -> pd.DataFrame:
        """Transform bronze data to silver layer format.

        Args:
            df: Source DataFrame from bronze layer.
            table_name: Name for the output table.
            transformations: List of transformation functions to apply.
            deduplicate: Whether to remove duplicate rows.
            drop_nulls: Whether to drop rows with null values.
            null_columns: Specific columns to check for null values.

        Returns:
            Transformed DataFrame.
        """
        result = df.copy()

        # Remove bronze metadata columns for clean silver data
        bronze_meta_cols = ["_ingestion_timestamp", "_source_system"]
        result = result.drop(
            columns=[c for c in bronze_meta_cols if c in result.columns], errors="ignore"
        )

        # Apply custom transformations
        if transformations:
            for transform_fn in transformations:
                result = transform_fn(result)

        # Remove duplicates
        if deduplicate:
            result = result.drop_duplicates()

        # Handle null values
        if drop_nulls:
            if null_columns:
                result = result.dropna(subset=null_columns)
            else:
                result = result.dropna()

        # Add silver layer metadata
        result = self._add_metadata(result)

        # Save to silver layer
        output_path = self.data_path / f"{table_name}.csv"
        save_csv(result, str(output_path))

        return result

    def clean_column_names(self, df: pd.DataFrame) -> pd.DataFrame:
        """Standardize column names.

        Args:
            df: DataFrame to clean.

        Returns:
            DataFrame with standardized column names.
        """
        result = df.copy()
        result.columns = [
            col.lower().strip().replace(" ", "_").replace("-", "_")
            for col in result.columns
        ]
        return result

    def standardize_dates(
        self, df: pd.DataFrame, date_columns: list[str], format: str = "%Y-%m-%d"
    ) -> pd.DataFrame:
        """Standardize date columns to a consistent format.

        Args:
            df: DataFrame to process.
            date_columns: List of column names containing dates.
            format: Target date format.

        Returns:
            DataFrame with standardized date columns.
        """
        result = df.copy()
        for col in date_columns:
            if col in result.columns:
                result[col] = pd.to_datetime(result[col], errors="coerce")
                result[col] = result[col].dt.strftime(format)
        return result

    def cast_types(
        self, df: pd.DataFrame, type_mapping: dict[str, str]
    ) -> pd.DataFrame:
        """Cast columns to specified data types.

        Args:
            df: DataFrame to process.
            type_mapping: Dictionary mapping column names to target types.

        Returns:
            DataFrame with casted column types.
        """
        result = df.copy()
        for col, dtype in type_mapping.items():
            if col in result.columns:
                if dtype == "int":
                    result[col] = pd.to_numeric(result[col], errors="coerce").astype(
                        "Int64"
                    )
                elif dtype == "float":
                    result[col] = pd.to_numeric(result[col], errors="coerce")
                elif dtype == "str":
                    result[col] = result[col].astype(str)
                elif dtype == "datetime":
                    result[col] = pd.to_datetime(result[col], errors="coerce")
        return result

    def _add_metadata(self, df: pd.DataFrame) -> pd.DataFrame:
        """Add silver layer metadata.

        Args:
            df: DataFrame to add metadata to.

        Returns:
            DataFrame with metadata columns.
        """
        df["_silver_timestamp"] = datetime.now(timezone.utc).isoformat()
        return df

    def read_table(self, table_name: str) -> pd.DataFrame:
        """Read a table from the silver layer.

        Args:
            table_name: Name of the table to read.

        Returns:
            DataFrame containing the table data.
        """
        file_path = self.data_path / f"{table_name}.csv"
        return load_csv(str(file_path))

    def list_tables(self) -> list[str]:
        """List all tables in the silver layer.

        Returns:
            List of table names.
        """
        return [f.stem for f in self.data_path.glob("*.csv")]
