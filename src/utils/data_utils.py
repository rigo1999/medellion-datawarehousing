"""Utility functions for data operations."""

import pandas as pd
from pathlib import Path
from typing import Optional


def load_csv(file_path: str) -> pd.DataFrame:
    """Load data from a CSV file.

    Args:
        file_path: Path to the CSV file.

    Returns:
        DataFrame containing the loaded data.
    """
    path = Path(file_path)
    if not path.exists():
        raise FileNotFoundError(f"File not found: {file_path}")
    return pd.read_csv(file_path)


def save_csv(df: pd.DataFrame, file_path: str, index: bool = False) -> None:
    """Save DataFrame to a CSV file.

    Args:
        df: DataFrame to save.
        file_path: Path to save the CSV file.
        index: Whether to include the index in the output.
    """
    path = Path(file_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(file_path, index=index)


def validate_schema(
    df: pd.DataFrame, required_columns: list[str], column_types: Optional[dict] = None
) -> bool:
    """Validate DataFrame schema.

    Args:
        df: DataFrame to validate.
        required_columns: List of required column names.
        column_types: Optional dictionary mapping column names to expected dtypes.

    Returns:
        True if schema is valid.

    Raises:
        ValueError: If schema validation fails.
    """
    missing_columns = set(required_columns) - set(df.columns)
    if missing_columns:
        raise ValueError(f"Missing required columns: {missing_columns}")

    if column_types:
        for col, expected_type in column_types.items():
            if col in df.columns and not pd.api.types.is_dtype_equal(
                df[col].dtype, expected_type
            ):
                # Allow numeric type flexibility
                if expected_type in ["float64", "int64"] and pd.api.types.is_numeric_dtype(
                    df[col]
                ):
                    continue
                raise ValueError(
                    f"Column '{col}' has type {df[col].dtype}, expected {expected_type}"
                )

    return True
