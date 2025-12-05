"""Gold Layer implementation for business aggregations and analytics."""

import pandas as pd
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable, Optional

from ..utils.data_utils import load_csv, save_csv


class GoldLayer:
    """Gold Layer for business-level aggregations and analytics-ready data.

    The Gold layer contains curated, business-level data that is ready
    for analytics and reporting. Data is aggregated, joined, and enriched
    to provide meaningful business insights.
    """

    def __init__(self, data_path: str = "data/aggregated"):
        """Initialize the Gold Layer.

        Args:
            data_path: Base path for storing gold layer data.
        """
        self.data_path = Path(data_path)
        self.data_path.mkdir(parents=True, exist_ok=True)

    def aggregate(
        self,
        df: pd.DataFrame,
        table_name: str,
        group_by: list[str],
        aggregations: dict[str, str | list[str]],
    ) -> pd.DataFrame:
        """Aggregate data for business reporting.

        Args:
            df: Source DataFrame from silver layer.
            table_name: Name for the output table.
            group_by: Columns to group by.
            aggregations: Dictionary mapping columns to aggregation functions.

        Returns:
            Aggregated DataFrame.
        """
        result = df.copy()

        # Remove silver metadata columns
        silver_meta_cols = ["_silver_timestamp"]
        result = result.drop(
            columns=[c for c in silver_meta_cols if c in result.columns], errors="ignore"
        )

        # Perform aggregation
        agg_result = result.groupby(group_by).agg(aggregations).reset_index()

        # Flatten multi-level column names if necessary
        if isinstance(agg_result.columns, pd.MultiIndex):
            agg_result.columns = [
                "_".join(col).strip("_") if col[1] else col[0]
                for col in agg_result.columns.values
            ]

        # Add gold layer metadata
        agg_result = self._add_metadata(agg_result)

        # Save to gold layer
        output_path = self.data_path / f"{table_name}.csv"
        save_csv(agg_result, str(output_path))

        return agg_result

    def create_dimension(
        self,
        df: pd.DataFrame,
        table_name: str,
        key_column: str,
        attributes: list[str],
    ) -> pd.DataFrame:
        """Create a dimension table for star schema.

        Args:
            df: Source DataFrame.
            table_name: Name for the dimension table.
            key_column: Column to use as the dimension key.
            attributes: Columns to include as dimension attributes.

        Returns:
            Dimension table DataFrame.
        """
        columns = [key_column] + [c for c in attributes if c != key_column]
        columns = [c for c in columns if c in df.columns]

        result = df[columns].drop_duplicates().reset_index(drop=True)
        result = self._add_metadata(result)

        # Save to gold layer
        output_path = self.data_path / f"dim_{table_name}.csv"
        save_csv(result, str(output_path))

        return result

    def create_fact(
        self,
        df: pd.DataFrame,
        table_name: str,
        dimension_keys: list[str],
        measures: list[str],
    ) -> pd.DataFrame:
        """Create a fact table for star schema.

        Args:
            df: Source DataFrame.
            table_name: Name for the fact table.
            dimension_keys: Columns that are foreign keys to dimensions.
            measures: Columns containing measurable values.

        Returns:
            Fact table DataFrame.
        """
        # Remove silver metadata columns
        silver_meta_cols = ["_silver_timestamp"]
        result = df.drop(
            columns=[c for c in silver_meta_cols if c in df.columns], errors="ignore"
        )

        columns = dimension_keys + measures
        columns = [c for c in columns if c in result.columns]

        result = result[columns].copy()
        result = self._add_metadata(result)

        # Save to gold layer
        output_path = self.data_path / f"fact_{table_name}.csv"
        save_csv(result, str(output_path))

        return result

    def join_tables(
        self,
        left_df: pd.DataFrame,
        right_df: pd.DataFrame,
        table_name: str,
        on: str | list[str],
        how: str = "inner",
    ) -> pd.DataFrame:
        """Join two tables and save the result.

        Args:
            left_df: Left DataFrame.
            right_df: Right DataFrame.
            table_name: Name for the output table.
            on: Column(s) to join on.
            how: Join type (inner, left, right, outer).

        Returns:
            Joined DataFrame.
        """
        result = pd.merge(left_df, right_df, on=on, how=how)
        result = self._add_metadata(result)

        # Save to gold layer
        output_path = self.data_path / f"{table_name}.csv"
        save_csv(result, str(output_path))

        return result

    def calculate_metrics(
        self,
        df: pd.DataFrame,
        table_name: str,
        metrics: dict[str, Callable[[pd.DataFrame], pd.Series]],
    ) -> pd.DataFrame:
        """Calculate derived metrics and KPIs.

        Args:
            df: Source DataFrame.
            table_name: Name for the output table.
            metrics: Dictionary mapping metric names to calculation functions.

        Returns:
            DataFrame with calculated metrics.
        """
        result = df.copy()

        for metric_name, calc_fn in metrics.items():
            result[metric_name] = calc_fn(result)

        result = self._add_metadata(result)

        # Save to gold layer
        output_path = self.data_path / f"{table_name}.csv"
        save_csv(result, str(output_path))

        return result

    def _add_metadata(self, df: pd.DataFrame) -> pd.DataFrame:
        """Add gold layer metadata.

        Args:
            df: DataFrame to add metadata to.

        Returns:
            DataFrame with metadata columns.
        """
        df["_gold_timestamp"] = datetime.now(timezone.utc).isoformat()
        return df

    def read_table(self, table_name: str) -> pd.DataFrame:
        """Read a table from the gold layer.

        Args:
            table_name: Name of the table to read.

        Returns:
            DataFrame containing the table data.
        """
        file_path = self.data_path / f"{table_name}.csv"
        return load_csv(str(file_path))

    def list_tables(self) -> list[str]:
        """List all tables in the gold layer.

        Returns:
            List of table names.
        """
        return [f.stem for f in self.data_path.glob("*.csv")]
