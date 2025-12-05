"""Bronze Layer implementation for raw data ingestion."""

import pandas as pd
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from ..utils.data_utils import load_csv, save_csv


class BronzeLayer:
    """Bronze Layer for raw data ingestion and storage.

    The Bronze layer is the landing zone for raw data from source systems.
    Data is stored as-is with minimal transformations, preserving the
    original format while adding metadata for tracking.
    """

    def __init__(self, data_path: str = "data/raw"):
        """Initialize the Bronze Layer.

        Args:
            data_path: Base path for storing bronze layer data.
        """
        self.data_path = Path(data_path)
        self.data_path.mkdir(parents=True, exist_ok=True)

    def ingest_data(
        self,
        source_path: str,
        table_name: str,
        source_system: str = "unknown",
    ) -> pd.DataFrame:
        """Ingest raw data from a source file.

        Args:
            source_path: Path to the source data file.
            table_name: Name of the table/dataset.
            source_system: Identifier for the source system.

        Returns:
            DataFrame with ingested data and metadata columns.
        """
        # Load raw data
        df = load_csv(source_path)

        # Add metadata columns
        df = self._add_metadata(df, source_system)

        # Save to bronze layer
        output_path = self.data_path / f"{table_name}.csv"
        save_csv(df, str(output_path))

        return df

    def ingest_dataframe(
        self,
        df: pd.DataFrame,
        table_name: str,
        source_system: str = "unknown",
    ) -> pd.DataFrame:
        """Ingest data from a DataFrame.

        Args:
            df: Source DataFrame to ingest.
            table_name: Name of the table/dataset.
            source_system: Identifier for the source system.

        Returns:
            DataFrame with ingested data and metadata columns.
        """
        # Add metadata columns
        df_with_metadata = self._add_metadata(df.copy(), source_system)

        # Save to bronze layer
        output_path = self.data_path / f"{table_name}.csv"
        save_csv(df_with_metadata, str(output_path))

        return df_with_metadata

    def _add_metadata(self, df: pd.DataFrame, source_system: str) -> pd.DataFrame:
        """Add metadata columns to the DataFrame.

        Args:
            df: DataFrame to add metadata to.
            source_system: Identifier for the source system.

        Returns:
            DataFrame with metadata columns added.
        """
        df["_ingestion_timestamp"] = datetime.now(timezone.utc).isoformat()
        df["_source_system"] = source_system
        return df

    def read_table(self, table_name: str) -> pd.DataFrame:
        """Read a table from the bronze layer.

        Args:
            table_name: Name of the table to read.

        Returns:
            DataFrame containing the table data.
        """
        file_path = self.data_path / f"{table_name}.csv"
        return load_csv(str(file_path))

    def list_tables(self) -> list[str]:
        """List all tables in the bronze layer.

        Returns:
            List of table names.
        """
        return [f.stem for f in self.data_path.glob("*.csv")]
