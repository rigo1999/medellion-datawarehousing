"""Tests for the Bronze Layer."""

import pandas as pd
import pytest
import tempfile
import os
from pathlib import Path

from src.bronze.bronze_layer import BronzeLayer


class TestBronzeLayer:
    """Test cases for BronzeLayer class."""

    def setup_method(self):
        """Set up test fixtures."""
        self.temp_dir = tempfile.mkdtemp()
        self.bronze = BronzeLayer(data_path=self.temp_dir)

    def test_init_creates_data_path(self):
        """Test that initialization creates the data path directory."""
        temp_path = tempfile.mkdtemp()
        new_path = os.path.join(temp_path, "new_bronze")
        bronze = BronzeLayer(data_path=new_path)
        assert Path(new_path).exists()

    def test_ingest_dataframe_adds_metadata(self):
        """Test that ingesting a DataFrame adds metadata columns."""
        df = pd.DataFrame({"id": [1, 2, 3], "name": ["A", "B", "C"]})

        result = self.bronze.ingest_dataframe(df, "test_table", source_system="test")

        assert "_ingestion_timestamp" in result.columns
        assert "_source_system" in result.columns
        assert result["_source_system"].iloc[0] == "test"

    def test_ingest_dataframe_saves_to_file(self):
        """Test that ingesting a DataFrame saves it to a file."""
        df = pd.DataFrame({"id": [1, 2, 3], "value": [10.5, 20.5, 30.5]})

        self.bronze.ingest_dataframe(df, "saved_table")

        file_path = Path(self.temp_dir) / "saved_table.csv"
        assert file_path.exists()

    def test_read_table_returns_data(self):
        """Test that read_table returns the stored data."""
        df = pd.DataFrame({"id": [1, 2], "name": ["X", "Y"]})
        self.bronze.ingest_dataframe(df, "read_test")

        result = self.bronze.read_table("read_test")

        assert len(result) == 2
        assert "id" in result.columns
        assert "name" in result.columns

    def test_list_tables_returns_table_names(self):
        """Test that list_tables returns all table names."""
        df = pd.DataFrame({"id": [1]})
        self.bronze.ingest_dataframe(df, "table1")
        self.bronze.ingest_dataframe(df, "table2")

        tables = self.bronze.list_tables()

        assert "table1" in tables
        assert "table2" in tables

    def test_ingest_data_from_file(self):
        """Test ingesting data from a CSV file."""
        # Create a temp CSV file
        source_file = os.path.join(self.temp_dir, "source.csv")
        df = pd.DataFrame({"col1": ["a", "b"], "col2": [1, 2]})
        df.to_csv(source_file, index=False)

        result = self.bronze.ingest_data(
            source_file, "from_file", source_system="file_system"
        )

        assert len(result) == 2
        assert "_source_system" in result.columns
        assert result["_source_system"].iloc[0] == "file_system"
