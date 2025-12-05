"""Tests for the Silver Layer."""

import pandas as pd
import pytest
import tempfile
from pathlib import Path

from src.silver.silver_layer import SilverLayer


class TestSilverLayer:
    """Test cases for SilverLayer class."""

    def setup_method(self):
        """Set up test fixtures."""
        self.temp_dir = tempfile.mkdtemp()
        self.silver = SilverLayer(data_path=self.temp_dir)

    def test_transform_removes_bronze_metadata(self):
        """Test that transform removes bronze layer metadata columns."""
        df = pd.DataFrame(
            {
                "id": [1, 2],
                "value": [10, 20],
                "_ingestion_timestamp": ["2023-01-01", "2023-01-02"],
                "_source_system": ["test", "test"],
            }
        )

        result = self.silver.transform(df, "test_table")

        assert "_ingestion_timestamp" not in result.columns
        assert "_source_system" not in result.columns

    def test_transform_adds_silver_metadata(self):
        """Test that transform adds silver layer metadata."""
        df = pd.DataFrame({"id": [1, 2], "value": [10, 20]})

        result = self.silver.transform(df, "test_table")

        assert "_silver_timestamp" in result.columns

    def test_transform_removes_duplicates(self):
        """Test that transform removes duplicate rows."""
        df = pd.DataFrame({"id": [1, 1, 2], "value": [10, 10, 20]})

        result = self.silver.transform(df, "test_table", deduplicate=True)

        assert len(result) == 2

    def test_transform_applies_custom_transformations(self):
        """Test that custom transformations are applied."""
        df = pd.DataFrame({"id": [1, 2], "value": [10, 20]})

        def double_values(df):
            df["value"] = df["value"] * 2
            return df

        result = self.silver.transform(
            df, "test_table", transformations=[double_values]
        )

        assert result["value"].tolist()[:2] == [20, 40]

    def test_clean_column_names(self):
        """Test column name standardization."""
        df = pd.DataFrame({"Column Name": [1], "Another-Column": [2]})

        result = self.silver.clean_column_names(df)

        assert "column_name" in result.columns
        assert "another_column" in result.columns

    def test_standardize_dates(self):
        """Test date standardization."""
        df = pd.DataFrame({"date": ["2023-01-15", "2023-02-20"]})

        result = self.silver.standardize_dates(df, ["date"])

        assert result["date"].iloc[0] == "2023-01-15"
        assert result["date"].iloc[1] == "2023-02-20"

    def test_cast_types(self):
        """Test type casting."""
        df = pd.DataFrame({"id": ["1", "2"], "value": ["10.5", "20.5"]})

        result = self.silver.cast_types(df, {"id": "int", "value": "float"})

        assert pd.api.types.is_integer_dtype(result["id"])
        assert pd.api.types.is_float_dtype(result["value"])

    def test_drop_nulls(self):
        """Test null value handling."""
        df = pd.DataFrame({"id": [1, 2, 3], "value": [10, None, 30]})

        result = self.silver.transform(df, "test_table", drop_nulls=True)

        assert len(result) == 2

    def test_read_and_list_tables(self):
        """Test reading and listing tables."""
        df = pd.DataFrame({"id": [1]})
        self.silver.transform(df, "table_a")
        self.silver.transform(df, "table_b")

        tables = self.silver.list_tables()
        assert "table_a" in tables
        assert "table_b" in tables

        read_df = self.silver.read_table("table_a")
        assert len(read_df) == 1
