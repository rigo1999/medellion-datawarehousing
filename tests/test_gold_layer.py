"""Tests for the Gold Layer."""

import pandas as pd
import pytest
import tempfile
from pathlib import Path

from src.gold.gold_layer import GoldLayer


class TestGoldLayer:
    """Test cases for GoldLayer class."""

    def setup_method(self):
        """Set up test fixtures."""
        self.temp_dir = tempfile.mkdtemp()
        self.gold = GoldLayer(data_path=self.temp_dir)

    def test_aggregate_groups_data(self):
        """Test that aggregate groups data correctly."""
        df = pd.DataFrame(
            {
                "category": ["A", "A", "B", "B"],
                "value": [10, 20, 30, 40],
            }
        )

        result = self.gold.aggregate(
            df,
            "agg_test",
            group_by=["category"],
            aggregations={"value": "sum"},
        )

        assert len(result) == 2
        a_sum = result[result["category"] == "A"]["value"].iloc[0]
        b_sum = result[result["category"] == "B"]["value"].iloc[0]
        assert a_sum == 30
        assert b_sum == 70

    def test_aggregate_adds_gold_metadata(self):
        """Test that aggregate adds gold layer metadata."""
        df = pd.DataFrame({"category": ["A"], "value": [10]})

        result = self.gold.aggregate(
            df, "meta_test", group_by=["category"], aggregations={"value": "sum"}
        )

        assert "_gold_timestamp" in result.columns

    def test_create_dimension(self):
        """Test dimension table creation."""
        df = pd.DataFrame(
            {
                "product_id": [1, 1, 2, 2],
                "product_name": ["Widget", "Widget", "Gadget", "Gadget"],
                "category": ["Electronics", "Electronics", "Home", "Home"],
            }
        )

        result = self.gold.create_dimension(
            df, "product", key_column="product_id", attributes=["product_name", "category"]
        )

        assert len(result) == 2
        assert "dim_product.csv" in [f.name for f in Path(self.temp_dir).glob("*.csv")]

    def test_create_fact(self):
        """Test fact table creation."""
        df = pd.DataFrame(
            {
                "date_id": [1, 2, 3],
                "product_id": [101, 102, 101],
                "quantity": [5, 10, 3],
                "revenue": [50.0, 100.0, 30.0],
            }
        )

        result = self.gold.create_fact(
            df,
            "sales",
            dimension_keys=["date_id", "product_id"],
            measures=["quantity", "revenue"],
        )

        assert len(result) == 3
        assert "fact_sales.csv" in [f.name for f in Path(self.temp_dir).glob("*.csv")]

    def test_join_tables(self):
        """Test joining two tables."""
        left = pd.DataFrame({"id": [1, 2, 3], "name": ["A", "B", "C"]})
        right = pd.DataFrame({"id": [1, 2], "value": [100, 200]})

        result = self.gold.join_tables(left, right, "joined", on="id", how="inner")

        assert len(result) == 2
        assert "name" in result.columns
        assert "value" in result.columns

    def test_calculate_metrics(self):
        """Test metric calculation."""
        df = pd.DataFrame(
            {
                "quantity": [10, 20, 30],
                "price": [5.0, 10.0, 15.0],
            }
        )

        result = self.gold.calculate_metrics(
            df,
            "with_metrics",
            metrics={
                "revenue": lambda x: x["quantity"] * x["price"],
            },
        )

        assert "revenue" in result.columns
        assert result["revenue"].tolist() == [50.0, 200.0, 450.0]

    def test_read_and_list_tables(self):
        """Test reading and listing tables."""
        df = pd.DataFrame({"category": ["A"], "value": [10]})
        self.gold.aggregate(df, "report_a", group_by=["category"], aggregations={"value": "sum"})

        tables = self.gold.list_tables()
        assert "report_a" in tables

        read_df = self.gold.read_table("report_a")
        assert len(read_df) == 1
