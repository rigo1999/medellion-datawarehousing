#!/usr/bin/env python
"""Example ETL pipeline demonstrating medallion architecture.

This script demonstrates how to use the medallion architecture
to process sales data through Bronze, Silver, and Gold layers.
"""

import pandas as pd
from pathlib import Path
import sys

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.bronze import BronzeLayer
from src.silver import SilverLayer
from src.gold import GoldLayer


def create_sample_data() -> tuple[pd.DataFrame, pd.DataFrame]:
    """Create sample sales and product data for demonstration."""
    # Sample sales transactions
    sales_data = pd.DataFrame(
        {
            "transaction_id": [1, 2, 3, 4, 5, 6, 7, 8],
            "product_id": [101, 102, 101, 103, 102, 101, 103, 102],
            "quantity": [2, 1, 3, 1, 2, 1, 4, 2],
            "unit_price": [29.99, 49.99, 29.99, 99.99, 49.99, 29.99, 99.99, 49.99],
            "date": [
                "2024-01-15",
                "2024-01-15",
                "2024-01-16",
                "2024-01-16",
                "2024-01-17",
                "2024-01-17",
                "2024-01-18",
                "2024-01-18",
            ],
            "customer_id": [1001, 1002, 1001, 1003, 1002, 1004, 1001, 1003],
        }
    )

    # Sample product master data
    products_data = pd.DataFrame(
        {
            "product_id": [101, 102, 103],
            "product_name": ["Widget A", "Widget B", "Premium Widget"],
            "category": ["Basic", "Standard", "Premium"],
            "supplier": ["Supplier X", "Supplier Y", "Supplier Z"],
        }
    )

    return sales_data, products_data


def run_pipeline():
    """Run the complete ETL pipeline."""
    print("=" * 60)
    print("Medallion Architecture ETL Pipeline Demo")
    print("=" * 60)

    # Initialize layers
    bronze = BronzeLayer(data_path="data/raw")
    silver = SilverLayer(data_path="data/processed")
    gold = GoldLayer(data_path="data/aggregated")

    # Create sample data
    sales_df, products_df = create_sample_data()

    # ==========================================================================
    # BRONZE LAYER - Raw Data Ingestion
    # ==========================================================================
    print("\n--- BRONZE LAYER: Ingesting raw data ---")

    bronze_sales = bronze.ingest_dataframe(
        sales_df, "sales_transactions", source_system="pos_system"
    )
    print(f"Ingested {len(bronze_sales)} sales transactions")

    bronze_products = bronze.ingest_dataframe(
        products_df, "products", source_system="master_data"
    )
    print(f"Ingested {len(bronze_products)} products")

    print("\nBronze layer tables:", bronze.list_tables())

    # ==========================================================================
    # SILVER LAYER - Data Cleaning and Transformation
    # ==========================================================================
    print("\n--- SILVER LAYER: Cleaning and transforming data ---")

    # Transform sales data
    def calculate_total(df):
        df["total_amount"] = df["quantity"] * df["unit_price"]
        return df

    silver_sales = silver.transform(
        bronze_sales,
        "sales_clean",
        transformations=[silver.clean_column_names, calculate_total],
        deduplicate=True,
    )
    print(f"Transformed sales data: {len(silver_sales)} records")

    # Transform products data
    silver_products = silver.transform(
        bronze_products,
        "products_clean",
        transformations=[silver.clean_column_names],
    )
    print(f"Transformed products data: {len(silver_products)} records")

    print("\nSilver layer tables:", silver.list_tables())

    # ==========================================================================
    # GOLD LAYER - Business Aggregations
    # ==========================================================================
    print("\n--- GOLD LAYER: Creating business aggregations ---")

    # Create dimension table for products
    dim_products = gold.create_dimension(
        silver_products,
        "product",
        key_column="product_id",
        attributes=["product_name", "category", "supplier"],
    )
    print(f"Created product dimension: {len(dim_products)} records")

    # Create fact table for sales
    fact_sales = gold.create_fact(
        silver_sales,
        "sales",
        dimension_keys=["product_id", "customer_id", "date"],
        measures=["quantity", "unit_price", "total_amount"],
    )
    print(f"Created sales fact table: {len(fact_sales)} records")

    # Create sales summary by product
    sales_by_product = gold.aggregate(
        silver_sales,
        "sales_by_product",
        group_by=["product_id"],
        aggregations={
            "quantity": "sum",
            "total_amount": "sum",
            "transaction_id": "count",
        },
    )
    # Rename for clarity
    sales_by_product = sales_by_product.rename(
        columns={"transaction_id": "transaction_count"}
    )
    print(f"Created sales by product summary: {len(sales_by_product)} records")

    # Create daily sales summary
    daily_sales = gold.aggregate(
        silver_sales,
        "daily_sales",
        group_by=["date"],
        aggregations={
            "quantity": "sum",
            "total_amount": "sum",
            "transaction_id": "count",
        },
    )
    print(f"Created daily sales summary: {len(daily_sales)} records")

    print("\nGold layer tables:", gold.list_tables())

    # ==========================================================================
    # Display Results
    # ==========================================================================
    print("\n" + "=" * 60)
    print("PIPELINE RESULTS")
    print("=" * 60)

    print("\n📊 Sales by Product:")
    print(sales_by_product.to_string(index=False))

    print("\n📅 Daily Sales:")
    print(daily_sales.to_string(index=False))

    print("\n📦 Product Dimension:")
    print(dim_products.to_string(index=False))

    print("\n✅ Pipeline completed successfully!")
    print(f"Data stored in: data/raw, data/processed, data/aggregated")


if __name__ == "__main__":
    run_pipeline()
