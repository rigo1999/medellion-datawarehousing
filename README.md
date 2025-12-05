# Medallion Data Warehouse

A Python implementation of the Medallion Architecture for data warehousing. This project provides a structured approach to organizing data through three distinct layers: Bronze, Silver, and Gold.

## 🏗️ Architecture Overview

The Medallion Architecture is a data design pattern used to logically organize data in a lakehouse/data warehouse:

```
┌─────────────┐    ┌─────────────┐    ┌─────────────┐
│   Bronze    │ -> │   Silver    │ -> │    Gold     │
│   (Raw)     │    │  (Cleaned)  │    │(Aggregated) │
└─────────────┘    └─────────────┘    └─────────────┘
```

### Bronze Layer (Raw Data)
- Landing zone for raw data from source systems
- Data is stored as-is with minimal transformations
- Adds metadata like ingestion timestamp and source system

### Silver Layer (Cleaned Data)
- Cleaned, validated, and transformed data
- Data quality rules are applied
- Duplicates are removed and data types are standardized

### Gold Layer (Business Data)
- Business-level aggregations and analytics-ready data
- Fact and dimension tables for star schema
- KPIs and metrics for reporting

## 📁 Project Structure

```
medellion-datawarehousing/
├── src/
│   ├── bronze/         # Bronze layer implementation
│   ├── silver/         # Silver layer implementation
│   ├── gold/           # Gold layer implementation
│   └── utils/          # Utility functions
├── data/
│   ├── raw/            # Bronze layer data storage
│   ├── processed/      # Silver layer data storage
│   └── aggregated/     # Gold layer data storage
├── scripts/
│   └── example_pipeline.py  # Example ETL pipeline
├── tests/              # Unit tests
├── requirements.txt    # Python dependencies
└── README.md
```

## 🚀 Getting Started

### Prerequisites
- Python 3.10+
- pip

### Installation

1. Clone the repository:
```bash
git clone https://github.com/rigo1999/medellion-datawarehousing.git
cd medellion-datawarehousing
```

2. Install dependencies:
```bash
pip install -r requirements.txt
```

### Running the Example Pipeline

Run the example pipeline to see the medallion architecture in action:

```bash
python scripts/example_pipeline.py
```

This will:
1. Generate sample sales and product data
2. Ingest data into the Bronze layer
3. Clean and transform data in the Silver layer
4. Create aggregations and dimension tables in the Gold layer

## 💻 Usage

### Bronze Layer

```python
from src.bronze import BronzeLayer

# Initialize the bronze layer
bronze = BronzeLayer(data_path="data/raw")

# Ingest data from a DataFrame
bronze_data = bronze.ingest_dataframe(
    df=my_dataframe,
    table_name="sales",
    source_system="pos_system"
)

# Or ingest from a CSV file
bronze_data = bronze.ingest_data(
    source_path="path/to/file.csv",
    table_name="sales",
    source_system="file_system"
)
```

### Silver Layer

```python
from src.silver import SilverLayer

# Initialize the silver layer
silver = SilverLayer(data_path="data/processed")

# Transform bronze data
silver_data = silver.transform(
    df=bronze_data,
    table_name="sales_clean",
    transformations=[silver.clean_column_names],
    deduplicate=True,
    drop_nulls=True
)

# Standardize dates
silver_data = silver.standardize_dates(silver_data, ["order_date"])

# Cast types
silver_data = silver.cast_types(silver_data, {"quantity": "int", "price": "float"})
```

### Gold Layer

```python
from src.gold import GoldLayer

# Initialize the gold layer
gold = GoldLayer(data_path="data/aggregated")

# Create aggregations
sales_summary = gold.aggregate(
    df=silver_data,
    table_name="sales_summary",
    group_by=["product_id"],
    aggregations={"quantity": "sum", "revenue": "sum"}
)

# Create dimension table
dim_product = gold.create_dimension(
    df=silver_products,
    table_name="product",
    key_column="product_id",
    attributes=["product_name", "category"]
)

# Create fact table
fact_sales = gold.create_fact(
    df=silver_data,
    table_name="sales",
    dimension_keys=["product_id", "customer_id", "date_id"],
    measures=["quantity", "revenue"]
)
```

## 🧪 Testing

Run the test suite:

```bash
pytest
```

Run with coverage:

```bash
pytest --cov=src
```

## 📝 License

This project is open source and available under the MIT License.