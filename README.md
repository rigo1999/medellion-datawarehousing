# Data Warehousing Project with Medallion Architecture

## Overview

This project implements a data warehouse using the Medallion Architecture (Bronze, Silver, Gold layers) with SQL Server. The architecture organizes data into layers that progressively refine raw data into business-ready insights, supporting analytical and reporting use cases.

The project integrates data from two primary sources:
- **CRM (Customer Relationship Management)**: Customer information, product details, and sales transactions.
- **ERP (Enterprise Resource Planning)**: Additional customer demographics, location data, and product categories.

## Architecture

The Medallion Architecture consists of three layers:

### Bronze Layer
- **Purpose**: Raw data ingestion and storage.
- **Characteristics**: Data is stored as-is from source systems, with minimal transformation. Tables retain original naming and structure.
- **Tables**:
  - `bronze.crm_cust_info`: Customer information from CRM.
  - `bronze.crm_prd_info`: Product information from CRM.
  - `bronze.crm_sales_details`: Sales details from CRM.
  - `bronze.erp_loc_a101`: Location data from ERP.
  - `bronze.erp_cust_az12`: Additional customer data from ERP.
  - `bronze.erp_px_cat_g1v2`: Product category data from ERP.

### Silver Layer
- **Purpose**: Data cleansing, standardization, and integration.
- **Characteristics**: Data is cleaned, deduplicated, and standardized. Relationships between tables are established.
- **Tables**: Follow similar naming to Bronze but with quality improvements.

### Gold Layer
- **Purpose**: Business-ready data for analytics and reporting.
- **Characteristics**: Structured as star schema with dimension and fact tables.
- **Tables**:
  - `gold.dim_customers`: Dimension table for customer details.
  - `gold.dim_products`: Dimension table for product information.
  - `gold.fact_sales`: Fact table for sales transactions.

## Folder Structure

```
sql-data-warehouse-project/
├── datasets/
│   ├── source_crm/
│   │   ├── cust_info.csv
│   │   ├── prd_info.csv
│   │   └── sales_details.csv
│   └── source_erp/
│       ├── CUST_AZ12.csv
│       ├── LOC_A101.csv
│       └── PX_CAT_G1V2.csv
├── docs/
│   ├── data_architecture.drawio
│   ├── data_catalog.md
│   ├── data_flow.drawio
│   ├── data_integration.drawio
│   ├── data_model.drawio
│   ├── ETL.drawio
│   ├── naming_conventions.md
│   └── ...
├── scripts/
│   ├── init_database.sql
│   ├── bronze/
│   │   ├── ddl_bronze.sql
│   │   └── proc_load_bronze.sql
│   ├── silver/
│   │   ├── ddl_silver.sql
│   │   └── proc_load_silver.sql
│   └── gold/
│       └── ddl_gold.sql
└── tests/
    ├── quality_checks_gold.sql
    └── quality_checks_silver.sql
```

## Setup Instructions

1. **Prerequisites**:
   - SQL Server instance (e.g., SQL Server Management Studio).
   - Access to the source CSV files in `datasets/`.

2. **Initialize Database**:
   - Run `scripts/init_database.sql` to create the Bronze layer tables and schemas.

3. **Load Data**:
   - Execute `scripts/bronze/proc_load_bronze.sql` to load raw data into Bronze tables.
   - Run `scripts/silver/proc_load_silver.sql` to transform and load data into Silver layer.
   - Execute DDL scripts in `gold/` to create Gold layer structures.

4. **Quality Checks**:
   - Run `tests/quality_checks_silver.sql` and `tests/quality_checks_gold.sql` to validate data integrity.

## Naming Conventions

- **Bronze/Silver**: `<sourcesystem>_<entity>` (e.g., `crm_cust_info`).
- **Gold**: `<category>_<entity>` (e.g., `dim_customers`, `fact_sales`).
- **Columns**: Snake_case, surrogate keys end with `_key`, technical columns prefixed with `dwh_`.
- **Stored Procedures**: `load_<layer>` (e.g., `load_bronze`).

For detailed conventions, refer to `docs/naming_conventions.md`.

## Data Catalog

The Gold layer data catalog is documented in `docs/data_catalog.md`, including table purposes, columns, and descriptions.

## Usage

- Query the Gold layer for business analytics.
- Use dimensions (`dim_*`) to slice and dice data.
- Aggregate facts (`fact_*`) for metrics like sales amounts and quantities.
- Integrate with BI tools (e.g., Power BI, Tableau) for visualizations.

## Documentation

- `docs/data_catalog.md`: Detailed Gold layer schema.
- `docs/naming_conventions.md`: Naming standards.
- `docs/*.drawio`: Diagrams for architecture, data flow, ETL processes, etc.

## Contributing

- Follow the naming conventions.
- Update documentation for any schema changes.
- Run quality checks before committing changes.

## License

[Specify license if applicable]
