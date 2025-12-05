"""Bronze Layer - Raw data ingestion layer.

The Bronze layer is the landing zone for raw data. Data is ingested
as-is from source systems with minimal transformations. This layer
preserves the original data format and adds metadata like ingestion
timestamp.
"""

from .bronze_layer import BronzeLayer

__all__ = ["BronzeLayer"]
