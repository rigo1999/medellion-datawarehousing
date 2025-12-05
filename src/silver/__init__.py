"""Silver Layer - Cleaned and transformed data layer.

The Silver layer contains cleaned, validated, and transformed data.
Data quality rules are applied, duplicates are removed, and data
types are standardized. This layer prepares data for business use.
"""

from .silver_layer import SilverLayer

__all__ = ["SilverLayer"]
