"""
Feature engineering processors.

Owner: Data Engineering Team
Create new features from existing data.
"""

import pandas as pd

from dmviz.core.registry import Registry
from dmviz.processing.base import BaseProcessor


@Registry.register("feature_engineer", category="processing")
class FeatureEngineer(BaseProcessor):
    """
    Create derived features.
    
    Config: operations (list of feature definitions)
    INPUT:  pd.DataFrame
    OUTPUT: pd.DataFrame (with new columns)
    
    Example operations:
        - {"name": "age_squared", "expr": "age ** 2"}
        - {"name": "income_ratio", "expr": "income / household_size"}
    """
    
    name = "feature_engineer"
    
    def _run(self, df: pd.DataFrame) -> pd.DataFrame:
        """Apply feature engineering."""
        raise NotImplementedError("TODO: Implement feature engineering")


@Registry.register("column_selector", category="processing")
class ColumnSelector(BaseProcessor):
    """
    Select/drop columns.
    
    Config: columns (list), mode ("keep" | "drop")
    INPUT:  pd.DataFrame
    OUTPUT: pd.DataFrame (filtered columns)
    """
    
    name = "column_selector"
    
    def _run(self, df: pd.DataFrame) -> pd.DataFrame:
        """Select or drop columns."""
        raise NotImplementedError("TODO: Implement column selection")
