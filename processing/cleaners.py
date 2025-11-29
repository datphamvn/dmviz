"""
Data cleaning processors.

Owner: Data Engineering Team
Handle missing values, outliers, duplicates.
"""

import pandas as pd

from dmviz.core.registry import Registry
from dmviz.processing.base import BaseProcessor


@Registry.register("missing_handler", category="processing")
class MissingHandler(BaseProcessor):
    """
    Handle missing values.
    
    Config: strategy ("drop" | "mean" | "median" | "mode" | "constant"), fill_value
    INPUT:  pd.DataFrame (with missing values)
    OUTPUT: pd.DataFrame (no missing values)
    
    State (after fit): learned fill values per column
    """
    
    name = "missing_handler"
    
    def __init__(self, config: dict | None = None):
        super().__init__(config)
        self.fill_values_: dict = {}  # Learned in fit()
    
    def fit(self, df: pd.DataFrame) -> "MissingHandler":
        """Learn fill values from training data."""
        raise NotImplementedError("TODO: Learn fill values")
    
    def _run(self, df: pd.DataFrame) -> pd.DataFrame:
        """Apply missing value handling."""
        raise NotImplementedError("TODO: Implement missing handling")


@Registry.register("outlier_handler", category="processing")
class OutlierHandler(BaseProcessor):
    """
    Handle outliers.
    
    Config: method ("iqr" | "zscore"), action ("drop" | "clip" | "nan"), threshold
    INPUT:  pd.DataFrame (with outliers)
    OUTPUT: pd.DataFrame (outliers handled)
    
    State (after fit): learned bounds per column
    """
    
    name = "outlier_handler"
    
    def __init__(self, config: dict | None = None):
        super().__init__(config)
        self.bounds_: dict = {}  # Learned in fit()
    
    def fit(self, df: pd.DataFrame) -> "OutlierHandler":
        """Learn outlier bounds from training data."""
        raise NotImplementedError("TODO: Learn bounds")
    
    def _run(self, df: pd.DataFrame) -> pd.DataFrame:
        """Apply outlier handling."""
        raise NotImplementedError("TODO: Implement outlier handling")
