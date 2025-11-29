"""
Data transformation processors.

Owner: Data Engineering Team
Normalization, encoding, scaling.
"""

import pandas as pd

from dmviz.core.registry import Registry
from dmviz.processing.base import BaseProcessor


@Registry.register("normalizer", category="processing")
class Normalizer(BaseProcessor):
    """
    Normalize numeric columns.
    
    Config: method ("zscore" | "minmax" | "robust")
    INPUT:  pd.DataFrame (numeric columns)
    OUTPUT: pd.DataFrame (normalized)
    
    State (after fit): mean, std, min, max per column
    """
    
    name = "normalizer"
    
    def __init__(self, config: dict | None = None):
        super().__init__(config)
        self.stats_: dict = {}  # Learned in fit()
    
    def fit(self, df: pd.DataFrame) -> "Normalizer":
        """Learn normalization parameters."""
        raise NotImplementedError("TODO: Learn normalization stats")
    
    def _run(self, df: pd.DataFrame) -> pd.DataFrame:
        """Apply normalization."""
        raise NotImplementedError("TODO: Implement normalization")


@Registry.register("encoder", category="processing")
class Encoder(BaseProcessor):
    """
    Encode categorical columns.
    
    Config: method ("onehot" | "label" | "target"), columns
    INPUT:  pd.DataFrame (with categorical columns)
    OUTPUT: pd.DataFrame (encoded)
    
    State (after fit): category mappings per column
    """
    
    name = "encoder"
    
    def __init__(self, config: dict | None = None):
        super().__init__(config)
        self.mappings_: dict = {}  # Learned in fit()
    
    def fit(self, df: pd.DataFrame) -> "Encoder":
        """Learn encoding mappings."""
        raise NotImplementedError("TODO: Learn mappings")
    
    def _run(self, df: pd.DataFrame) -> pd.DataFrame:
        """Apply encoding."""
        raise NotImplementedError("TODO: Implement encoding")
