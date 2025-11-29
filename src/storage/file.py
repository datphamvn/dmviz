"""
File-based storage backends.

Owner: Platform Team
Local and cloud file storage.
"""

import pandas as pd

from dmviz.src.core.registry import Registry
from dmviz.src.storage.base import BaseStorage


@Registry.register("parquet_storage", category="storage")
class ParquetStorage(BaseStorage):
    """
    Parquet file storage.
    
    Config: path, compression
    INPUT:  pd.DataFrame
    OUTPUT: bool (success)
    """
    
    name = "parquet_storage"
    
    def _run(self, df: pd.DataFrame) -> bool:
        """Save as Parquet."""
        raise NotImplementedError("TODO: Implement Parquet save")
    
    def read(self, identifier: str) -> pd.DataFrame:
        """Read from Parquet."""
        raise NotImplementedError("TODO: Implement Parquet read")


@Registry.register("csv_storage", category="storage")
class CSVStorage(BaseStorage):
    """
    CSV file storage.
    
    Config: path, delimiter, encoding
    INPUT:  pd.DataFrame
    OUTPUT: bool (success)
    """
    
    name = "csv_storage"
    
    def _run(self, df: pd.DataFrame) -> bool:
        """Save as CSV."""
        raise NotImplementedError("TODO: Implement CSV save")
    
    def read(self, identifier: str) -> pd.DataFrame:
        """Read from CSV."""
        raise NotImplementedError("TODO: Implement CSV read")
