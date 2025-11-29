"""
Base classes for data storage.

Owner: Platform Team
Contract:
    INPUT:  pd.DataFrame + identifier
    OUTPUT: bool (success) | pd.DataFrame (read)
"""

from abc import abstractmethod
from pathlib import Path
from typing import Literal
import pandas as pd

from dmviz.core.base import BaseComponent


Layer = Literal["bronze", "silver", "gold"]


class BaseStorage(BaseComponent[pd.DataFrame, bool]):
    """
    Base class for storage backends.
    
    Contract:
        save():  pd.DataFrame -> bool
        read():  str -> pd.DataFrame
    
    Subclasses must implement:
        _run(df) -> bool  (save)
        read(identifier) -> pd.DataFrame
    """
    
    name = "storage"
    
    @abstractmethod
    def _run(self, df: pd.DataFrame) -> bool:
        """Save DataFrame to storage. Returns success status."""
        ...
    
    @abstractmethod
    def read(self, identifier: str) -> pd.DataFrame:
        """Read DataFrame from storage."""
        ...
    
    def exists(self, identifier: str) -> bool:
        """Check if data exists."""
        raise NotImplementedError("TODO: Implement exists check")


class DataLake:
    """
    Data Lake with Bronze/Silver/Gold layers (Medallion Architecture).
    
    Contract:
        save():  pd.DataFrame + name + layer -> Path
        read():  name + layer -> pd.DataFrame
    
    Layers:
        bronze - Raw data, as-is from source
        silver - Cleaned, validated data
        gold   - Business-ready, aggregated
    
    Usage:
        lake = DataLake("./data")
        lake.save(raw_df, "customers", layer="bronze")
        df = lake.read("customers", layer="silver")
    """
    
    def __init__(
        self,
        base_path: str | Path = "./data/lake",
        file_format: Literal["parquet", "csv"] = "parquet",
    ):
        self.base_path = Path(base_path)
        self.file_format = file_format
    
    def save(self, df: pd.DataFrame, name: str, layer: Layer = "bronze") -> Path:
        """Save DataFrame to a layer."""
        raise NotImplementedError("TODO: Implement save")
    
    def read(self, name: str, layer: Layer = "silver") -> pd.DataFrame:
        """Read DataFrame from a layer."""
        raise NotImplementedError("TODO: Implement read")
    
    def exists(self, name: str, layer: Layer) -> bool:
        """Check if dataset exists in layer."""
        raise NotImplementedError("TODO: Implement exists")
    
    def list_datasets(self, layer: Layer) -> list[str]:
        """List all datasets in a layer."""
        raise NotImplementedError("TODO: Implement listing")
    
    def promote(self, name: str, from_layer: Layer, to_layer: Layer) -> Path:
        """Copy dataset from one layer to another."""
        raise NotImplementedError("TODO: Implement promote")
