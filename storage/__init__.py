"""
Storage module - data persistence layer.

Owner: Platform Team

Contract:
    save():  pd.DataFrame -> bool | Path
    read():  str -> pd.DataFrame
"""

from dmviz.storage.base import BaseStorage, DataLake, Layer
from dmviz.storage.file import ParquetStorage, CSVStorage
from dmviz.storage.warehouse import WarehouseStorage

__all__ = [
    "BaseStorage",
    "DataLake",
    "Layer",
    "ParquetStorage",
    "CSVStorage",
    "WarehouseStorage",
]
