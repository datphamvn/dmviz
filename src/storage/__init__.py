"""
Storage module - data persistence layer.

Owner: Platform Team

Contract:
    save():  pd.DataFrame -> bool | Path
    read():  str -> pd.DataFrame
"""

from dmviz.src.storage.base import BaseStorage, DataLake, Layer
from dmviz.src.storage.file import ParquetStorage, CSVStorage
from dmviz.src.storage.warehouse import WarehouseStorage

__all__ = [
    "BaseStorage",
    "DataLake",
    "Layer",
    "ParquetStorage",
    "CSVStorage",
    "WarehouseStorage",
]
