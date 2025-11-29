"""
Data source connectors - implement one per data source.

Owner: Data Engineering Team
Each connector handles a specific data source type.
"""

from pathlib import Path
from typing import Union
import pandas as pd

from dmviz.src.core.registry import Registry
from dmviz.src.ingestion.base import BaseIngester, BatchIngester


@Registry.register("csv_ingester", category="ingestion")
class CSVIngester(BatchIngester):
    """
    Load CSV files.
    
    Config: delimiter, encoding, chunk_size
    INPUT:  str | Path (file path)
    OUTPUT: pd.DataFrame
    """
    
    name = "csv_ingester"
    
    def _run(self, source: Union[str, Path, dict]) -> pd.DataFrame:
        raise NotImplementedError("TODO: Implement CSV loading")


@Registry.register("json_ingester", category="ingestion")
class JSONIngester(BaseIngester):
    """
    Load JSON files.
    
    Config: orient, lines
    INPUT:  str | Path (file path)
    OUTPUT: pd.DataFrame
    """
    
    name = "json_ingester"
    
    def _run(self, source: Union[str, Path, dict]) -> pd.DataFrame:
        raise NotImplementedError("TODO: Implement JSON loading")


@Registry.register("api_ingester", category="ingestion")
class APIIngester(BaseIngester):
    """
    Load from REST APIs.
    
    Config: headers, params, json_path
    INPUT:  str (URL)
    OUTPUT: pd.DataFrame
    """
    
    name = "api_ingester"
    
    def _run(self, source: Union[str, Path, dict]) -> pd.DataFrame:
        raise NotImplementedError("TODO: Implement API loading")


@Registry.register("parquet_ingester", category="ingestion")
class ParquetIngester(BaseIngester):
    """
    Load Parquet files.
    
    INPUT:  str | Path (file path)
    OUTPUT: pd.DataFrame
    """
    
    name = "parquet_ingester"
    
    def _run(self, source: Union[str, Path, dict]) -> pd.DataFrame:
        raise NotImplementedError("TODO: Implement Parquet loading")


def auto_ingest(source: Union[str, Path]) -> pd.DataFrame:
    """Auto-detect file type and ingest."""
    raise NotImplementedError("TODO: Implement auto-detection")
