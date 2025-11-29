"""
Ingestion module - data source connectors.

Owner: Data Engineering Team

Contract:
    INPUT:  str | Path | dict (source location)
    OUTPUT: pd.DataFrame
"""

from dmviz.src.ingestion.base import BaseIngester, BatchIngester
from dmviz.src.ingestion.connectors import (
    CSVIngester,
    JSONIngester,
    APIIngester,
    ParquetIngester,
    auto_ingest,
)
from dmviz.src.ingestion.streaming import KafkaConsumer

__all__ = [
    "BaseIngester",
    "BatchIngester",
    "CSVIngester",
    "JSONIngester",
    "APIIngester",
    "ParquetIngester",
    "KafkaConsumer",
    "auto_ingest",
]
