"""
Ingestion module - data source connectors.

Owner: Data Engineering Team

Contract:
    INPUT:  str | Path | dict (source location)
    OUTPUT: pd.DataFrame
"""

from dmviz.ingestion.base import BaseIngester, BatchIngester
from dmviz.ingestion.connectors import (
    CSVIngester,
    JSONIngester,
    APIIngester,
    ParquetIngester,
    auto_ingest,
)
from dmviz.ingestion.streaming import KafkaConsumer

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
