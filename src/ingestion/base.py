"""
Base classes for data ingestion.

Owner: Data Engineering Team
Contract:
    INPUT:  str | Path | dict (source location/config)
    OUTPUT: pd.DataFrame
"""

from abc import abstractmethod
from pathlib import Path
from typing import Any, Union, Iterator
import pandas as pd

from dmviz.src.core.base import BaseComponent


class BaseIngester(BaseComponent[Union[str, Path, dict], pd.DataFrame]):
    """
    Base class for all data ingesters.
    
    Contract:
        INPUT:  str | Path | dict (file path, URL, or config dict)
        OUTPUT: pd.DataFrame
    
    Subclasses must implement:
        _run(source) -> pd.DataFrame
    """
    
    name = "ingester"
    
    @abstractmethod
    def _run(self, source: Union[str, Path, dict]) -> pd.DataFrame:
        """Load data from source into DataFrame."""
        ...
    
    def stream(self, source: Any) -> Iterator[pd.DataFrame]:
        """Stream data in chunks. Override for large datasets."""
        yield self._run(source)


class BatchIngester(BaseIngester):
    """Ingester with chunking support for large files."""
    
    name = "batch_ingester"
    
    def __init__(self, config: dict[str, Any] | None = None):
        super().__init__(config)
        self.chunk_size = self.config.get("chunk_size", 10000)
    
    def stream(self, source: Any) -> Iterator[pd.DataFrame]:
        """Yield data in chunks."""
        raise NotImplementedError("TODO: Implement chunked streaming")
