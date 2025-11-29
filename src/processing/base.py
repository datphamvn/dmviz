"""
Base classes for data processing.

Owner: Data Engineering Team
Contract:
    INPUT:  pd.DataFrame
    OUTPUT: pd.DataFrame
"""

from abc import abstractmethod
from typing import Any
import pandas as pd

from dmviz.src.core.base import BaseComponent


class BaseProcessor(BaseComponent[pd.DataFrame, pd.DataFrame]):
    """
    Base class for all data processors.
    
    Contract:
        INPUT:  pd.DataFrame
        OUTPUT: pd.DataFrame
    
    Subclasses must implement:
        _run(df) -> pd.DataFrame
    
    Optional override:
        fit(df) -> self  (for processors that learn parameters)
    """
    
    name = "processor"
    
    @abstractmethod
    def _run(self, df: pd.DataFrame) -> pd.DataFrame:
        """Process the DataFrame. Override in subclasses."""
        ...
    
    def fit(self, df: pd.DataFrame) -> "BaseProcessor":
        """Fit processor on training data. Override for stateful processors."""
        return self
    
    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        """Alias for _run (sklearn API compatibility)."""
        return self._run(df)
    
    def fit_transform(self, df: pd.DataFrame) -> pd.DataFrame:
        """Fit and transform in one call."""
        return self.fit(df).transform(df)


class ProcessorChain:
    """
    Chain multiple processors sequentially.
    
    Contract:
        INPUT:  pd.DataFrame
        OUTPUT: pd.DataFrame (after all processors applied)
    
    Usage:
        chain = ProcessorChain([MissingHandler(), Normalizer()])
        clean_df = chain.process(raw_df)
    """
    
    def __init__(self, processors: list[BaseProcessor]):
        self.processors = processors
    
    def process(self, df: pd.DataFrame) -> pd.DataFrame:
        """Apply all processors in sequence."""
        result = df
        for processor in self.processors:
            result = processor.transform(result)
        return result
    
    def fit(self, df: pd.DataFrame) -> "ProcessorChain":
        """Fit all processors on training data."""
        raise NotImplementedError("TODO: Implement chain fitting")
    
    def add(self, processor: BaseProcessor) -> "ProcessorChain":
        """Add a processor to the chain."""
        self.processors.append(processor)
        return self
