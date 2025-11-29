"""
Core base classes for pipeline components.

Design: Every component inherits from BaseComponent for consistent interface.
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Generic, TypeVar, Optional

T = TypeVar("T")
InputT = TypeVar("InputT")
OutputT = TypeVar("OutputT")


@dataclass
class StageResult(Generic[T]):
    """
    Standardized result from any pipeline stage.
    
    Attributes:
        data: Output data from the stage
        success: Whether stage completed successfully
        stage: Name of the stage that produced this result
        timestamp: When the stage completed
        metrics: Performance/quality metrics (e.g., elapsed_seconds, rows_processed)
        errors: List of error messages if failed
    """
    
    data: T
    success: bool = True
    stage: str = ""
    timestamp: datetime = field(default_factory=datetime.now)
    metrics: dict[str, Any] = field(default_factory=dict)
    errors: list[str] = field(default_factory=list)
    
    def __bool__(self) -> bool:
        return self.success


class BaseComponent(ABC, Generic[InputT, OutputT]):
    """
    Base class for all pipeline components.
    
    Contract:
        - INPUT: InputT (defined by subclass)
        - OUTPUT: StageResult[OutputT] via execute()
    
    Subclasses must implement:
        - _run(input_data: InputT) -> OutputT
    """
    
    name: str = "base"
    
    def __init__(self, config: Optional[dict[str, Any]] = None):
        self.config = config or {}
    
    @abstractmethod
    def _run(self, input_data: InputT) -> OutputT:
        """Implement actual logic here. Override in subclasses."""
        ...
    
    def execute(self, input_data: InputT) -> StageResult[OutputT]:
        """Execute component with error handling. Returns StageResult."""
        raise NotImplementedError("TODO: Implement execute with error handling")


class Pipeline:
    """
    Compose multiple components into a pipeline.
    
    Contract:
        - INPUT: initial_input (any type)
        - OUTPUT: StageResult from final stage
    
    Usage:
        pipeline = Pipeline([Ingester(), Processor(), Saver()])
        result = pipeline.run(initial_data)
    """
    
    def __init__(self, stages: list[BaseComponent]):
        self.stages = stages
    
    def run(self, initial_input: Any) -> StageResult:
        """Execute all stages sequentially."""
        raise NotImplementedError("TODO: Implement pipeline execution")
