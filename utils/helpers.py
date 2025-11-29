"""
Shared utility functions.

Owner: All teams
"""

from pathlib import Path
from typing import Any
import pandas as pd


def load_config(path: str | Path) -> dict[str, Any]:
    """
    Load configuration from YAML/JSON file.
    
    INPUT:  file path (yaml or json)
    OUTPUT: dict
    """
    raise NotImplementedError("TODO: Implement config loading")


def timer(func):
    """
    Decorator to time function execution.
    
    Usage:
        @timer
        def my_function():
            ...
    """
    raise NotImplementedError("TODO: Implement timer decorator")


def validate_dataframe(
    df: pd.DataFrame,
    required_columns: list[str] | None = None,
    dtypes: dict[str, str] | None = None,
) -> bool:
    """
    Validate DataFrame structure.
    
    INPUT:  DataFrame, optional schema requirements
    OUTPUT: bool (True if valid, raises if not)
    """
    raise NotImplementedError("TODO: Implement validation")
