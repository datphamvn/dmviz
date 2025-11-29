"""
Dataset utilities for dmviz.

Provides easy access to demonstration datasets.
"""

from pathlib import Path
from typing import Optional
import pandas as pd
import yaml


CATALOG_PATH = Path(__file__).parent / "catalog.yaml"


def get_catalog() -> dict:
    """Load dataset catalog."""
    with open(CATALOG_PATH) as f:
        return yaml.safe_load(f)["datasets"]


def list_datasets() -> list[str]:
    """List available dataset names."""
    return list(get_catalog().keys())


def load_dataset(name: str, as_frame: bool = True) -> pd.DataFrame:
    """
    Load a dataset by name.
    
    Args:
        name: Dataset name (e.g., "california_housing", "iris")
        as_frame: Return as DataFrame (default True)
    
    Returns:
        DataFrame with features and target
    """
    catalog = get_catalog()
    
    if name not in catalog:
        raise ValueError(f"Unknown dataset: {name}. Available: {list_datasets()}")
    
    info = catalog[name]
    source = info.get("source")
    
    if source == "sklearn":
        return _load_sklearn(info["loader"])
    else:
        raise NotImplementedError(f"Source '{source}' not yet supported. Use download.py")


def _load_sklearn(loader: str) -> pd.DataFrame:
    """Load dataset from sklearn."""
    import importlib
    
    module_path, func_name = loader.rsplit(".", 1)
    module = importlib.import_module(module_path)
    loader_func = getattr(module, func_name)
    
    data = loader_func(as_frame=True)
    
    if hasattr(data, "frame"):
        return data.frame
    else:
        # Older sklearn format
        df = pd.DataFrame(data.data, columns=data.feature_names)
        df["target"] = data.target
        return df


__all__ = ["get_catalog", "list_datasets", "load_dataset"]

