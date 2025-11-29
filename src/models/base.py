"""
Base classes for statistical models.

Owner: ML/Statistics Team
Contract:
    fit():     X, y -> self
    predict(): X -> np.ndarray
"""

from abc import abstractmethod
from dataclasses import dataclass, field
from typing import Any, Optional, Union
import numpy as np
import pandas as pd


@dataclass
class ModelResult:
    """
    Result container for model fitting.
    
    Attributes:
        coefficients: Model parameters (weights)
        intercept: Bias term (if applicable)
        predictions: Training predictions
        metrics: Performance metrics (r2, mse, accuracy, etc.)
        converged: Whether optimization converged
        iterations: Number of iterations (for iterative solvers)
    """
    
    coefficients: Optional[np.ndarray] = None
    intercept: Optional[float] = None
    predictions: Optional[np.ndarray] = None
    metrics: dict[str, float] = field(default_factory=dict)
    converged: bool = True
    iterations: int = 0
    
    def summary(self) -> str:
        """Generate text summary of results."""
        raise NotImplementedError("TODO: Implement summary")


class BaseModel:
    """
    Base class for all statistical models.
    
    Contract:
        fit(X, y):   np.ndarray, np.ndarray -> self
        predict(X):  np.ndarray -> np.ndarray
    
    After fit:
        is_fitted = True
        result_: ModelResult with coefficients and metrics
    
    Subclasses must implement:
        fit(X, y) -> self
        predict(X) -> np.ndarray
    """
    
    name: str = "base_model"
    
    def __init__(self, **kwargs):
        self.params = kwargs
        self.is_fitted = False
        self.result_: Optional[ModelResult] = None
    
    @abstractmethod
    def fit(
        self,
        X: Union[np.ndarray, pd.DataFrame],
        y: Optional[Union[np.ndarray, pd.Series]] = None,
    ) -> "BaseModel":
        """Fit model to data. Override in subclasses."""
        ...
    
    @abstractmethod
    def predict(self, X: Union[np.ndarray, pd.DataFrame]) -> np.ndarray:
        """Make predictions. Override in subclasses."""
        ...
    
    def fit_predict(
        self,
        X: Union[np.ndarray, pd.DataFrame],
        y: Optional[Union[np.ndarray, pd.Series]] = None,
    ) -> np.ndarray:
        """Fit and predict in one call."""
        return self.fit(X, y).predict(X)
    
    def _to_numpy(
        self,
        X: Union[np.ndarray, pd.DataFrame],
        y: Optional[Union[np.ndarray, pd.Series]] = None,
    ) -> tuple[np.ndarray, Optional[np.ndarray]]:
        """Convert inputs to numpy arrays."""
        X_arr = X.values if isinstance(X, pd.DataFrame) else np.asarray(X)
        y_arr = None
        if y is not None:
            y_arr = y.values if isinstance(y, pd.Series) else np.asarray(y)
        return X_arr, y_arr
    
    def get_params(self) -> dict[str, Any]:
        """Get model parameters (sklearn compatibility)."""
        return self.params.copy()
    
    def set_params(self, **params) -> "BaseModel":
        """Set model parameters (sklearn compatibility)."""
        self.params.update(params)
        return self
