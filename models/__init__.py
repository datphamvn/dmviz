"""
Models module - statistical and ML models.

Owner: ML/Statistics Team

Contract:
    fit(X, y):   arrays -> self
    predict(X):  array -> array
"""

from dmviz.models.base import BaseModel, ModelResult
from dmviz.models.regression import LinearRegressor
from dmviz.models.classification import LogisticClassifier
from dmviz.models.em import EMClustering
from dmviz.models.mle import MLEEstimator

__all__ = [
    "BaseModel",
    "ModelResult",
    "LinearRegressor",
    "LogisticClassifier",
    "EMClustering",
    "MLEEstimator",
]
