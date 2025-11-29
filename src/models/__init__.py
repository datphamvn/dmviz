"""
Models module - statistical and ML models.

Owner: ML/Statistics Team

Contract:
    fit(X, y):   arrays -> self
    predict(X):  array -> array
"""

from dmviz.src.models.base import BaseModel, ModelResult

# TODO: Implement these modules
# from dmviz.src.models.regression import LinearRegressor
# from dmviz.src.models.classification import LogisticClassifier
# from dmviz.src.models.em import EMClustering
# from dmviz.src.models.mle import MLEEstimator

__all__ = [
    "BaseModel",
    "ModelResult",
]
