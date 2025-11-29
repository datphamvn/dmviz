"""
Visualization module - plotting and graphics.

Owner: Analytics Team

Contract:
    INPUT:  data arrays, DataFrames, or model outputs
    OUTPUT: Figure objects (save, show, close)
"""

from dmviz.src.viz.base import Figure, PlotStyle, Plotter

# TODO: Implement these modules
# from dmviz.src.viz.eda import distribution_plot, correlation_heatmap, histogram, boxplot
# from dmviz.src.viz.model import regression_diagnostics, roc_curve, confusion_matrix, learning_curve
# from dmviz.src.viz.cluster import cluster_scatter, gmm_contours, elbow_plot

__all__ = [
    # Base
    "Figure",
    "PlotStyle",
    "Plotter",
]
