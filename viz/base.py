"""
Base visualization utilities.

Owner: Analytics Team
Contract:
    INPUT:  data (array, DataFrame, or model outputs)
    OUTPUT: Figure (wrapper around matplotlib figure)
"""

from pathlib import Path
from typing import Optional, Union
from dataclasses import dataclass, field
import matplotlib.pyplot as plt
import numpy as np


@dataclass
class Figure:
    """
    Container for matplotlib figure.
    
    Methods:
        save(path):  Save to file
        show():      Display interactively
        close():     Free memory
    """
    
    fig: plt.Figure
    axes: Union[plt.Axes, np.ndarray]
    title: str = ""
    
    def save(self, path: Union[str, Path], dpi: int = 150) -> Path:
        """Save figure to file."""
        raise NotImplementedError("TODO: Implement save")
    
    def show(self) -> None:
        """Display the figure."""
        plt.show()
    
    def close(self) -> None:
        """Close figure to free memory."""
        plt.close(self.fig)


@dataclass
class PlotStyle:
    """
    Styling configuration for plots.
    
    Attributes:
        figsize: (width, height) in inches
        style: matplotlib style name
        colors: color palette list
    """
    
    figsize: tuple[int, int] = (10, 6)
    style: str = "seaborn-v0_8-whitegrid"
    colors: list[str] = field(default_factory=lambda: [
        "#4C72B0", "#55A868", "#C44E52", "#8172B3",
    ])
    
    def apply(self) -> None:
        """Apply style settings globally."""
        raise NotImplementedError("TODO: Implement style application")


class Plotter:
    """
    Main plotting interface.
    
    Contract:
        figure() -> Figure
        save(path) -> Path
    
    Usage:
        plotter = Plotter()
        fig = plotter.figure(nrows=2, ncols=2)
        # ... draw on fig.axes ...
        plotter.save("output.png")
    """
    
    def __init__(self, style: Optional[PlotStyle] = None):
        self.style = style or PlotStyle()
        self._current_fig: Optional[Figure] = None
    
    def figure(self, nrows: int = 1, ncols: int = 1, figsize: tuple | None = None) -> Figure:
        """Create a new figure."""
        raise NotImplementedError("TODO: Implement figure creation")
    
    def save(self, path: Union[str, Path], fig: Optional[Figure] = None) -> Path:
        """Save figure to file."""
        raise NotImplementedError("TODO: Implement save")
    
    def get_colors(self, n: int) -> list[str]:
        """Get n colors from palette."""
        raise NotImplementedError("TODO: Implement color selection")
