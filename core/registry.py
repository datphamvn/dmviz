"""
Component Registry for dynamic discovery and instantiation.

Enables teams to register components independently.
"""

from typing import Type, TypeVar, Optional, Any
from dmviz.core.base import BaseComponent

T = TypeVar("T", bound=BaseComponent)


class Registry:
    """
    Global registry for pipeline components.
    
    Usage:
        # Register (in your module)
        @Registry.register("csv_loader", category="ingestion")
        class CSVLoader(BaseIngester):
            ...
        
        # Retrieve (anywhere)
        loader = Registry.create("csv_loader", config)
    """
    
    _components: dict[str, Type[BaseComponent]] = {}
    _categories: dict[str, set[str]] = {}
    
    @classmethod
    def register(cls, name: str, category: str = "default"):
        """Decorator to register a component class."""
        def decorator(component_cls: Type[T]) -> Type[T]:
            cls._components[name] = component_cls
            if category not in cls._categories:
                cls._categories[category] = set()
            cls._categories[category].add(name)
            component_cls.name = name
            return component_cls
        return decorator
    
    @classmethod
    def get(cls, name: str) -> Optional[Type[BaseComponent]]:
        """Retrieve a component class by name."""
        return cls._components.get(name)
    
    @classmethod
    def create(cls, name: str, config: Optional[dict[str, Any]] = None) -> BaseComponent:
        """Instantiate a component by name with config."""
        component_cls = cls.get(name)
        if component_cls is None:
            raise KeyError(f"Component '{name}' not found")
        return component_cls(config)
    
    @classmethod
    def list_all(cls) -> list[str]:
        """List all registered component names."""
        return list(cls._components.keys())
    
    @classmethod
    def list_category(cls, category: str) -> list[str]:
        """List components in a category."""
        return list(cls._categories.get(category, set()))
