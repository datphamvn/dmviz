"""
Database/warehouse storage backends.

Owner: Platform Team
SQL databases and data warehouses.
"""

import pandas as pd

from dmviz.src.core.registry import Registry
from dmviz.src.storage.base import BaseStorage


@Registry.register("warehouse_storage", category="storage")
class WarehouseStorage(BaseStorage):
    """
    SQL database storage.
    
    Config: connection_string, table_name, schema, if_exists
    INPUT:  pd.DataFrame
    OUTPUT: bool (success)
    """
    
    name = "warehouse_storage"
    
    def _run(self, df: pd.DataFrame) -> bool:
        """Save to database table."""
        raise NotImplementedError("TODO: Implement DB write")
    
    def read(self, identifier: str) -> pd.DataFrame:
        """Read from database (table name or SQL query)."""
        raise NotImplementedError("TODO: Implement DB read")
    
    def execute(self, query: str) -> pd.DataFrame:
        """Execute arbitrary SQL query."""
        raise NotImplementedError("TODO: Implement SQL execution")
