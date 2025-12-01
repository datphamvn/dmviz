from sqlalchemy import Column, Integer, String, Float, DateTime
from database import Base

class Transaction(Base):
    __tablename__ = "transactions"

    id = Column(Integer, primary_key=True, index=True)
    Invoice = Column(String, index=True)
    StockCode = Column(String)
    Description = Column(String)
    Quantity = Column(Integer)
    InvoiceDate = Column(DateTime)
    Price = Column(Float)
    CustomerID = Column(Float)
    Country = Column(String)