from fastapi import FastAPI, Depends, HTTPException
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from sqlalchemy.orm import Session
from database import SessionLocal, engine
from models import Base, Transaction
import pandas as pd
from datetime import datetime, timedelta
import os

Base.metadata.create_all(bind=engine)

app = FastAPI()

bearer = HTTPBearer()

API_KEY = os.getenv("API_KEY", "mysecretkey")

def get_current_user(credentials: HTTPAuthorizationCredentials = Depends(bearer)):
    if credentials.credentials != API_KEY:
        raise HTTPException(status_code=401, detail="Invalid token")
    return credentials.credentials

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

@app.on_event("startup")
def startup_event():
    db = SessionLocal()
    try:
        # Check if data is already loaded
        if db.query(Transaction).first() is None:
            print("Loading data from Excel to database...")
            load_excel_to_db('/app/data/online_retail_II.xlsx',
                             'Year 2009-2010', 
                             db)
            load_excel_to_db('/app/data/online_retail_II.xlsx',
                             'Year 2010-2011',
                             db)
    finally:
        db.close()

@app.get("/")
def read_root():
    return {"message": "FastAPI with SQLite"}

@app.get("/transactions/", response_model=list[dict])
def read_transactions(skip: int = 0, limit: int = 10, db: Session = Depends(get_db)):
    transactions = db.query(Transaction).offset(skip).limit(limit).all()
    return [{"id": t.id, "Invoice": t.Invoice, "StockCode": t.StockCode, "Description": t.Description, "Quantity": t.Quantity, "InvoiceDate": t.InvoiceDate, "Price": t.Price, "CustomerID": t.CustomerID, "Country": t.Country} for t in transactions]

@app.get("/transactions/{transaction_id}", response_model=dict)
def read_transaction(transaction_id: int, db: Session = Depends(get_db)):
    transaction = db.query(Transaction).filter(Transaction.id == transaction_id).first()
    if transaction is None:
        raise HTTPException(status_code=404, detail="Transaction not found")
    return {"id": transaction.id, "Invoice": transaction.Invoice, "StockCode": transaction.StockCode, "Description": transaction.Description, "Quantity": transaction.Quantity, "InvoiceDate": transaction.InvoiceDate, "Price": transaction.Price, "CustomerID": transaction.CustomerID, "Country": transaction.Country}

@app.post("/transactions/", response_model=dict)
def create_transaction(Invoice: str, StockCode: str, Description: str, Quantity: int, InvoiceDate: str, Price: float, CustomerID: float, Country: str, db: Session = Depends(get_db)):
    invoice_date = datetime.strptime(InvoiceDate, '%Y-%m-%d %H:%M:%S')
    db_transaction = Transaction(Invoice=Invoice, StockCode=StockCode, Description=Description, Quantity=Quantity, InvoiceDate=invoice_date, Price=Price, CustomerID=CustomerID, Country=Country)
    db.add(db_transaction)
    db.commit()
    db.refresh(db_transaction)
    return {"id": db_transaction.id, "Invoice": db_transaction.Invoice, "StockCode": db_transaction.StockCode, "Description": db_transaction.Description, "Quantity": db_transaction.Quantity, "InvoiceDate": db_transaction.InvoiceDate, "Price": db_transaction.Price, "CustomerID": db_transaction.CustomerID, "Country": db_transaction.Country}

@app.put("/transactions/{transaction_id}", response_model=dict)
def update_transaction(transaction_id: int, Invoice: str, StockCode: str, Description: str, Quantity: int, InvoiceDate: str, Price: float, CustomerID: float, Country: str, db: Session = Depends(get_db)):
    transaction = db.query(Transaction).filter(Transaction.id == transaction_id).first()
    if transaction is None:
        raise HTTPException(status_code=404, detail="Transaction not found")
    invoice_date = datetime.strptime(InvoiceDate, '%Y-%m-%d %H:%M:%S')
    transaction.Invoice = Invoice
    transaction.StockCode = StockCode
    transaction.Description = Description
    transaction.Quantity = Quantity
    transaction.InvoiceDate = invoice_date
    transaction.Price = Price
    transaction.CustomerID = CustomerID
    transaction.Country = Country
    db.commit()
    db.refresh(transaction)
    return {"id": transaction.id, "Invoice": transaction.Invoice, "StockCode": transaction.StockCode, "Description": transaction.Description, "Quantity": transaction.Quantity, "InvoiceDate": transaction.InvoiceDate, "Price": transaction.Price, "CustomerID": transaction.CustomerID, "Country": transaction.Country}

@app.delete("/transactions/{transaction_id}")
def delete_transaction(transaction_id: int, db: Session = Depends(get_db)):
    transaction = db.query(Transaction).filter(Transaction.id == transaction_id).first()
    if transaction is None:
        raise HTTPException(status_code=404, detail="Transaction not found")
    db.delete(transaction)
    db.commit()
    return {"message": "Transaction deleted"}

@app.get("/api/v1/raw_transactions")
def get_raw_transactions(limit: int = 100, offset: int = 0, start_date: str = None, end_date: str = None, db: Session = Depends(get_db), token: str = Depends(get_current_user)):
    query = db.query(Transaction)
    if start_date:
        start = datetime.strptime(start_date, '%Y-%m-%d')
        query = query.filter(Transaction.InvoiceDate >= start)
    if end_date:
        # Include entire end_date by adding 1 day
        end = datetime.strptime(end_date, '%Y-%m-%d') + timedelta(days=1)
        query = query.filter(Transaction.InvoiceDate < end)
    transactions = query.offset(offset).limit(limit).all()
    return [
        {
            "Invoice": t.Invoice,
            "StockCode": t.StockCode,
            "Description": t.Description,
            "Quantity": t.Quantity,
            "InvoiceDate": t.InvoiceDate.isoformat() if t.InvoiceDate else None,
            "Price": t.Price,
            "CustomerID": t.CustomerID,
            "Country": t.Country
        } for t in transactions
    ]


def load_excel_to_db(excel_path: str, sheet_name: str, db: Session):
    df = pd.read_excel(excel_path, sheet_name=sheet_name)
    for _, row in df.iterrows():
        # Assuming Excel has the required columns
        invoice_date = pd.to_datetime(row['InvoiceDate'])
        db_transaction = Transaction(
            Invoice=str(row['Invoice']),
            StockCode=str(row['StockCode']),
            Description=str(row['Description']) if pd.notna(row['Description']) else None,
            Quantity=int(row['Quantity']),
            InvoiceDate=invoice_date,
            Price=float(row['Price']),
            CustomerID=float(row['Customer ID']) if pd.notna(row['Customer ID']) else None,
            Country=str(row['Country'])
        )
        print("Inserting transaction:", row['Invoice'])
        db.add(db_transaction)
        print("Inserting success")
    db.commit()