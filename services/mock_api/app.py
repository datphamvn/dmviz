"""Minimal Mock API for transaction data."""

from datetime import datetime, timedelta
from fastapi import FastAPI, HTTPException, Depends
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
import random
import os

app = FastAPI(title="Mock Transaction API")
bearer = HTTPBearer()
API_KEY = os.getenv("API_KEY", "sk_9f9d4af6075324d188c58252687db3da32a255a2fe528d8230b07e41e112c251")

# Mock data generator
COUNTRIES = ["United Kingdom", "France", "Germany", "Spain", "Italy", "Netherlands"]
PRODUCTS = [
    ("85123A", "WHITE HANGING HEART T-LIGHT HOLDER"),
    ("71053", "WHITE METAL LANTERN"),
    ("84406B", "CREAM CUPID HEARTS COAT HANGER"),
    ("84029G", "KNITTED UNION FLAG HOT WATER BOTTLE"),
    ("84029E", "RED WOOLLY HOTTIE WHITE HEART"),
    ("22752", "SET 7 BABUSHKA NESTING BOXES"),
    ("21730", "GLASS STAR FROSTED T-LIGHT HOLDER"),
]

def generate_transactions(start_date: datetime, end_date: datetime, count: int = 1000) -> list:
    """Generate mock transaction data."""
    transactions = []
    delta = (end_date - start_date).days
    for i in range(count):
        product = random.choice(PRODUCTS)
        date = start_date + timedelta(days=random.randint(0, max(delta, 1)), 
                                       hours=random.randint(8, 18),
                                       minutes=random.randint(0, 59))
        transactions.append({
            "Invoice": f"{500000 + i}",
            "StockCode": product[0],
            "Description": product[1],
            "Quantity": random.randint(1, 12),
            "InvoiceDate": date.isoformat(),
            "Price": round(random.uniform(1.0, 50.0), 2),
            "CustomerID": float(random.randint(12000, 18000)),
            "Country": random.choice(COUNTRIES),
        })
    return sorted(transactions, key=lambda x: x["InvoiceDate"])

# Pre-generate mock data (Dec 2010 - Dec 2011)
MOCK_DATA = generate_transactions(datetime(2010, 12, 1), datetime(2011, 12, 9), 5000)


def verify_token(credentials: HTTPAuthorizationCredentials = Depends(bearer)):
    if credentials.credentials != API_KEY:
        raise HTTPException(status_code=401, detail="Invalid token")
    return credentials.credentials


@app.get("/")
def root():
    return {"message": "Mock Transaction API", "total_records": len(MOCK_DATA)}


@app.get("/api/v1/raw_transactions")
def get_raw_transactions(
    limit: int = 100,
    offset: int = 0,
    start_date: str = None,
    end_date: str = None,
    token: str = Depends(verify_token),
):
    """Get raw transactions with pagination and date filtering."""
    data = MOCK_DATA
    
    if start_date:
        start = datetime.strptime(start_date, "%Y-%m-%d")
        data = [t for t in data if datetime.fromisoformat(t["InvoiceDate"]) >= start]
    
    if end_date:
        end = datetime.strptime(end_date, "%Y-%m-%d")
        data = [t for t in data if datetime.fromisoformat(t["InvoiceDate"]) <= end]
    
    return data[offset : offset + limit]


@app.get("/transactions/")
def list_transactions(skip: int = 0, limit: int = 10):
    """List transactions (no auth required)."""
    return MOCK_DATA[skip : skip + limit]


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)

