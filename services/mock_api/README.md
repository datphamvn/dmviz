# FastAPI Project

This is a FastAPI project with SQLite database integration.

## Setup

1. Install dependencies:
   ```
   pip install -r requirements.txt
   ```

2. Run the application:
   ```
   uvicorn main:app --reload
   ```

3. Access the API at http://127.0.0.1:8000

## API Endpoints

- GET / : Root message
- GET /transactions/ : List transactions
- GET /transactions/{id} : Get transaction by ID
- POST /transactions/ : Create new transaction (provide Invoice, StockCode, Description, Quantity, InvoiceDate, Price, CustomerID, Country)
- PUT /transactions/{id} : Update transaction
- DELETE /transactions/{id} : Delete transaction
- GET /api/v1/raw_transactions : Get raw transactions with pagination and date filtering (params: limit, offset, start_date, end_date) - Requires Bearer token authentication

## Authentication

The `/api/v1/raw_transactions` endpoint requires a Bearer token. Set the `API_KEY` environment variable or use the default "mysecretkey". Include in requests: `Authorization: Bearer mysecretkey`

## Loading Data

Data is automatically loaded from the Excel file on startup if the database is empty.