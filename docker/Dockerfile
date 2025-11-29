FROM python:3.11-slim

WORKDIR /app

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy package
COPY . .

# Install package in editable mode
RUN pip install --no-cache-dir -e .

CMD ["python"]

