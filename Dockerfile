FROM python:3.10-slim

WORKDIR /app

# Copy requirements first to leverage cache
COPY ./paper_trading_backend/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of the application
COPY ./paper_trading_backend/ .

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8003"]
