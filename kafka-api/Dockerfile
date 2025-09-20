FROM python:3.11-slim

WORKDIR /app

# Copy requirements and install
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy API code
COPY . .

EXPOSE 8000

CMD ["uvicorn", "kafka_api:app", "--host", "0.0.0.0", "--port", "8000"]
