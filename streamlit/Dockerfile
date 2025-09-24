# ----------------------------
# Streamlit Dockerfile (Python 3.11, Kafka-compatible)
# ----------------------------

FROM python:3.11-slim

# Set working directory
WORKDIR /app

# Copy requirements first for caching
COPY requirements.txt .

# Install system dependencies safely
RUN apt-get update && apt-get upgrade -y \
    && apt-get install -y --no-install-recommends \
        build-essential \
        gcc \
        g++ \
        libpq-dev \
        netcat-openbsd \
        curl \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Upgrade pip and install Python dependencies
RUN pip install --no-cache-dir --upgrade pip
RUN pip install --no-cache-dir -r requirements.txt
RUN pip uninstall -y google-generativeai
RUN pip install google-genai





# Copy the Streamlit app and scripts
COPY . .

# Expose Streamlit port
EXPOSE 8501

# Environment variables (can be overridden in Docker Compose)
ENV KAFKA_HOST=kafka
ENV KAFKA_PORT=9092
ENV ML_API_URL=http://scm_ml-api:8001

# Start Streamlit after waiting for Kafka
CMD ["bash", "-c", "/app/wait-for-kafka.sh && streamlit run /app/streamlit_app.py --server.port 8501 --server.address 0.0.0.0"]
