FROM apache/airflow:2.9.3

# Copy requirements
COPY requirements.txt .

# Install packages as airflow user
USER airflow
RUN pip install --no-cache-dir --upgrade pip \
    && pip install --no-cache-dir -r requirements.txt
