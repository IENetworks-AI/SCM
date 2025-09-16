import os
import json
import requests
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import logging
import time

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Default args
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 9, 2),
    'email_on_failure': True,
    'email': ['your-email@example.com'],
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

# DAG definition
dag = DAG(
    'scm_extract_dag',
    default_args=default_args,
    description='Extract SCM data from APIs and save as JSON',
    schedule='@hourly',
    catchup=False,
)

# API endpoints and output files
API_ENDPOINTS = {
    "https://scm-backend-test.ienetworks.co/api/scm/stock/fixed-assets": "fixedasset.json",
    "https://scm-backend-test.ienetworks.co/api/scm/stock/project/inventories": "inventories.json",
    "https://scm-backend-test.ienetworks.co/api/scm/stock/tools/requested": "requested.json",
    "https://scm-backend-test.ienetworks.co/api/scm/stock/tools": "tools.json",
    "https://scm-backend-test.ienetworks.co/api/scm/stock/inventory/index": "index.json",
    "https://scm-backend-test.ienetworks.co/api/scm/stock/approved": "approved1.json",
    "https://scm-backend-test.ienetworks.co/api/scm/stock/inventory/allCustomRequests/new":"returned_inventory.json",
    "https://scm-backend-test.ienetworks.co/api/scm/stock/tools/allCustomRequests":"returned_tools.json"
}


BASE_OUTPUT_DIR = "/opt/airflow/output"


def fetch_and_save_json(**kwargs):
    """
    Fetch JSON data from APIs and save to local files.
    Handles retries for network/server errors.
    """
    access_token = os.getenv("ACCESS_TOKEN", "YOUR_ACCESS_TOKEN")
    headers = {"Authorization": f"Bearer {access_token}"}

    for url, output_file in API_ENDPOINTS.items():
        logger.info(f"Starting request to {url}")
        success = False

        for attempt in range(3):  # Retry up to 3 times
            try:
                response = requests.get(url, headers=headers, timeout=30)
                response.raise_for_status()  # Raise HTTPError for 4xx/5xx
                data = response.json()

                # Ensure output directory exists
                os.makedirs(BASE_OUTPUT_DIR, exist_ok=True)
                output_path = os.path.join(BASE_OUTPUT_DIR, output_file)

                with open(output_path, "w", encoding="utf-8") as f:
                    json.dump(data, f, ensure_ascii=False, indent=4)

                logger.info(f"Successfully saved data to {output_path}")
                success = True
                break  # Exit retry loop on success

            except requests.exceptions.HTTPError as e:
                logger.error(f"HTTP error on {url}: {e} (status {response.status_code})")
                if 500 <= response.status_code < 600 and attempt < 2:
                    logger.info("Server error, retrying in 5 seconds...")
                    time.sleep(5)
                else:
                    break

            except requests.exceptions.RequestException as e:
                logger.error(f"Request error on {url}: {e}")
                if attempt < 2:
                    logger.info("Retrying in 5 seconds...")
                    time.sleep(5)
                else:
                    break

            except json.JSONDecodeError as e:
                logger.error(f"JSON decode error on {url}: {e}")
                break

            except Exception as e:
                logger.error(f"Unexpected error on {url}: {e}")
                break

        if not success:
            logger.warning(f"Failed to fetch data from {url} after 3 attempts")

# Task
extract_task = PythonOperator(
    task_id='extract_scm_data',
    python_callable=fetch_and_save_json,
    dag=dag,
)