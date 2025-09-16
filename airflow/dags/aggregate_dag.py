import os
import json
import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import logging
from kafka import KafkaProducer

# --------------------------
# Logging configuration
# --------------------------
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# --------------------------
# Default args
# --------------------------
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 9, 15),
    'email_on_failure': True,
    'email': ['your-email@example.com'],
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

# --------------------------
# DAG definition
# --------------------------
dag = DAG(
    'scm_transaction_dag',
    default_args=default_args,
    description='Aggregate SCM transactions and inventory from combined CSVs',
    schedule='@hourly',
    catchup=False,
)

BASE_OUTPUT_DIR = "/opt/airflow/output"

# --------------------------
# Kafka configuration
# --------------------------
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
TOPIC_AGG_REQUESTS = "scm_agg_requests"
TOPIC_AGG_INVENTORY = "scm_agg_inventory"

# --------------------------
# Helper: Push DataFrame to Kafka
# --------------------------
def push_to_kafka(df, topic):
    try:
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS.split(','),
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        for record in df.to_dict(orient='records'):
            producer.send(topic, record)
        producer.flush()
        producer.close()
        logger.info(f"Pushed {len(df)} rows to Kafka topic: {topic}")
    except Exception as e:
        logger.error(f"Error pushing to Kafka topic {topic}: {e}")

# --------------------------
# Task: Build Transaction History from combined_requested.csv
# --------------------------
def build_transaction_history(**kwargs):
    try:
        file_path = os.path.join(BASE_OUTPUT_DIR, 'combined_requested.csv')
        transactions_df = pd.read_csv(file_path)

        # Rename project column to match DAG expectations
        if 'requested_project_name' in transactions_df.columns:
            transactions_df.rename(columns={'requested_project_name': 'project_name'}, inplace=True)

        # Fill missing approval/return columns if needed
        for col in ['is_approved', 'is_requester_received', 'is_returned', 'returned_quantity']:
            if col not in transactions_df.columns:
                transactions_df[col] = 0
            else:
                transactions_df[col] = transactions_df[col].fillna(0)

        transactions_df['last_updated'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        # Save CSV for aggregation
        transaction_csv = os.path.join(BASE_OUTPUT_DIR, 'scm_transaction_history.csv')
        transactions_df.to_csv(transaction_csv, index=False, encoding='utf-8')
        logger.info(f"Transaction history saved: {transaction_csv}")

    except Exception as e:
        logger.error(f"Error in build_transaction_history: {e}")
        raise

# --------------------------
# Task: Aggregate Transactions & Push to Kafka
# --------------------------
def aggregate_transactions(**kwargs):
    try:
        transaction_csv = os.path.join(BASE_OUTPUT_DIR, 'scm_transaction_history.csv')
        df = pd.read_csv(transaction_csv)

        agg_df = df.groupby(['project_name', 'item_name', 'source'], as_index=False).agg(
            total_requested=pd.NamedAgg(column='requested_quantity', aggfunc='sum'),
            total_received=pd.NamedAgg(column='is_requester_received', aggfunc='sum'),
            total_returned=pd.NamedAgg(column='returned_quantity', aggfunc='sum'),
            total_approved=pd.NamedAgg(column='is_approved', aggfunc='sum')
        )
        agg_df['last_updated'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        # Save CSV
        agg_csv = os.path.join(BASE_OUTPUT_DIR, 'scm_transaction_aggregated.csv')
        agg_df.to_csv(agg_csv, index=False, encoding='utf-8')
        logger.info(f"Aggregated transaction data saved: {agg_csv}")

        # Push to Kafka
        push_to_kafka(agg_df, TOPIC_AGG_REQUESTS)

    except Exception as e:
        logger.error(f"Error in aggregate_transactions: {e}")
        raise

# --------------------------
# Task: Aggregate Inventory & Push to Kafka
# --------------------------
def aggregate_inventory(**kwargs):
    try:
        file_path = os.path.join(BASE_OUTPUT_DIR, 'combined_all.csv')
        inventory_df = pd.read_csv(file_path)

        # Ensure necessary columns
        for col in ['project_name', 'item_name', 'quantity', 'price']:
            if col not in inventory_df.columns:
                inventory_df[col] = 0

        # Fill missing project names
        if 'project_name' not in inventory_df.columns and 'department_id' in inventory_df.columns:
            inventory_df['project_name'] = inventory_df['department_id'].fillna('Unknown')
        else:
            inventory_df['project_name'] = inventory_df['project_name'].fillna('Unknown')

        # Aggregate inventory
        agg_inv = inventory_df.groupby(['project_name', 'item_name'], as_index=False).agg(
            current_stock=pd.NamedAgg(column='quantity', aggfunc='sum'),
            amount=pd.NamedAgg(column='price', aggfunc=lambda x: (inventory_df.loc[x.index, 'quantity'] * x).sum())
        )
        agg_inv['last_updated'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        # Save CSV
        inv_csv = os.path.join(BASE_OUTPUT_DIR, 'scm_inventory_aggregated.csv')
        agg_inv.to_csv(inv_csv, index=False, encoding='utf-8')
        logger.info(f"Aggregated inventory saved: {inv_csv}")

        # Push to Kafka
        push_to_kafka(agg_inv, TOPIC_AGG_INVENTORY)

    except Exception as e:
        logger.error(f"Error in aggregate_inventory: {e}")
        raise

# --------------------------
# Tasks
# --------------------------
build_history_task = PythonOperator(
    task_id='build_transaction_history',
    python_callable=build_transaction_history,
    dag=dag
)

aggregate_task = PythonOperator(
    task_id='aggregate_transactions',
    python_callable=aggregate_transactions,
    dag=dag
)

aggregate_inventory_task = PythonOperator(
    task_id='aggregate_inventory',
    python_callable=aggregate_inventory,
    dag=dag
)

# --------------------------
# Task Dependencies
# --------------------------
build_history_task >> [aggregate_task, aggregate_inventory_task]
