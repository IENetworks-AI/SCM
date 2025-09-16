import os
import json
import csv
import pandas as pd
from typing import Dict, List, Any
from datetime import datetime
from kafka import KafkaProducer
from airflow import DAG
from airflow.operators.python import PythonOperator
import logging
from datetime import datetime, timedelta

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
    'scm_transform_dag',
    default_args=default_args,
    description='Transform SCM JSON to CSV and stream to Kafka',
    schedule='@hourly',
    catchup=False,
)

BASE_OUTPUT_DIR = "/opt/airflow/output"

# --- Asset Transformation ---
NESTED_OBJECTS_ASSET = {
    "store": ["id", "store_name", "location", "project_id", "store_keeper_id", "description", "is_permanent", 
              "created_by", "updated_by", "is_deleted", "created_at", "updated_at"],
    "uom": ["id", "name", "is_countable", "created_by", "updated_by", "is_deleted", "description", "created_at", "updated_at"],
    "currency": ["id", "name", "code", "symbol", "format", "exchange_rate", "active", "created_at", "updated_at", "is_offshore"],
    "department": [],
    "manufacturer": [],
    "category": [],
    "asset_user": []
}

def flatten_asset_item(item):
    flattened = {}
    for key, value in item.items():
        if key not in NESTED_OBJECTS_ASSET:
            flattened[key] = value
    flattened["is_fixed_asset"] = 1
    for nested_obj, fields in NESTED_OBJECTS_ASSET.items():
        nested_data = item.get(nested_obj, {}) if item.get(nested_obj) is not None else {}
        for field in fields:
            flattened[f"{nested_obj}_{field}"] = nested_data.get(field, None)
    return flattened

def transform_assets():
    columns_to_keep = ["id", "item_name", "price", "amount", "is_consumable", "department_id", 
                       "date_of_purchased", "description", "store_store_name", "uom_name", "quantity"]
    try:
        with open(os.path.join(BASE_OUTPUT_DIR, "fixedasset.json"), "r", encoding="utf-8") as f:
            data = json.load(f)
        flattened_data = [flatten_asset_item(item) for item in data["data"]]
        df = pd.DataFrame(flattened_data)
        if 'id' in df.columns:
            df['id'] = df['id'].astype(str)
        for col in columns_to_keep:
            if col not in df.columns:
                df[col] = pd.NA
                logger.warning(f"Column '{col}' was missing in assets and filled with NaN.")
        df = df[columns_to_keep]
        df = df.dropna(subset=["department_id", "quantity"], how="all")
        df.to_csv(os.path.join(BASE_OUTPUT_DIR, "cleaned_assets.csv"), index=False, encoding="utf-8")
        logger.info("Process completed: fixedasset.json flattened, converted to CSV, columns filtered, and saved as 'cleaned_assets.csv'.")
    except Exception as e:
        logger.error(f"An error occurred in transform_assets: {e}")

# --- Tool Transformation ---
NESTED_OBJECTS_TOOL = {
    "store": ["id", "store_name", "location", "project_id", "store_keeper_id", "description", "is_permanent", 
              "created_by", "updated_by", "is_deleted", "created_at", "updated_at"],
    "status": ["id", "status_name", "created_by", "updated_by", "is_deleted", "description", "created_at", "updated_at"],
    "uom": ["id", "name", "is_countable", "created_by", "updated_by", "is_deleted", "description", "created_at", "updated_at"],
    "currency": ["id", "name", "code", "symbol", "format", "exchange_rate", "active", "created_at", "updated_at", "is_offshore"],
    "department": [],
    "manufacturer": [],
    "category": []
}

def flatten_tool_item(item):
    flattened = {}
    for key, value in item.items():
        if key not in NESTED_OBJECTS_TOOL and key != "inventory_user":
            flattened[key] = value
    for nested_obj, fields in NESTED_OBJECTS_TOOL.items():
        nested_data = item.get(nested_obj, {}) if item.get(nested_obj) is not None else {}
        for field in fields:
            flattened[f"{nested_obj}_{field}"] = nested_data.get(field, None)
    return flattened

def transform_tools():
    columns_to_keep = ["id", "item_name", "price", "amount", "is_consumable", "department_id", 
                       "date_of_purchased", "description", "store_store_name", "uom_name", "quantity"]
    try:
        with open(os.path.join(BASE_OUTPUT_DIR, "tools.json"), "r", encoding="utf-8") as f:
            data = json.load(f)
        flattened_data = [flatten_tool_item(item) for item in data["data"]]
        df = pd.DataFrame(flattened_data)
        if 'id' in df.columns:
            df['id'] = df['id'].astype(str)
        for col in columns_to_keep:
            if col not in df.columns:
                df[col] = pd.NA
                logger.warning(f"Column '{col}' was missing in tools and filled with NaN.")
        df = df[columns_to_keep]
        df = df.dropna(subset=["department_id", "quantity"], how="all")
        df.to_csv(os.path.join(BASE_OUTPUT_DIR, "cleaned_tools.csv"), index=False, encoding="utf-8")
        logger.info("Process completed: tools.json flattened, converted to CSV, columns filtered, and saved as 'cleaned_tools.csv'.")
    except Exception as e:
        logger.error(f"An error occurred in transform_tools: {e}")

# --- Inventory Transformation ---
NESTED_OBJECTS_INVENTORY = {
    "type": ["id", "inventory_type", "created_by", "updated_by", "is_deleted", "description", "created_at", "updated_at"],
    "project": ["id", "project_name", "budget", "currency_id", "contract_sign_date", "client_id", "start_date", 
                "lc_opening_date", "advance_payment_date", "end_date", "forex_resource", "isActive", "milestone_amount", 
                "isDeleted", "system_id", "contract_payment", "forex_contract_payment", "forex_contract_payment_currency", 
                "is_office", "created_by", "updated_by", "created_at", "updated_at", "plannedStart", "plannedFinish", 
                "startVariance", "finishVariance", "actualStart", "actualFinish", "start", "finish", "duration", 
                "actualDuration", "isOpportunity", "sector_id", "businessUnitId"],
    "store": ["id", "store_name", "location", "project_id", "store_keeper_id", "description", "is_permanent", 
              "created_by", "updated_by", "is_deleted", "created_at", "updated_at"],
    "status": ["id", "status_name", "created_by", "updated_by", "is_deleted", "description", "created_at", "updated_at"],
    "uom": ["id", "name", "is_countable", "created_by", "updated_by", "is_deleted", "description", "created_at", "updated_at"],
    "currency": ["id", "name", "code", "symbol", "format", "exchange_rate", "active", "created_at", "updated_at", "is_offshore"],
    "department": [],
    "manufacturer": [],
    "category": []
}

def flatten_inventory_item(item):
    flattened = {}
    for key, value in item.items():
        if key not in NESTED_OBJECTS_INVENTORY and key != "inventory_user":
            flattened[key] = value
    for nested_obj, fields in NESTED_OBJECTS_INVENTORY.items():
        nested_data = item.get(nested_obj, {}) if item.get(nested_obj) is not None else {}
        for field in fields:
            flattened[f"{nested_obj}_{field}"] = nested_data.get(field, None)
    return flattened

def transform_inventory():
    columns_to_keep = ["id", "item_name", "price", "amount", "is_consumable", "department_id", 
                       "date_of_purchased", "description", "store_store_name", "uom_name", "quantity",]
    try:
        with open(os.path.join(BASE_OUTPUT_DIR, "index.json"), "r", encoding="utf-8") as f:
            data = json.load(f)
        flattened_data = [flatten_inventory_item(item) for item in data["data"]]
        df = pd.DataFrame(flattened_data)
        if 'id' in df.columns:
            df['id'] = df['id'].astype(str)
        for col in columns_to_keep:
            if col not in df.columns:
                df[col] = pd.NA
                logger.warning(f"Column '{col}' was missing in inventory and filled with NaN.")
        df = df[columns_to_keep]
        df = df.dropna(subset=["department_id", "quantity"], how="all")
        df.to_csv(os.path.join(BASE_OUTPUT_DIR, "cleaned_inventory.csv"), index=False, encoding="utf-8")
        logger.info("Process completed: index.json flattened, converted to CSV, columns filtered, and saved as 'cleaned_inventory.csv'.")
    except Exception as e:
        logger.error(f"An error occurred in transform_inventory: {e}")

# --- Requested Tool Transformation ---
def flatten_requested_tool(data):
    desired_columns = ['id', 'item_name', 'requested_date', 'requested_project_name', 'requested_quantity',
                       'requester_id', 'requester_name', 'requester_received_date', 'status_name',
                       'store_name', 'tool_id', 'uom_name','is_returned','current_consumed_amount','returned_quantity']
    flattened_data = []
    if 'data' not in data:
        return flattened_data
    for item in data['data']:
        flattened_item = {key: str(item.get(key, '')) if key == 'id' or key == 'tool_id' else item.get(key, '') for key in desired_columns}
        flattened_data.append(flattened_item)
    return flattened_data

def transform_requested_tool():
    try:
        with open(os.path.join(BASE_OUTPUT_DIR, 'requested.json'), 'r', encoding='utf-8') as file:
            data = json.load(file)
        flattened_data = flatten_requested_tool(data)
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        csv_file = os.path.join(BASE_OUTPUT_DIR, f'flattened_requested_{timestamp}.csv')
        fieldnames = ['id', 'item_name', 'requested_date', 'requested_project_name', 'requested_quantity',
                      'requester_id', 'requester_name', 'requester_received_date', 'status_name',
                      'store_name', 'tool_id', 'uom_name','is_returned','current_consumed_amount','returned_quantity']
        with open(csv_file, 'w', newline='', encoding='utf-8') as file:
            writer = csv.DictWriter(file, fieldnames=fieldnames)
            writer.writeheader()
            for item in flattened_data:
                writer.writerow(item)
        logger.info(f"Flattened requested tool data saved to {csv_file}")
        return csv_file
    except Exception as e:
        logger.error(f"An error occurred in transform_requested_tool: {e}")
        return None

# --- Requested Inventory Transformation ---
def flatten_requested_inventory(data):
    desired_columns = ['id', 'item_name', 'requested_date', 'requested_project_name', 'requested_quantity',
                       'requester_id', 'requester_name', 'store_name', 'tool_id', 'uom_name','requester_received_date', 'status_name','is_returned','current_consumed_amount','returned_quantity']
    flattened_data = []
    if 'data' not in data:
        return flattened_data
    for item in data['data']:
        flattened_item = {
            'id': str(item.get('id', '')),
            'item_name': item.get('item_name', ''),
            'requested_date': item.get('requested_date', ''),
            'requested_project_name': item.get('requested_project_name', ''),
            'requested_quantity': item.get('requested_quantity', ''),
            'requester_id': item.get('requester_id', ''),
            'requester_name': item.get('name', ''),
            'store_name': item.get('store_name', ''),
            'tool_id': str(item.get('inventory_id', '')),
            'uom_name': item.get('uom_name', ''),
            'is_returned': item.get('is_returned', ''),
            'current_consumed_amount': item.get('current_consumed_amount', ''),
            #'consumed_amount': item.get('consumed_amount', ''),
            'status_name': item.get('status_name', ''),
            'returned_quantity': item.get('returned_quantity', ''),
            'requester_received_date': item.get('requester_received_date', '')
        }
        flattened_data.append(flattened_item)
    return flattened_data

def transform_requested_inventory():
    try:
        with open(os.path.join(BASE_OUTPUT_DIR, 'inventories.json'), 'r', encoding='utf-8') as file:
            data = json.load(file)
        flattened_data = flatten_requested_inventory(data)
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        csv_file = os.path.join(BASE_OUTPUT_DIR, f'flattened_inventories_{timestamp}.csv')
        fieldnames = ['id', 'item_name', 'requested_date', 'requested_project_name', 'requested_quantity',
                      'requester_id', 'requester_name', 'store_name', 'tool_id', 'uom_name','requester_received_date', 'status_name','is_returned','current_consumed_amount','returned_quantity']
        with open(csv_file, 'w', newline='', encoding='utf-8') as file:
            writer = csv.DictWriter(file, fieldnames=fieldnames)
            writer.writeheader()
            for item in flattened_data:
                writer.writerow(item)
        logger.info(f"Flattened requested inventory data saved to {csv_file}")
        return csv_file
    except Exception as e:
        logger.error(f"An error occurred in transform_requested_inventory: {e}")
        return None

# --- Approved Data Transformation ---


# --- Combine CSVs and Produce to Kafka ---
def combine_and_produce(**kwargs):
    try:
        requested_tool_csv = transform_requested_tool()
        requested_inventory_csv = transform_requested_inventory()
     
        if not requested_tool_csv or not requested_inventory_csv:
            logger.error("Failed to generate requested tool or inventory CSV.")
            return

        # Combine requested files
        requested_tool_df = pd.read_csv(requested_tool_csv)
        inventories_requested_df = pd.read_csv(requested_inventory_csv)
        requested_tool_df['id'] = requested_tool_df['id'].astype(str)
        requested_tool_df['tool_id'] = requested_tool_df['tool_id'].astype(str)
        inventories_requested_df['id'] = inventories_requested_df['id'].astype(str)
        inventories_requested_df['tool_id'] = inventories_requested_df['tool_id'].astype(str)
        requested_tool_df['source'] = 'tools'
        inventories_requested_df['source'] = 'inventory'
        requested_tool_df = requested_tool_df.dropna(subset=['requested_date', 'requested_quantity','item_name'])
        inventories_requested_df = inventories_requested_df.dropna(subset=['requested_date', 'requested_quantity','item_name'])
        requested_tool_df['status_name'] = requested_tool_df['status_name'].fillna("Good")
        requested_tool_df['requester_received_date'] = requested_tool_df['requester_received_date'].fillna(requested_tool_df['requested_date'])
        requested_tool_df['requested_project_name'] = requested_tool_df['requested_project_name'].fillna("non project item")
        requested_tool_df['returned_quantity'] = requested_tool_df['returned_quantity'].fillna(0)
        inventories_requested_df['returned_quantity'] = inventories_requested_df['returned_quantity'].fillna(0)


        inventories_requested_df['status_name'] = inventories_requested_df['status_name'].fillna("Good")
        inventories_requested_df['requester_received_date'] = inventories_requested_df['requester_received_date'].fillna(inventories_requested_df['requested_date'])
        inventories_requested_df['requested_project_name'] = inventories_requested_df['requested_project_name'].fillna("non project item")
        requested_tool_df = requested_tool_df.drop(columns=['storekeeper_received_date', 'tool_type'], errors='ignore')
        inventories_requested_df = inventories_requested_df.drop(columns=['storekeeper_received_date', 'tool_type'], errors='ignore')
        combined_requested_df = pd.concat([requested_tool_df, inventories_requested_df], ignore_index=True)
        combined_requested_df = combined_requested_df.drop_duplicates()
       

        combined_requested_df.to_csv(os.path.join(BASE_OUTPUT_DIR, 'combined_requested.csv'), index=False)
        logger.info("Combined requested files with fields saved as 'combined_requested.csv'.")

        # Combine assets, tools, inventory
        assets_df = pd.read_csv(os.path.join(BASE_OUTPUT_DIR, 'cleaned_assets.csv'))
        tools_df = pd.read_csv(os.path.join(BASE_OUTPUT_DIR, 'cleaned_tools.csv'))
        inventory_df = pd.read_csv(os.path.join(BASE_OUTPUT_DIR, 'cleaned_inventory.csv'))
        assets_df['id'] = assets_df['id'].astype(str)
        tools_df['id'] = tools_df['id'].astype(str)
        inventory_df['id'] = inventory_df['id'].astype(str)
        assets_df['source'] = 'asset'
        tools_df['source'] = 'tools'
        inventory_df['source'] = 'inventory'
        assets_df = assets_df.dropna(subset=['quantity','item_name'])
        tools_df = tools_df.dropna(subset=['quantity','item_name'])
        inventory_df = inventory_df.dropna(subset=['quantity'])
        combined_all_df = pd.concat([assets_df, tools_df, inventory_df], ignore_index=True)
        combined_all_df = combined_all_df.drop_duplicates()

        combined_all_df.to_csv(os.path.join(BASE_OUTPUT_DIR, 'combined_all.csv'), index=False)
        logger.info("Combined all files with current_stock saved as 'combined_all.csv'.")

        # Produce to Kafka
        kafka_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
        producer = KafkaProducer(bootstrap_servers=kafka_servers.split(','), value_serializer=lambda v: json.dumps(v).encode('utf-8'))
        for _, row in combined_requested_df.iterrows():
            producer.send('scm_requests', row.to_dict())
        for _, row in combined_all_df.iterrows():
            producer.send('scm_inventory', row.to_dict())
        producer.flush()
        logger.info("Streamed combined_requested.csv and combined_all.csv to Kafka topics 'scm_requests' and 'scm_inventory'.")
    except Exception as e:
        logger.error(f"An error occurred in combine_and_produce: {e}")

# Tasks
transform_assets_task = PythonOperator(task_id='transform_assets', python_callable=transform_assets, dag=dag)
transform_tools_task = PythonOperator(task_id='transform_tools', python_callable=transform_tools, dag=dag)
transform_inventory_task = PythonOperator(task_id='transform_inventory', python_callable=transform_inventory, dag=dag)
combine_and_produce_task = PythonOperator(task_id='combine_and_produce', python_callable=combine_and_produce, dag=dag)

# Task dependencies
[transform_assets_task, transform_tools_task, transform_inventory_task] >> combine_and_produce_task