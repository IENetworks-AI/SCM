#!/usr/bin/env python
import os
import json
import time
import streamlit as st
import pandas as pd
import plotly.express as px
from kafka import KafkaConsumer
from datetime import datetime
import logging

# -------------------------- Logging configuration
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# -------------------------- Kafka configuration
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "scm_pipeline-kafka-1:9092")
TOPIC_REQUESTS = "scm_requests"
TOPIC_INVENTORY = "scm_inventory"
MAX_MESSAGES = 5000  # Maximum messages to keep in memory

# -------------------------- Kafka consumer
def create_kafka_consumer(topic):
    try:
        consumer = KafkaConsumer(
            topic,
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS.split(','),
            value_deserializer=lambda x: json.loads(x.decode('utf-8')),
            auto_offset_reset='earliest',
            group_id=f'streamlit_{topic}_group_{int(time.time())}',
            enable_auto_commit=True,
        )
        logger.info(f"Created consumer for topic: {topic}")
        return consumer
    except Exception as e:
        logger.error(f"Error creating Kafka consumer for {topic}: {e}")
        st.error(f"Failed to connect to Kafka for {topic}: {e}")
        return None

# -------------------------- Fetch Kafka data
def fetch_kafka_data(consumer, batch_size=1000, poll_timeout_ms=2000):
    data = []
    if not consumer:
        return data
    try:
        messages = consumer.poll(timeout_ms=poll_timeout_ms, max_records=batch_size)
        for _, partition_messages in messages.items():
            for message in partition_messages:
                try:
                    data.append(message.value)
                except Exception as e:
                    logger.error(f"Failed to parse message: {e}")
        return data
    except Exception as e:
        logger.error(f"Error fetching data from Kafka: {e}")
        st.error(f"Error fetching data from Kafka: {e}")
        return []

# -------------------------- Aggregation functions
def aggregate_transactions(df, period='W'):
    if df.empty:
        return pd.DataFrame()
    if 'requested_date' not in df.columns:
        df['requested_date'] = datetime.now()
    df['requested_date'] = pd.to_datetime(df['requested_date'], errors='coerce')
    df = df.set_index('requested_date')

    for col in ['requested_quantity','current_consumed_amount','consumed_amount','returned_quantity','amount']:
        if col not in df.columns:
            df[col] = 0

    agg = df.resample(period).agg({
        'requested_quantity': 'sum',
        'current_consumed_amount': 'sum',
        'consumed_amount': 'sum',
        'returned_quantity': 'sum',
        'amount': 'sum'
    }).reset_index()
    return agg

def ensure_column(df, col):
    if col not in df.columns:
        df[col] = None
    return df

# -------------------------- Inventory display
def prepare_inventory_display(df, view_option="Quantity"):
    if df.empty:
        return pd.DataFrame()
    
    for col in ['item_name','price','date_of_purchased','store_store_name','quantity','amount']:
        if col not in df.columns:
            df[col] = 0
    
    agg_df = df.groupby(['item_name','price','date_of_purchased','store_store_name'], as_index=False).agg({
        'amount':'sum',
        'quantity':'first'
    })
    
    status_field = 'amount' if view_option=="Amount" else 'quantity'
    
    def stock_status(row):
        if row[status_field] <= 5:
            return "🔴 Critical"
        elif row[status_field] <= 20:
            return "🟡 Low Stock"
        else:
            return "🟢 Sufficient"
    
    agg_df['Status'] = agg_df.apply(stock_status, axis=1)
    return agg_df

# -------------------------- Alert for unreturned items
def generate_unreturned_item_alert(requests_df, selected_project):
    if requests_df.empty or selected_project == "All Projects":
        return "Select a specific project to view unreturned items."
    
    project_requests = requests_df[requests_df['project_display'] == selected_project].copy()
    if project_requests.empty:
        return f"No request data for project {selected_project}."

    project_requests['returned_quantity'] = project_requests.get('returned_quantity', 0).fillna(0)
    project_requests['current_consumed_amount'] = project_requests.get('current_consumed_amount', 0).fillna(0)

    unreturned_requests = project_requests[
        (project_requests['returned_quantity'] == 0) & 
        (project_requests['current_consumed_amount'] == 0)
    ]

    if unreturned_requests.empty:
        return f"No unreturned or unconsumed items for project {selected_project}."
    
    unreturned_requests['requested_date'] = pd.to_datetime(unreturned_requests.get('requested_date', pd.Timestamp.now()), errors='coerce')
    unreturned_requests['requester_received_date'] = pd.to_datetime(unreturned_requests.get('requester_received_date', errors='coerce'), errors='coerce')
    unreturned_requests['relevant_date'] = unreturned_requests['requester_received_date'].fillna(unreturned_requests['requested_date'])

    oldest_request = unreturned_requests.loc[unreturned_requests['relevant_date'].idxmin()]
    
    requester = oldest_request.get('requester_name', 'Unknown Requester')
    item = oldest_request.get('item_name', 'Unknown Item')
    project = oldest_request.get('project_display', 'Unknown Project')
    date = oldest_request['relevant_date'].strftime('%Y-%m-%d')
    days_held = (datetime.now() - oldest_request['relevant_date']).days

    return f"⚠️ Longest unreturned/unconsumed item:\n- {requester} (Project: {project}) requested {item} on {date} ({days_held} days ago)"

# -------------------------- Streamlit App
st.set_page_config(page_title="SCM Real-Time Dashboard", page_icon="📊", layout="wide")

def main():
    st.markdown("""
        <div style="background-color:#16a34a;color:white;padding:1rem;border-radius:0.5rem;margin-bottom:1rem;">
            <h1 style="margin:0;">📦 SCM Real-Time Dashboard</h1>
            <p style="margin:0;">Monitor inventory and transactions across projects</p>
        </div>
    """, unsafe_allow_html=True)

    # Session state
    if "requests_data" not in st.session_state:
        st.session_state.requests_data = []
    if "inventory_data" not in st.session_state:
        st.session_state.inventory_data = []

    # Kafka consumers
    requests_consumer = create_kafka_consumer(TOPIC_REQUESTS)
    inventory_consumer = create_kafka_consumer(TOPIC_INVENTORY)

    # Fetch new data
    if requests_consumer:
        new_requests = fetch_kafka_data(requests_consumer, batch_size=MAX_MESSAGES)
        if new_requests:
            st.session_state.requests_data.extend(new_requests)
            st.session_state.requests_data = st.session_state.requests_data[-MAX_MESSAGES:]
        else:
            st.warning("No requests messages fetched from Kafka")
    else:
        st.warning("Requests consumer not available")

    if inventory_consumer:
        new_inventory = fetch_kafka_data(inventory_consumer, batch_size=MAX_MESSAGES)
        if new_inventory:
            st.session_state.inventory_data.extend(new_inventory)
            st.session_state.inventory_data = st.session_state.inventory_data[-MAX_MESSAGES:]
        else:
            st.warning("No inventory messages fetched from Kafka")
    else:
        st.warning("Inventory consumer not available")

    # Convert to DataFrames
    requests_df = pd.DataFrame(st.session_state.requests_data)
    inventory_df = pd.DataFrame(st.session_state.inventory_data)

    # Handle missing columns
    if 'requested_project_name' in requests_df.columns:
        requests_df['project_display'] = requests_df['requested_project_name'].fillna('').astype(str).str.strip()
    else:
        requests_df['project_display'] = requests_df.get('project_display', '')
    
    if 'department_id' in inventory_df.columns:
        inventory_df['project_display'] = inventory_df['department_id'].fillna('').astype(str).str.strip()
    else:
        inventory_df['project_display'] = inventory_df.get('project_display', '')

    # Ensure extra fields
    new_fields = ['returned_date', 'is_requester_received', 'requester_received_date',
                  'current_consumed_amount', 'consumed_amount', 'is_approved', 'approved_date']
    for col in new_fields:
        requests_df = ensure_column(requests_df, col)
        inventory_df = ensure_column(inventory_df, col)

    # Sidebar filters
    with st.sidebar:
        st.markdown("## Project Filters")
        selected_project_inventory = st.selectbox("Inventory Project", ["All Projects"] + sorted(inventory_df['project_display'].unique().tolist()))
        selected_project_usage = st.selectbox("Usage Project", ["All Projects"] + sorted(requests_df['project_display'].unique().tolist()))

    # Main layout
    col_left, col_right = st.columns(2)

    # Inventory
    with col_left:
        st.subheader("Inventory Analysis")
        view_option = st.selectbox("View Stock By:", ["Quantity", "Amount"], key="inventory_view")
        inventory_filtered = inventory_df if selected_project_inventory=="All Projects" else inventory_df[inventory_df['project_display']==selected_project_inventory]
        inventory_display = prepare_inventory_display(inventory_filtered, view_option)
        if not inventory_display.empty:
            st.dataframe(inventory_display)
        else:
            st.info("No inventory data available.")

    # Usage analytics
    with col_right:
        st.subheader("Usage Analytics")
        requests_filtered = requests_df if selected_project_usage=="All Projects" else requests_df[requests_df['project_display']==selected_project_usage]
        trans_agg = aggregate_transactions(requests_filtered)
        if not trans_agg.empty:
            pie_fig = px.pie(
                trans_agg.melt(value_vars=['requested_quantity','current_consumed_amount','returned_quantity'],
                               var_name='Transaction', value_name='Count'),
                names='Transaction', values='Count'
            )
            st.plotly_chart(pie_fig, use_container_width=True)
        else:
            st.info("No transaction data available.")

        # Alert
        alert_msg = generate_unreturned_item_alert(requests_df, selected_project_usage)
        if alert_msg.startswith("⚠️"):
            st.error(alert_msg)
        else:
            st.success(alert_msg)

if __name__ == "__main__":
    main()
