
import os
import json
import time
import streamlit as st
import pandas as pd
import plotly.express as px
from kafka import KafkaConsumer
from datetime import datetime
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Kafka configuration
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
TOPIC_REQUESTS = "scm_requests"
TOPIC_INVENTORY = "scm_inventory"

# Initialize Kafka consumers
def create_kafka_consumer(topic):
    try:
        consumer = KafkaConsumer(
            topic,
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS.split(','),
            value_deserializer=lambda x: json.loads(x.decode('utf-8')),
            auto_offset_reset='latest',  # Start from latest messages
            group_id=f'streamlit_{topic}_group',
            enable_auto_commit=True,
        )
        return consumer
    except Exception as e:
        logger.error(f"Error creating Kafka consumer for {topic}: {e}")
        st.error(f"Failed to connect to Kafka for {topic}: {e}")
        return None

# Fetch data from Kafka with timeout
def fetch_kafka_data(consumer, max_messages=100, timeout=5):
    data = []
    start_time = time.time()
    try:
        while len(data) < max_messages and (time.time() - start_time) < timeout:
            messages = consumer.poll(timeout_ms=1000)  # Poll for 1 second
            for topic_partition, partition_messages in messages.items():
                for message in partition_messages:
                    data.append(message.value)
                    if len(data) >= max_messages:
                        break
        return data
    except Exception as e:
        logger.error(f"Error fetching data from Kafka: {e}")
        st.error(f"Error fetching data from Kafka: {e}")
        return []

# Streamlit app
def main():
    st.set_page_config(
        page_title="SCM Real-Time Dashboard",
        page_icon="📊",
        layout="wide",
    )

    st.title("SCM Real-Time Dashboard")
    st.markdown("Real-time monitoring of SCM inventory and requests from Kafka topics.")

    # Initialize session state for data storage
    if 'requests_data' not in st.session_state:
        st.session_state.requests_data = []
    if 'inventory_data' not in st.session_state:
        st.session_state.inventory_data = []

    # Create Kafka consumers
    requests_consumer = create_kafka_consumer(TOPIC_REQUESTS)
    inventory_consumer = create_kafka_consumer(TOPIC_INVENTORY)

    # Placeholder for dynamic updates
    placeholder = st.empty()

    while True:
        with placeholder.container():
            # Fetch data from Kafka
            if requests_consumer:
                new_requests = fetch_kafka_data(requests_consumer)
                if new_requests:
                    st.session_state.requests_data.extend(new_requests)
                    # Limit to last 1000 records to prevent memory issues
                    st.session_state.requests_data = st.session_state.requests_data[-1000:]

            if inventory_consumer:
                new_inventory = fetch_kafka_data(inventory_consumer)
                if new_inventory:
                    st.session_state.inventory_data.extend(new_inventory)
                    st.session_state.inventory_data = st.session_state.inventory_data[-1000:]

            # Convert to DataFrames
            requests_df = pd.DataFrame(st.session_state.requests_data)
            inventory_df = pd.DataFrame(st.session_state.inventory_data)

            # Display metrics
            st.subheader("Key Metrics")
            col1, col2, col3 = st.columns(3)
            if not requests_df.empty:
                total_requests = len(requests_df)
                pending_requests = len(requests_df[requests_df['is_approved'].isna()])
                fulfilled_requests = len(requests_df[requests_df['is_requester_received'] == 1])
                with col1:
                    st.metric("Total Requests", total_requests)
                with col2:
                    st.metric("Pending Requests", pending_requests)
                with col3:
                    st.metric("Fulfilled Requests", fulfilled_requests)
            else:
                st.warning("No request data available yet.")

            if not inventory_df.empty:
                total_items = len(inventory_df)
                total_stock = inventory_df['current_stock'].sum()
                low_stock_items = len(inventory_df[inventory_df['current_stock'] < 10])
                with col1:
                    st.metric("Total Inventory Items", total_items)
                with col2:
                    st.metric("Total Stock Quantity", f"{total_stock:.0f}")
                with col3:
                    st.metric("Low Stock Items (<10)", low_stock_items)
            else:
                st.warning("No inventory data available yet.")

            # Visualizations
            st.subheader("Visualizations")
            if not inventory_df.empty:
                # Stock by Source
                fig_stock = px.bar(
                    inventory_df.groupby('source')['current_stock'].sum().reset_index(),
                    x='source',
                    y='current_stock',
                    title="Current Stock by Source",
                    labels={'current_stock': 'Stock Quantity', 'source': 'Source'},
                )
                st.plotly_chart(fig_stock, use_container_width=True)

            if not requests_df.empty:
                # Request Status Distribution
                status_counts = requests_df['is_approved'].fillna('Pending').value_counts().reset_index()
                status_counts.columns = ['Status', 'Count']
                fig_status = px.pie(
                    status_counts,
                    names='Status',
                    values='Count',
                    title="Request Status Distribution",
                )
                st.plotly_chart(fig_status, use_container_width=True)

            # Data Tables
            st.subheader("Raw Data")
            if not requests_df.empty:
                st.write("Requests Data")
                st.dataframe(requests_df[['id', 'item_name', 'requested_quantity', 'is_approved', 'last_updated']])
            if not inventory_df.empty:
                st.write("Inventory Data")
                st.dataframe(inventory_df[['id', 'item_name', 'current_stock', 'source', 'last_updated']])

        # Update every 10 seconds
        time.sleep(10)

if __name__ == "__main__":
    main()
