import os
import json
import time
import streamlit as st
import pandas as pd
import plotly.express as px
from kafka import KafkaConsumer
from datetime import datetime
import logging

# --------------------------
# Logging configuration
# --------------------------
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# --------------------------
# Kafka configuration
# --------------------------
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "scm_pipeline-kafka-1:9092")

TOPIC_REQUESTS = "scm_requests"
TOPIC_INVENTORY = "scm_inventory"

MAX_MESSAGES = 1000  # Keep last 1000 messages in memory

# --------------------------
# Kafka consumer
# --------------------------
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
        return consumer
    except Exception as e:
        logger.error(f"Error creating Kafka consumer for {topic}: {e}")
        st.error(f"Failed to connect to Kafka for {topic}: {e}")
        return None

# --------------------------
# Fetch Kafka data
# --------------------------
def fetch_kafka_data(consumer, batch_size=1000, poll_timeout_ms=500):
    data = []
    try:
        while True:
            messages = consumer.poll(timeout_ms=poll_timeout_ms, max_records=batch_size)
            if not messages:
                break
            for _, partition_messages in messages.items():
                for message in partition_messages:
                    data.append(message.value)
        return data
    except Exception as e:
        logger.error(f"Error fetching data from Kafka: {e}")
        st.error(f"Error fetching data from Kafka: {e}")
        return []

# --------------------------
# Aggregation functions
# --------------------------
def aggregate_inventory(df):
    if df.empty:
        return df
    cols = ['item_name', 'date_of_purchased', 'price', 'description', 'quantity', 'amount', 'current_stock']
    for col in cols:
        if col not in df.columns:
            df[col] = 0
    agg_df = df.groupby(['item_name', 'date_of_purchased', 'price', 'description'], as_index=False).agg({
        'quantity': 'sum',
        'amount': 'sum',
        'current_stock': 'sum'
    })
    return agg_df

def aggregate_transactions(df, period='W'):
    if df.empty:
        return pd.DataFrame()
    if 'last_updated' not in df.columns:
        df['last_updated'] = datetime.now()
    df['last_updated'] = pd.to_datetime(df['last_updated'])
    df = df.set_index('last_updated')
    for col in ['requested_quantity', 'quantity_consumed', 'quantity_returned', 'amount']:
        if col not in df.columns:
            df[col] = 0
    agg = df.resample(period).agg({
        'requested_quantity': 'sum',
        'quantity_consumed': 'sum',
        'quantity_returned': 'sum',
        'amount': 'sum'
    }).reset_index()
    return agg

def ensure_column(df, col):
    if col not in df.columns:
        df[col] = None
    return df

# --------------------------
# Streamlit app
# --------------------------
def main():
    st.set_page_config(page_title="SCM Real-Time Dashboard", page_icon="📊", layout="wide")
    st.title("📦 SCM Real-Time Dashboard")
    st.markdown("Project-wise monitoring of SCM inventory and transactions.")

    # --------------------------
    # Session state
    # --------------------------
    if "requests_data" not in st.session_state:
        st.session_state.requests_data = []
    if "inventory_data" not in st.session_state:
        st.session_state.inventory_data = []

    # --------------------------
    # Kafka consumers
    # --------------------------
    requests_consumer = create_kafka_consumer(TOPIC_REQUESTS)
    inventory_consumer = create_kafka_consumer(TOPIC_INVENTORY)

    # --------------------------
    # Fetch new data
    # --------------------------
    if requests_consumer:
        new_requests = fetch_kafka_data(requests_consumer)
        if new_requests:
            st.session_state.requests_data.extend(new_requests)
            st.session_state.requests_data = st.session_state.requests_data[-MAX_MESSAGES:]

    if inventory_consumer:
        new_inventory = fetch_kafka_data(inventory_consumer)
        if new_inventory:
            st.session_state.inventory_data.extend(new_inventory)
            st.session_state.inventory_data = st.session_state.inventory_data[-MAX_MESSAGES:]

    # --------------------------
    # Convert to DataFrames
    # --------------------------
    requests_df = pd.DataFrame(st.session_state.requests_data)
    inventory_df = pd.DataFrame(st.session_state.inventory_data)

    # --------------------------
    # Ensure project columns exist (robust)
    # --------------------------
    possible_request_cols = ['requested_project_name', 'project_name', 'project']
    possible_inventory_cols = ['department_id', 'store_store_name', 'store_name']

    request_proj_col = next((c for c in possible_request_cols if c in requests_df.columns), None)
    inventory_proj_col = next((c for c in possible_inventory_cols if c in inventory_df.columns), None)

    if request_proj_col:
        requests_df[request_proj_col] = requests_df[request_proj_col].fillna('').astype(str).str.strip()
    else:
        requests_df['requested_project_name'] = ''
        request_proj_col = 'requested_project_name'

    if inventory_proj_col:
        inventory_df[inventory_proj_col] = inventory_df[inventory_proj_col].fillna('').astype(str).str.strip()
    else:
        inventory_df['department_id'] = ''
        inventory_proj_col = 'department_id'

    # --------------------------
    # Combine unique project names
    # --------------------------
    all_projects = pd.concat([requests_df[request_proj_col], inventory_df[inventory_proj_col]])
    all_projects = all_projects[all_projects != ''].unique().tolist()

    # --------------------------
    # Project selection
    # --------------------------
    selected_project = st.selectbox("Select Project", ["All Projects"] + all_projects)

    if selected_project != "All Projects":
        if not requests_df.empty:
            requests_df = requests_df[requests_df[request_proj_col] == selected_project]
        if not inventory_df.empty:
            inventory_df = inventory_df[inventory_df[inventory_proj_col] == selected_project]

    # --------------------------
    # Aggregate inventory
    # --------------------------
    inventory_agg = aggregate_inventory(inventory_df)

    st.subheader("Inventory Analysis")

    view_option = st.selectbox("View Available Stock By:", ["Quantity", "Amount"])

    if not inventory_agg.empty:
        display_df = inventory_df.copy()

        # Compute Available
        display_df['Available'] = display_df['current_stock'] if view_option=="Quantity" else display_df['amount']

        # Compute stock status
        def stock_status(row):
            if row['Available'] <= 5:
                return "🔴 Critical"
            elif row['Available'] <= 20:
                return "🟡 Low Stock"
            else:
                return "🟢 Sufficient"
        display_df['Status'] = display_df.apply(stock_status, axis=1)

        # Ensure columns exist
        for col in ['department_id','amount','date_of_purchased']:
            if col not in display_df.columns:
                display_df[col] = None

        display_df = display_df.sort_values(by='Available', ascending=False)

        total_value = display_df['amount'].sum()
        critical_count = (display_df['Status']=="🔴 Critical").sum()
        low_count = (display_df['Status']=="🟡 Low Stock").sum()
        sufficient_count = (display_df['Status']=="🟢 Sufficient").sum()

        # Metrics cards
        col1, col2, col3, col4 = st.columns(4)
        col1.metric("Critical Items", critical_count)
        col2.metric("Low Stock", low_count)
        col3.metric("Sufficient", sufficient_count)
        col4.metric("Total Value", f"${total_value:,.2f}")

        # Searchable inventory table
        search_query = st.text_input("Search items...")
        if search_query:
            display_df = display_df[display_df['item_name'].str.contains(search_query, case=False)]

        st.dataframe(display_df[['item_name','Available','Status','department_id','amount','date_of_purchased']])

    else:
        st.write("No inventory data available.")

    # --------------------------
    # Usage analytics pie chart
    # --------------------------
    st.subheader("Usage Analytics")
    trans_agg = aggregate_transactions(requests_df)
    if not trans_agg.empty:
        pie_fig = px.pie(
            trans_agg.melt(value_vars=['requested_quantity','quantity_consumed','quantity_returned'],
                            var_name='Transaction', value_name='Count'),
            names='Transaction',
            values='Count',
            color='Transaction',
            color_discrete_map={
                'requested_quantity': 'blue',
                'quantity_consumed': 'green',
                'quantity_returned': 'orange'
            },
            title="Requested / Consumed / Returned"
        )
        st.plotly_chart(pie_fig, use_container_width=True)
    else:
        st.write("No transaction data available.")

if __name__ == "__main__":
    main()
