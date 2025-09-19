import os
import json
import time
import streamlit as st
import pandas as pd
import plotly.express as px
from kafka import KafkaConsumer
from datetime import datetime, timedelta
import logging

# --------------------------  # Logging configuration
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# --------------------------  # Kafka configuration
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "scm_pipeline-kafka-1:9092")

TOPIC_REQUESTS = "scm_requests"
TOPIC_INVENTORY = "scm_inventory"

MAX_MESSAGES = 5000  # Increased to handle more data without truncation

# --------------------------  # Kafka consumer
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

# --------------------------  # Fetch Kafka data
def fetch_kafka_data(consumer, batch_size=1000, poll_timeout_ms=2000):
    data = []
    try:
        messages = consumer.poll(timeout_ms=poll_timeout_ms, max_records=batch_size)
        for _, partition_messages in messages.items():
            for message in partition_messages:
                try:
                    data.append(message.value)
                    logger.info(f"Polled message id={message.value.get('id')}")
                except Exception as e:
                    logger.error(f"Failed to parse message: {e}")
        logger.info(f"Fetched {len(data)} messages from topic {list(consumer.topics())[0] if consumer.topics() else 'unknown'}")
        return data
    except Exception as e:
        logger.error(f"Error fetching data from Kafka: {e}")
        st.error(f"Error fetching data from Kafka: {e}")
        return []

# --------------------------  # Aggregation functions
def aggregate_transactions(df, period='W'):
    if df.empty:
        return pd.DataFrame()
    if 'requested_date' not in df.columns:
        df['requested_date'] = datetime.now()
    df['requested_date'] = pd.to_datetime(df['requested_date'])
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

# --------------------------  # Inventory aggregation for display
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

# --------------------------  # Generate alert for unreturned/unconsumed item
def generate_unreturned_item_alert(requests_df, selected_project):
    if requests_df.empty or selected_project == "All Projects":
        return "Select a specific project to view unreturned items."
    
    # Filter requests for the selected project
    project_requests = requests_df[requests_df['project_display'] == selected_project].copy()
    
    if project_requests.empty:
        return f"No request data for project {selected_project}."
    
    # Identify items not returned or consumed
    project_requests['returned_quantity'] = project_requests['returned_quantity'].fillna(0)
    project_requests['current_consumed_amount'] = project_requests['current_consumed_amount'].fillna(0)
    unreturned_requests = project_requests[
        (project_requests['returned_quantity'] == 0) & 
        (project_requests['current_consumed_amount'] == 0)
    ]
    
    if unreturned_requests.empty:
        return f"No unreturned or unconsumed items for project {selected_project}."
    
    # Convert dates
    unreturned_requests['requested_date'] = pd.to_datetime(unreturned_requests['requested_date'], errors='coerce')
    unreturned_requests['requester_received_date'] = pd.to_datetime(unreturned_requests['requester_received_date'], errors='coerce')
    unreturned_requests['relevant_date'] = unreturned_requests['requester_received_date'].fillna(unreturned_requests['requested_date'])
    
    # Find the oldest request
    oldest_request = unreturned_requests.loc[unreturned_requests['relevant_date'].idxmin()]
    
    requester = oldest_request.get('requester_name', 'Unknown Requester')
    item = oldest_request['item_name']
    project = oldest_request['project_display']
    date = oldest_request['relevant_date'].strftime('%Y-%m-%d')
    days_held = (datetime.now() - oldest_request['relevant_date']).days
    
    return f"⚠️ Longest unreturned/unconsumed item:\n- {requester} (Project: {project}) requested {item} on {date} ({days_held} days ago)"

# --------------------------  # Streamlit app
def main():
    st.set_page_config(page_title="SCM Real-Time Dashboard", page_icon="📊", layout="wide")
    
    # Add Tailwind CSS
    st.markdown("""
        <link href="https://cdn.jsdelivr.net/npm/tailwindcss@2.2.19/dist/tailwind.min.css" rel="stylesheet">
    """, unsafe_allow_html=True)

    # Header
    st.markdown("""
        <div class="bg-green-600 text-white p-4 rounded-lg mb-4">
            <h1 class="text-3xl font-bold">📦 SCM Real-Time Dashboard</h1>
            <p class="text-lg">Monitor inventory and transactions across projects</p>
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
        new_requests = fetch_kafka_data(requests_consumer)
        if new_requests:
            st.session_state.requests_data.extend(new_requests)
            st.session_state.requests_data = st.session_state.requests_data[-MAX_MESSAGES:]
            logger.info(f"Total requests data points after update: {len(st.session_state.requests_data)}")
    else:
        st.warning("No requests consumer available - check Kafka connection.")

    if inventory_consumer:
        new_inventory = fetch_kafka_data(inventory_consumer)
        if new_inventory:
            st.session_state.inventory_data.extend(new_inventory)
            st.session_state.inventory_data = st.session_state.inventory_data[-MAX_MESSAGES:]
            logger.info(f"Total inventory data points after update: {len(st.session_state.inventory_data)}")
    else:
        st.warning("No inventory consumer available - check Kafka connection.")

    # Convert to DataFrames
    requests_df = pd.DataFrame(st.session_state.requests_data)
    inventory_df = pd.DataFrame(st.session_state.inventory_data)

    if requests_df.empty:
        st.warning("Requests DataFrame is empty - no usage data available.")
    if inventory_df.empty:
        st.warning("Inventory DataFrame is empty - no inventory data available.")

    # Project column setup
    if 'requested_project_name' in requests_df.columns:
        requests_df['project_display'] = requests_df['requested_project_name'].fillna('').astype(str).str.strip()
    else:
        requests_df['project_display'] = ''
        logger.warning("requested_project_name column missing in requests_df")

    if 'department_id' in inventory_df.columns:
        inventory_df['project_display'] = inventory_df['department_id'].fillna('').astype(str).str.strip()
    else:
        inventory_df['project_display'] = ''
        logger.warning("department_id column missing in inventory_df")

    # Get unique projects
    requests_projects = sorted(requests_df['project_display'][requests_df['project_display'] != ''].unique().tolist())
    inventory_projects = sorted(inventory_df['project_display'][inventory_df['project_display'] != ''].unique().tolist())
    
    logger.info(f"Requests projects count: {len(requests_projects)}")
    logger.info(f"Inventory projects count: {len(inventory_projects)}")

    # Ensure new DAG fields exist
    new_fields = ['returned_date', 'is_requester_received', 'requester_received_date',
                  'current_consumed_amount', 'consumed_amount', 'is_approved', 'approved_date']
    for col in new_fields:
        requests_df = ensure_column(requests_df, col)
        inventory_df = ensure_column(inventory_df, col)

    # Sidebar for project selection
    with st.sidebar:
        st.markdown("<h2 class='text-xl font-semibold text-blue-600'>Project Filters</h2>", unsafe_allow_html=True)
        selected_project_inventory = st.selectbox("Inventory Project", ["All Projects"] + inventory_projects, key="inventory_project")
        selected_project_usage = st.selectbox("Usage Project", ["All Projects"] + requests_projects, key="usage_project")

    # Main layout
    col_left, col_right = st.columns(2)

    # Inventory Analysis (Left)
    with col_left:
        st.markdown("<h2 class='text-2xl font-semibold text-gray-800'>Inventory Analysis</h2>", unsafe_allow_html=True)
        view_option = st.selectbox("View Stock By:", ["Quantity", "Amount"], key="inventory_view")

        if selected_project_inventory != "All Projects":
            inventory_df_filtered = inventory_df[inventory_df['project_display'] == selected_project_inventory].copy()
        else:
            inventory_df_filtered = inventory_df.copy()

        inventory_display = prepare_inventory_display(inventory_df_filtered, view_option)

        if not inventory_display.empty:
            critical_count = (inventory_display['Status']=="🔴 Critical").sum()
            low_count = (inventory_display['Status']=="🟡 Low Stock").sum()
            sufficient_count = (inventory_display['Status']=="🟢 Sufficient").sum()
            total_value = inventory_display['price'].sum()  # Changed to sum of price in Birr

            c1, c2, c3, c4 = st.columns(4)
            with c1:
                st.markdown(f"<div class='bg-red-100 p-3 rounded-lg'><p class='text-sm font-medium text-red-800'>Critical Items</p><p class='text-lg font-bold'>{critical_count}</p></div>", unsafe_allow_html=True)
            with c2:
                st.markdown(f"<div class='bg-yellow-100 p-3 rounded-lg'><p class='text-sm font-medium text-yellow-800'>Low Stock</p><p class='text-lg font-bold'>{low_count}</p></div>", unsafe_allow_html=True)
            with c3:
                st.markdown(f"<div class='bg-green-100 p-3 rounded-lg'><p class='text-sm font-medium text-green-800'>Sufficient</p><p class='text-lg font-bold'>{sufficient_count}</p></div>", unsafe_allow_html=True)
            with c4:
                st.markdown(f"<div class='bg-blue-100 p-3 rounded-lg'><p class='text-sm font-medium text-blue-800'>Total Value (Birr)</p><p class='text-lg font-bold'>{total_value:,.2f}</p></div>", unsafe_allow_html=True)

            search_query = st.text_input("Search items...", key="inventory_search")
            if search_query:
                inventory_display = inventory_display[inventory_display['item_name'].str.contains(search_query, case=False, na=False)]

            display_cols = ['item_name','quantity','amount','Status','price','store_store_name']
            st.dataframe(inventory_display[display_cols].sort_values(by='amount', ascending=False), use_container_width=True)

        else:
            st.markdown("<p class='text-gray-600'>No inventory data available.</p>", unsafe_allow_html=True)

    # Usage Analytics (Right)
    with col_right:
        st.markdown("<h2 class='text-2xl font-semibold text-gray-800'>Usage Analytics</h2>", unsafe_allow_html=True)
        
        if selected_project_usage != "All Projects":
            requests_df_filtered = requests_df[requests_df['project_display'] == selected_project_usage].copy()
        else:
            requests_df_filtered = requests_df.copy()

        trans_agg = aggregate_transactions(requests_df_filtered)
        if not trans_agg.empty:
            pie_fig = px.pie(
                trans_agg.melt(
                    value_vars=['requested_quantity','current_consumed_amount','returned_quantity'],
                    var_name='Transaction', value_name='Count'
                ),
                names='Transaction',
                values='Count',
                color='Transaction',
                color_discrete_map={
                    'requested_quantity': 'green',
                    'current_consumed_amount': 'blue',
                    'returned_quantity': 'orange'
                },
                title="Requested / Consumed / Returned"
            )
            st.plotly_chart(pie_fig, use_container_width=True)
        else:
            st.markdown("<p class='text-gray-600'>No transaction data available.</p>", unsafe_allow_html=True)

        # Alert for unreturned/unconsumed item
        alert_msg = generate_unreturned_item_alert(requests_df, selected_project_usage)
        if alert_msg.startswith("⚠️"):
            st.markdown(f"<div class='bg-red-50 border-l-4 border-red-500 p-4 rounded-lg'><p class='text-red-700'>{alert_msg}</p></div>", unsafe_allow_html=True)
        else:
            st.markdown(f"<div class='bg-blue-50 border-l-4 border-blue-500 p-4 rounded-lg'><p class='text-blue-700'>{alert_msg}</p></div>", unsafe_allow_html=True)

if __name__ == "__main__":
    main()