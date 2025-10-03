#!/usr/bin/env python
import os
import json
import time
import streamlit as st
import pandas as pd
import plotly.express as px
from kafka import KafkaConsumer
from datetime import datetime, timedelta
import logging
import requests
from google import genai

# -------------------------- Logging configuration
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# -------------------------- Determine Docker environment
def running_in_docker():
    return os.path.exists('/.dockerenv')

# -------------------------- Kafka configuration
if running_in_docker():
    KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_INTERNAL", "kafka:9092")
else:
    external_host = os.getenv("EXTERNAL_KAFKA_HOST", "localhost")
    external_port = os.getenv("EXTERNAL_KAFKA_PORT", "29092")
    KAFKA_BOOTSTRAP_SERVERS = f"{external_host}:{external_port}"

TOPIC_REQUESTS = "scm_requests"
TOPIC_INVENTORY = "scm_inventory"
MAX_MESSAGES = 5000

# -------------------------- Gemini API
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")
client = genai.Client(api_key=GEMINI_API_KEY) if GEMINI_API_KEY else None

# -------------------------- Helper Functions
def get_recent_data(df, months=3):
    if df.empty or 'requested_date' not in df.columns:
        return pd.DataFrame()
    df['requested_date'] = pd.to_datetime(df['requested_date'], errors='coerce')
    cutoff = datetime.now() - timedelta(days=30*months)
    return df[df['requested_date'] >= cutoff]

def filter_by_timeframe(df, user_question):
    if df.empty or 'requested_date' not in df.columns:
        return pd.DataFrame(), "No data"
    df['requested_date'] = pd.to_datetime(df['requested_date'], errors='coerce')
    today = datetime.now().date()

    if "last week" in user_question.lower():
        start = today - timedelta(days=today.weekday() + 7)
        end = start + timedelta(days=6)
        return df[(df['requested_date'].dt.date >= start) & (df['requested_date'].dt.date <= end)], f"{start} to {end}"

    elif "last month" in user_question.lower():
        first_day_this_month = today.replace(day=1)
        last_day_last_month = first_day_this_month - timedelta(days=1)
        first_day_last_month = last_day_last_month.replace(day=1)
        return df[(df['requested_date'].dt.date >= first_day_last_month) & (df['requested_date'].dt.date <= last_day_last_month)], f"{first_day_last_month} to {last_day_last_month}"

    else:
        cutoff = today - timedelta(days=90)
        return df[df['requested_date'].dt.date >= cutoff], "last 3 months"

def get_recent_items(df, project=None, months_list=[3,6,9,12], min_items=1):
    for months in months_list:
        cutoff = datetime.now() - timedelta(days=30*months)
        recent_df = df[df['requested_date'] >= cutoff]
        if project:
            recent_df = recent_df[recent_df['project_display'] == project]
        items = recent_df['item_name'].dropna().value_counts().index.tolist()
        if len(items) >= min_items:
            return items, months
    items = df['item_name'].dropna().value_counts().index.tolist()
    return items, months_list[-1]

def ask_chatbot(user_question, requests_df, max_retries=3, client=None):
    """
    Improved ask_chatbot:
    - Handles greetings and small talk with short replies.
    - Adds Inventory Status intent to handle 'critical state' queries.
    - Uses Gemini (client) when available with a tightly-scoped prompt.
    - Falls back to local insight text when model not used/available.
    """

    # Basic guards
    if not user_question or not str(user_question).strip():
        return "Please ask a valid question about inventory, usage, or forecasts."

    q = str(user_question).strip()
    q_lower = q.lower()

    # 1) Small talk / greetings -> short friendly reply (do not run analysis)
    import re
    if re.search(r"\b(hi|hello|hey|good morning|good afternoon|good evening|how are you|what's up)\b", q_lower):
        return "Hi! 👋 I can summarize usage, show unreturned items, or predict next-week demand. Try: 'Top items last month' or 'Predict next week demand for Cement in PROJECT_X'."

    # 2) Help / capabilities
    if re.search(r"\b(help|what can you do|capabilities|how to use)\b", q_lower):
        return ("I can:\n"
                "- Summarize requests over a timeframe (e.g. 'summary for last month')\n"
                "- List top requested items (e.g. 'top 5 items last 3 months')\n"
                "- Identify unreturned items for a project (e.g. 'unreturned items for PROJECT_X')\n"
                "- Forecast next-week demand for an item (e.g. 'predict next week demand for Cement in PROJECT_X')\n"
                "- Identify high-demand items (e.g. 'what items are in critical state')")

    # Helper: safe access to columns
    def has_col(df, col):
        return df is not None and hasattr(df, "columns") and col in df.columns

    # Try to detect project name token appearing in requests_df.project_display
    project_name = None
    if has_col(requests_df, "project_display"):
        tokens = re.findall(r"[A-Za-z0-9_-]+", q)
        projects = set(requests_df['project_display'].dropna().astype(str).unique())
        for t in tokens:
            if t in projects or t.upper() in projects:
                project_name = t if t in projects else t.upper()
                break

    # Try to detect item name by substring match (case-insensitive)
    item_name = None
    if has_col(requests_df, "item_name"):
        items = requests_df['item_name'].dropna().astype(str).unique().tolist()
        for it in items:
            if it and it.lower() in q_lower:
                item_name = it
                break

    # Intent detection
    forecast_keywords = ["next week", "next-week", "next month", "forecast", "predict", "prediction", "required"]
    unreturned_keywords = ["unreturned", "not returned", "still with", "longest unreturned", "unconsumed"]
    summary_keywords = ["summary", "top", "most requested", "top requested", "trend", "analysis", "how many", "total"]
    inventory_status_keywords = ["critical", "low stock", "stock status", "needed", "urgent"]

    is_forecast = any(k in q_lower for k in forecast_keywords)
    is_unreturned = any(k in q_lower for k in unreturned_keywords)
    is_summary = any(k in q_lower for k in summary_keywords)
    is_inventory_status = any(k in q_lower for k in inventory_status_keywords)

    # --- INVENTORY STATUS branch (Handles "what items are in critical state") ---
    if is_inventory_status:
        # Since we only have 'requests' data, we define 'critical' as 'highest recent demand'.
        period_df, analysis_window = filter_by_timeframe(requests_df, "last 3 months")
        if period_df is None or getattr(period_df, "empty", True):
             return f"No recent request data available to assess item status."

        if 'item_name' not in period_df.columns:
            return "Cannot determine critical items: 'item_name' column missing."

        # Ensure quantities are numeric
        period_df['requested_quantity'] = pd.to_numeric(period_df.get('requested_quantity', pd.Series(dtype=float)), errors='coerce').fillna(0)

        top_items = period_df['item_name'].value_counts().nlargest(5)
        top_items_text = ", ".join([f"{i} ({c})" for i, c in top_items.items()])

        response = (
            f"I do not have current **stock levels** to determine a true 'critical state'. "
            f"However, based on the **highest request volume** in the {analysis_window} window, "
            f"the items in highest demand (and thus potentially low on stock) are: "
            f"**{top_items_text}**. Recommendation: **Verify stock levels** for these top items immediately."
        )
        return response
    # --------------------------------------------------------------------------

    # FORECAST branch
    if is_forecast:
        # If user specified an item -> attempt ML API via client or fallback to local insight
        if item_name:
            if client:
                try:
                    prompt = (
                        "You are a concise forecasting assistant for inventory. INPUT: CSV rows below contain "
                        "columns project_display,item_name,requested_quantity,requested_date. TASK: produce a single-line "
                        "Forecast for the named item with the numeric predicted next-week quantity and a one-sentence rationale. "
                        "If data is insufficient, reply: 'INSUFFICIENT_DATA'.\n\n"
                        f"ITEM: {item_name}\n"
                        f"PROJECT: {project_name or 'ALL'}\n\n"
                        "DATA:\n"
                        f"{requests_df[['project_display','item_name','requested_quantity','requested_date']].to_csv(index=False)}\n\n"
                        "REPLY (exact format):\n"
                        "Forecast: <number> units — <one-sentence rationale>\n"
                    )
                    response = client.models.generate_content(
                        model="gemini-1.5-flash",
                        contents=prompt
                    )
                    text = getattr(response, "text", None) or str(response)
                    text = text.strip()
                    if "INSUFFICIENT_DATA" in text.upper() or not text:
                        raise ValueError("Model reported insufficient data or empty response")
                    return text
                except Exception as e:
                    logger.warning(f"Gemini forecast call failed or returned no useful result: {e}. Falling back to local forecast.")

            # Local heuristic fallback: average weekly demand from recent weeks
            try:
                df_local = requests_df.copy() if requests_df is not None else pd.DataFrame()
                if has_col(df_local, "requested_date") and has_col(df_local, "requested_quantity"):
                    df_local['requested_date'] = pd.to_datetime(df_local['requested_date'], errors='coerce')
                    df_local = df_local.dropna(subset=['requested_date'])
                    if project_name:
                        df_local = df_local[df_local.get('project_display', '') == project_name]
                    df_local = df_local[df_local.get('item_name', '') == item_name]
                    if df_local.empty:
                        return f"Insufficient historical data to predict next-week demand for '{item_name}'."
                    recent = datetime.now() - timedelta(weeks=12)
                    df_local = df_local[df_local['requested_date'] >= recent]
                    if df_local.empty:
                        return f"Insufficient recent historical data to predict next-week demand for '{item_name}'."
                    df_local['requested_quantity'] = pd.to_numeric(df_local['requested_quantity'], errors='coerce').fillna(0)
                    weekly = df_local.set_index('requested_date')['requested_quantity'].resample('W-MON').sum()
                    if weekly.empty:
                        return f"Insufficient weekly aggregation data for '{item_name}'."
                    pred = float(weekly.mean())
                    return f"Forecast: {pred:.1f} units — based on the average weekly requests over the last {len(weekly)} weeks."
                else:
                    return "Insufficient data: 'requested_date' or 'requested_quantity' column missing."
            except Exception as e:
                logger.warning(f"Local forecast failed: {e}")
                return "Unable to produce a forecast due to insufficient or malformed data."

        # No item specified -> provide top likely items next week
        else:
            items, months_used = get_recent_items(requests_df, project=project_name)
            if not items:
                return "Insufficient historical data to forecast items for next week."
            top_list = ", ".join(items[:5])
            if project_name:
                return f"Based on the last {months_used} months, project '{project_name}' is likely to request: {top_list} next week."
            return f"Based on the last {months_used} months, the likely items next week are: {top_list}."

    # UNRETURNED items branch
    if is_unreturned:
        if not project_name:
            return "Please specify the project to check unreturned items (e.g. 'unreturned items for PROJECT_X')."
        return generate_unreturned_item_alert(requests_df, project_name)

    # SUMMARY / ANALYSIS branch
    # Triggered by explicit keywords OR a long query that is NOT a forecast, status, or unreturned request.
    if is_summary or (len(q.split()) > 3 and not is_inventory_status):
        period_df, analysis_window = filter_by_timeframe(requests_df, q)
        if period_df is None or getattr(period_df, "empty", True):
            return f"No data available for the requested period ({analysis_window})."

        period_df['requested_quantity'] = pd.to_numeric(period_df.get('requested_quantity', pd.Series(dtype=float)), errors='coerce').fillna(0)

        total_requested = int(period_df['requested_quantity'].sum())
        avg_qty = float(period_df['requested_quantity'].mean()) if not period_df.empty else 0.0
        top_items = period_df['item_name'].value_counts().head(5)
        top_items_text = ", ".join([f"{i} ({c})" for i, c in top_items.items()])

        # Try Gemini for a short executive summary if client available
        if client:
            try:
                # Refined prompt: explicitly ask for calculation of previous trend
                prompt = (
                    "You are a concise inventory analyst. INPUT: CSV rows below contain project_display,item_name,requested_quantity,requested_date. "
                    "TASK: Produce a short executive summary (max 3 lines):\n"
                    "1) Headline: State the total requested quantity for the period and calculate the trend (increase/decrease/stable) compared to the *immediately preceding* period of the same length, based on 'requested_quantity' sum.\n"
                    "2) Top Items: List the top 3 items and their total requested counts for this period.\n"
                    "3) Recommendation: Provide one short, actionable recommendation based on the data.\n"
                    "If data insufficient, reply 'NO_DATA'.\n\n"
                    f"PERIOD: {analysis_window}\n"
                    f"DATA:\n{period_df[['project_display','item_name','requested_quantity','requested_date']].to_csv(index=False)}\n\n"
                    "REPLY (plain text, max 3 lines):"
                )
                response = client.models.generate_content(
                    model="gemini-1.5-flash",
                    contents=prompt
                )
                text = getattr(response, "text", None) or str(response)
                text = text.strip()
                if not text or text.upper().startswith("NO_DATA"):
                    raise ValueError("Model returned no usable summary")
                return text
            except Exception as e:
                logger.warning(f"Gemini summary call failed: {e}. Falling back to local summary.")
                pass # Fallback to local summary below

        # Local summary fallback
        try:
            # compute previous window trend (More robust calculation)
            trend = "stable"
            try:
                window_start = pd.to_datetime(period_df['requested_date']).min()
                window_end = pd.to_datetime(period_df['requested_date']).max()
                window_days = (window_end - window_start).days or 1

                prev_end = window_start - timedelta(days=1)
                prev_start = prev_end - timedelta(days=window_days)

                requests_df['requested_date'] = pd.to_datetime(requests_df.get('requested_date'), errors='coerce')
                requests_df['requested_quantity'] = pd.to_numeric(requests_df.get('requested_quantity'), errors='coerce').fillna(0)

                prev_df = requests_df[
                    (requests_df['requested_date'] >= prev_start) &
                    (requests_df['requested_date'] <= prev_end)
                ]
                prev_total = int(prev_df['requested_quantity'].sum()) if not prev_df.empty else 0

                if total_requested > prev_total * 1.10: # 10% increase threshold
                    trend = "significant increase"
                elif total_requested < prev_total * 0.90: # 10% decrease threshold
                    trend = "significant decrease"
                elif total_requested > prev_total:
                     trend = "slight increase"
                elif total_requested < prev_total:
                     trend = "slight decrease"
                else:
                    trend = "stable"
            except Exception:
                trend = "stable (comparison failed)"

            response = (
                f"Summary ({analysis_window}): Total requested = {total_requested}, Avg per request = {avg_qty:.2f}. "
                f"Top items: {top_items_text if top_items_text else 'No items'}. Trend vs previous window: **{trend}**. "
                "Recommendation: **Review the trend**; if increasing, **verify stock for top items** and set reorder thresholds."
            )
            return response
        except Exception as e:
            logger.warning(f"Local summary failed: {e}")
            return "Unable to produce a summary due to an internal error."

    # ITEM-specific fallback: user asked about a single item explicitly (e.g., "Tell me about Cement")
    if item_name:
        try:
            item_df = requests_df[requests_df.get('item_name', '') == item_name].copy()
            if item_df.empty:
                return f"No historical requests found for item '{item_name}'."

            if 'requested_date' not in item_df.columns:
                 return f"Item '{item_name}' found, but missing 'requested_date' for analysis."
            item_df['requested_date'] = pd.to_datetime(item_df['requested_date'], errors='coerce')
            item_df = item_df.dropna(subset=['requested_date'])

            if item_df.empty:
                 return f"Item '{item_name}' found, but all dates were invalid."

            item_df['requested_quantity'] = pd.to_numeric(item_df.get('requested_quantity', pd.Series(dtype=float)), errors='coerce').fillna(0)

            last_request = item_df.sort_values('requested_date', ascending=False).iloc[0]
            total_requested = int(item_df['requested_quantity'].sum())

            recent_avg = None
            try:
                recent = datetime.now() - timedelta(weeks=12)
                weekly = item_df[item_df['requested_date'] >= recent].set_index('requested_date')['requested_quantity'].resample('W-MON').sum()
                recent_avg = float(weekly.mean()) if not weekly.empty and weekly.sum() > 0 else None
            except Exception:
                recent_avg = None

            response = (
                f"Item **'{item_name}'** — Total requested (all time): **{total_requested} units**. "
                f"Most recent request on **{pd.to_datetime(last_request['requested_date']).date()}**."
            )
            if recent_avg is not None:
                response += f" Recent average weekly demand is **~{recent_avg:.1f} units** (based on last 12 weeks)."
            return response
        except Exception as e:
            logger.warning(f"Item-specific lookup failed: {e}")
            return "Unable to fetch item details due to internal error."

    # If nothing matched
    return ("I didn't understand exactly what you'd like. Try one of these options:\n"
            "- 'Give me a summary for last month'\n"
            "- 'Predict next week demand for Cement in PROJECT_X'\n"
            "- 'Show unreturned items for PROJECT_Y'\n"
            "- 'What items are in critical state?'")


def create_kafka_consumer(topic):
    try:
        # Create a consumer without committing offsets so each app run can
        # read from the earliest available messages. Subscribe and wait for
        # partition assignment, then seek to beginning to ensure we read
        # historical messages.
        consumer = KafkaConsumer(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS.split(','),
            value_deserializer=lambda x: json.loads(x.decode('utf-8')),
            auto_offset_reset='earliest',
            enable_auto_commit=False,
            consumer_timeout_ms=1000,
        )
        consumer.subscribe([topic])

        # Wait briefly for partition assignment
        assign_timeout = time.time() + 5
        while not consumer.assignment() and time.time() < assign_timeout:
            consumer.poll(timeout_ms=200)

        if consumer.assignment():
            try:
                consumer.seek_to_beginning(*list(consumer.assignment()))
            except Exception:
                # Not fatal; continue and let poll return available records
                pass

        logger.info(f"Created consumer for topic: {topic} (Bootstrap: {KAFKA_BOOTSTRAP_SERVERS}) Assignment: {consumer.assignment()}")
        return consumer
    except Exception as e:
        logger.error(f"Error creating Kafka consumer for {topic}: {e}")
        st.error(f"Failed to connect to Kafka for {topic}: {e}")
        return None

def fetch_kafka_data(consumer, batch_size=1000, poll_timeout_ms=2000):
    data = []
    if not consumer:
        return data
    try:
        # Attempt to ensure we read from the earliest available offsets for
        # this consumer instance. Seek to beginning if we have an assignment.
        try:
            if consumer.assignment():
                consumer.seek_to_beginning(*list(consumer.assignment()))
        except Exception:
            # ignore seek failures and continue
            pass

        total = 0
        # Poll in a few short iterations to allow the client to fetch messages
        # especially right after subscribing/assignment.
        for _ in range(3):
            messages = consumer.poll(timeout_ms=poll_timeout_ms, max_records=batch_size)
            batch_count = 0
            for _, partition_messages in messages.items():
                for message in partition_messages:
                    try:
                        data.append(message.value)
                        batch_count += 1
                    except Exception as e:
                        logger.error(f"Failed to parse message: {e}")
            total += batch_count
            logger.debug(f"Polled batch {_}: {batch_count} messages")
            if total >= 1:
                break
        logger.info(f"Fetched {total} messages from Kafka topic (requested up to {batch_size})")
        return data
    except Exception as e:
        logger.error(f"Error fetching data from Kafka: {e}")
        st.error(f"Error fetching data from Kafka: {e}")
        return []

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

def prepare_inventory_display(df, view_option="Quantity"):
    if df.empty:
        return pd.DataFrame(), {}
    for col in ['item_name','price','date_of_purchased','store_store_name','quantity','amount']:
        if col not in df.columns:
            df[col] = 0
    agg_df = df.groupby(['item_name','price','date_of_purchased','store_store_name'], as_index=False).agg({'amount':'sum','quantity':'first'})
    status_field = 'amount' if view_option=="Amount" else 'quantity'
    def stock_status(row):
        if row[status_field] <= 5:
            return "🔴 Critical"
        elif row[status_field] <= 20:
            return "🟡 Low Stock"
        else:
            return "🟢 Sufficient"
    agg_df['Status'] = agg_df.apply(stock_status, axis=1)
    summary = {
        "Critical": (agg_df['Status'] == "🔴 Critical").sum(),
        "Low Stock": (agg_df['Status'] == "🟡 Low Stock").sum(),
        "Sufficient": (agg_df['Status'] == "🟢 Sufficient").sum(),
        "Total Items": len(agg_df),
        "Total Price (Birr)": agg_df['amount'].sum()
    }
    return agg_df, summary

def generate_unreturned_item_alert(requests_df, selected_project):
    if requests_df.empty or selected_project == "All Projects":
        return "Select a specific project to view unreturned items."
    project_requests = requests_df[requests_df['project_display'] == selected_project].copy()
    if project_requests.empty:
        return f"No request data for project {selected_project}."
    project_requests['returned_quantity'] = project_requests.get('returned_quantity', 0).fillna(0)
    project_requests['current_consumed_amount'] = project_requests.get('current_consumed_amount', 0).fillna(0)
    unreturned_requests = project_requests[(project_requests['returned_quantity'] == 0) & 
                                           (project_requests['current_consumed_amount'] == 0)]
    if unreturned_requests.empty:
        return f"No unreturned or unconsumed items for project {selected_project}."
    unreturned_requests['requested_date'] = pd.to_datetime(unreturned_requests.get('requested_date', pd.Timestamp.now()), errors='coerce')
    unreturned_requests['requester_received_date'] = pd.to_datetime(unreturned_requests.get('requester_received_date', pd.Timestamp.now()), errors='coerce')
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
ML_API_URL = os.getenv("ML_API_URL", "http://scm_ml-api:8001")

def call_prediction_api(project_name, item_name):
    try:
        payload = {
            "project_name": project_name,
            "item_name": item_name,
            "requested_date": datetime.now().strftime("%Y-%m-%d"),
            "in_use": 1
        }
        response = requests.post(f"{ML_API_URL}/predict", json=payload, timeout=10)
        if response.status_code == 200:
            return response.json().get("predicted_quantity", None)
        else:
            st.error(f"Prediction API error: {response.text}")
            return None
    except Exception as e:
        st.error(f"Failed to call ML API: {e}")
        return None

def main():
    st.markdown("""
        <div style="background-color:#16a34a;color:white;padding:1rem;border-radius:0.5rem;margin-bottom:1rem;">
            <h1 style="margin:0;">📦 SCM Real-Time Dashboard</h1>
            <p style="margin:0;">Monitor inventory and transactions across projects</p>
        </div>
    """, unsafe_allow_html=True)

    if "requests_data" not in st.session_state:
        st.session_state.requests_data = []
    if "inventory_data" not in st.session_state:
        st.session_state.inventory_data = []

    requests_consumer = create_kafka_consumer(TOPIC_REQUESTS)
    inventory_consumer = create_kafka_consumer(TOPIC_INVENTORY)

    if requests_consumer:
        new_requests = fetch_kafka_data(requests_consumer, batch_size=MAX_MESSAGES)
        if new_requests:
            st.session_state.requests_data.extend(new_requests)
            st.session_state.requests_data = st.session_state.requests_data[-MAX_MESSAGES:]
    if inventory_consumer:
        new_inventory = fetch_kafka_data(inventory_consumer, batch_size=MAX_MESSAGES)
        if new_inventory:
            st.session_state.inventory_data.extend(new_inventory)
            st.session_state.inventory_data = st.session_state.inventory_data[-MAX_MESSAGES:]

    requests_df = pd.DataFrame(st.session_state.requests_data)
    inventory_df = pd.DataFrame(st.session_state.inventory_data)

    if 'requested_project_name' in requests_df.columns:
        requests_df['project_display'] = requests_df['requested_project_name'].fillna('').astype(str).str.strip()
    else:
        requests_df['project_display'] = pd.Series([''] * len(requests_df))

    if 'department_id' in inventory_df.columns:
        inventory_df['project_display'] = inventory_df['department_id'].fillna('').astype(str).str.strip()
    else:
        inventory_df['project_display'] = pd.Series([''] * len(inventory_df))

    new_fields = ['returned_date', 'is_requester_received', 'requester_received_date',
                  'current_consumed_amount', 'consumed_amount', 'is_approved', 'approved_date']
    for col in new_fields:
        requests_df = ensure_column(requests_df, col)
        inventory_df = ensure_column(inventory_df, col)

    with st.sidebar:
        st.markdown("## Project Filters")
        selected_project_inventory = st.selectbox(
            "Inventory Project",
            ["All Projects"] + sorted(inventory_df['project_display'].unique().tolist())
        )
        selected_project_usage = st.selectbox(
            "Usage Project",
            ["All Projects"] + sorted(requests_df['project_display'].unique().tolist())
        )

    col_left, col_right = st.columns(2)

    with col_left:
        st.subheader("Inventory Analysis")
        view_option = st.selectbox("View Stock By:", ["Quantity", "Amount"], key="inventory_view")
        inventory_filtered = inventory_df if selected_project_inventory == "All Projects" else inventory_df[inventory_df['project_display'] == selected_project_inventory]
        inventory_display, summary = prepare_inventory_display(inventory_filtered, view_option)
        if not inventory_display.empty:
            st.markdown(f"""
              **📊 Inventory Summary ({selected_project_inventory})**
              - 🔴 Critical Items: **{summary['Critical']}**
              - 🟡 Low Stock Items: **{summary['Low Stock']}**
              - 🟢 Sufficient Items: **{summary['Sufficient']}**
              - 📦 Total Items: **{summary['Total Items']}**
              - 💰 Total Price: **{summary['Total Price (Birr)']:,.2f} birr**
            """)
            st.dataframe(inventory_display)
        else:
            st.info("No inventory data available.")

    with col_right:
        st.subheader("Usage Analytics")
        requests_filtered = requests_df if selected_project_usage == "All Projects" else requests_df[requests_df['project_display'] == selected_project_usage]
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

        alert_msg = generate_unreturned_item_alert(requests_df, selected_project_usage)
        if alert_msg.startswith("⚠️"):
            st.error(alert_msg)
        else:
            st.success(alert_msg)

        # st.subheader("🔮 Demand Prediction")
        # available_projects = sorted(requests_df['project_display'].dropna().unique().tolist())
        # project_choice = st.selectbox("Select Project", available_projects if available_projects else ["No projects available"], key="predict_project")

        # if project_choice and project_choice != "No projects available":
        #     project_items = requests_df[requests_df['project_display'] == project_choice]['item_name'].dropna().unique().tolist()
        #     available_items = sorted(project_items)
        # else:
        #     available_items = []

        # item_choice = st.selectbox("Select Item", available_items if available_items else ["No items available"], key="predict_item")

        # if st.button("Predict Next Week Demand"):
        #     if project_choice not in ["", "No projects available"] and item_choice not in ["", "No items available"]:
        #         prediction = call_prediction_api(project_choice, item_choice)
        #         if prediction is not None:
        #             st.info(f"📈 Predicted next week quantity for {item_choice} (Project: {project_choice}): **{prediction:.2f}**")
        #     else:
        #         st.warning("Select both project and item for prediction.")

    # st.subheader("💬 Ask SCM Chatbot")
    st.subheader("💬 Ask SCM Chatbot")
    user_question = st.text_area("Enter your question about inventory or forecast:")
    if st.button("Ask Chatbot"):
        response = ask_chatbot(user_question, requests_df, client=client)
        st.markdown(f"**Response:**\n{response}")

if __name__ == "__main__":
    main()
