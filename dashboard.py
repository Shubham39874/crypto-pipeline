import sqlite3
import pandas as pd
import streamlit as st
import plotly.express as px
import time

# 1. Page Configuration
st.set_page_config(
    page_title="Real-Time Crypto Dashboard",
    page_icon="📈",
    layout="wide"
)

st.title("🚀 Real-Time Crypto Streaming Dashboard")
st.markdown("Pipeline Architecture: `Yahoo Finance` ➔ `Kafka Producer` ➔ `Kafka Topic` ➔ `Kafka Consumer` ➔ `Lakehouse (SQLite)` ➔ `Streamlit`")

# 2. Database Connection and Data Loading
DB_NAME = "crypto.db"

def load_data():
    try:
        conn = sqlite3.connect(DB_NAME)
        # Query from our Silver or Monitored lakehouse table
        df = pd.read_sql("SELECT * FROM silver_crypto ORDER BY timestamp DESC", conn)
        conn.close()
        return df
    except Exception as e:
        # Fallback to base table if silver table isn't present yet
        try:
            conn = sqlite3.connect(DB_NAME)
            df = pd.read_sql("SELECT symbol, price, timestamp FROM crypto_prices ORDER BY timestamp DESC", conn)
            conn.close()
            return df
        except:
            return pd.DataFrame()

# Load the data
df = load_data()

# Sidebar controls for Auto-Refresh
st.sidebar.header("Dashboard Controls")
auto_refresh = st.sidebar.checkbox("Enable Auto-Refresh (Every 5s)", value=True)

if df.empty:
    st.warning("⚠️ No data found in database yet. Ensure your `producer.py` and lakehouse consumer are running!")
else:
    # Sidebar Symbol Filter
    symbols = df["symbol"].unique().tolist()
    selected_symbol = st.sidebar.selectbox("Select Crypto Symbol", symbols)
    
    filtered_df = df[df["symbol"] == selected_symbol]
    
    # Top Metrics Display
    latest_price = filtered_df["price"].iloc[0]
    previous_price = filtered_df["price"].iloc[1] if len(filtered_df) > 1 else latest_price
    price_change = latest_price - previous_price
    
    st.subheader(f"Live Metrics for {selected_symbol}")
    col1, col2, col3 = st.columns(3)
    
    col1.metric(
        label=f"{selected_symbol} Current Price",
        value=f"${latest_price:,.2f}",
        delta=f"${price_change:,.2f}"
    )
    col2.metric(
        label="Total Records Captured",
        value=len(df)
    )
    col3.metric(
        label="Active Monitored Coins",
        value=len(symbols)
    )
    
    st.markdown("---")

    # Interactive Plotly Chart
    st.subheader(f"Price Trend History: {selected_symbol}")
    chart_df = filtered_df.sort_values("timestamp")
    
    fig = px.line(
        chart_df, 
        x="timestamp", 
        y="price", 
        markers=True,
        title=f"{selected_symbol} Price Over Time"
    )
    fig.update_layout(xaxis_title="Timestamp", yaxis_title="Price (USD)")
    st.plotly_chart(fig, use_container_width=True)

    # Raw Data Table
    with st.expander("🔍 View Raw Streaming Data Table"):
        st.dataframe(filtered_df, use_container_width=True)

# Handle Auto-refresh timer
if auto_refresh:
    time.sleep(5)
    st.rerun()