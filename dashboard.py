import sqlite3
import pandas as pd
import streamlit as st
import plotly.express as px

# 1. Page Configuration
st.set_page_config(
    page_title="Real-Time Crypto Dashboard",
    page_icon="📈",
    layout="wide"
)

st.title("🚀 Real-Time Crypto Streaming Dashboard")
st.markdown("Pipeline Architecture: `Yahoo Finance` ➔ `Kafka Producer` ➔ `Kafka Topic` ➔ `Kafka Consumer` ➔ `SQLite Database` ➔ `Streamlit`")

# 2. Database Connection and Data Loading Function
# We use st.cache_data with a short TTL (time-to-live) or direct querying to fetch fresh data
DB_NAME = "crypto.db"

def load_data():
    try:
        conn = sqlite3.connect(DB_NAME)
        # Query all records from our crypto table
        df = pd.read_sql("SELECT * FROM crypto_prices ORDER BY timestamp DESC", conn)
        conn.close()
        return df
    except Exception as e:
        st.error(f"Error loading data from database: {e}")
        return pd.DataFrame()

# Load the data
df = load_data()

if df.empty:
    st.warning("⚠️ No data found in the database yet. Make sure your `producer.py` and `db_consumer.py` are running!")
else:
    # 3. Sidebar Controls & Filters
    st.sidebar.header("Dashboard Filters")
    symbols = df["symbol"].unique().tolist()
    selected_symbol = st.sidebar.selectbox("Select Crypto Symbol", symbols)
    
    # Filter dataframe by selected symbol
    filtered_df = df[df["symbol"] == selected_symbol]
    
    # 4. Top Metrics Display
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

    # 5. Interactive Charts (Plotly)
    st.subheader(f"Price Trend History: {selected_symbol}")
    
    # Sort chronologically for the line chart
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

    # 6. Raw Data Table View
    with st.expander("🔍 View Raw Streaming Data Table"):
        st.dataframe(filtered_df, use_container_width=True)

    # 7. Auto-refresh button for real-time feel
    if st.button("🔄 Refresh Dashboard Data"):
        st.rerun()