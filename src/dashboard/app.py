import streamlit as st
import pandas as pd
import json
import time
from kafka import KafkaConsumer
import plotly.express as px

# --- Configuration ---
st.set_page_config(
    page_title="Real-Time Weather Analytics",
    layout="wide",
    initial_sidebar_state="expanded"
)

# --- Custom CSS for Premium Look ---
st.markdown("""
<style>
    .metric-card {
        background-color: #1E1E1E;
        border-radius: 10px;
        padding: 20px;
        box-shadow: 2px 2px 10px rgba(0,0,0,0.5);
        text-align: center;
        margin-bottom: 20px;
    }
    .metric-label {
        font-size: 14px;
        color: #AAAAAA;
        margin-bottom: 5px;
    }
    .metric-value {
        font-size: 28px;
        font-weight: bold;
        color: #FFFFFF;
    }
    .metric-delta {
        font-size: 12px;
        color: #00CC66;
    }
    /* Hide Streamlit Header/Footer for cleaner look */
    #MainMenu {visibility: hidden;}
    footer {visibility: hidden;}
</style>
""", unsafe_allow_html=True)

# --- Title ---
st.title("⚡ Real-Time Weather Analytics")
st.markdown("Monitor live temperature and wind conditions from OpenWeatherMap via Kafka & Spark Streaming.")

# --- Sidebar ---
with st.sidebar:
    st.header("Control Panel")
    st.write("Fetching real-time data...")
    selected_cities = st.multiselect(
        "Filter by City",
        ["Paris", "London", "New York", "Tokyo"],
        default=["Paris", "London", "New York", "Tokyo"]
    )
    st.markdown("---")
    st.info("Status: Listening to Kafka Topic `weather_aggregates`")

# --- Initialize Kafka Consumer ---
@st.cache_resource
def init_consumer():
    return KafkaConsumer(
        "weather_aggregates",
        bootstrap_servers="localhost:9092",
        auto_offset_reset="latest",
        value_deserializer=lambda x: json.loads(x.decode("utf-8"))
    )

consumer = init_consumer()

# --- Shared State ---
if 'data' not in st.session_state:
    st.session_state.data = []

# --- Layout Container ---
dashboard_placeholder = st.empty()

def update_dashboard():
    # Poll Kafka
    messages = consumer.poll(timeout_ms=500)
    
    for tp, msgs in messages.items():
        for msg in msgs:
            st.session_state.data.append(msg.value)
            # Keep buffer size manageable
            if len(st.session_state.data) > 2000:
                st.session_state.data.pop(0)

    # Prepare Data
    if st.session_state.data:
        df = pd.DataFrame(st.session_state.data)
        df['time'] = pd.to_datetime(df['window_start'])
        
        # Filter by City
        if selected_cities:
            df = df[df['name'].isin(selected_cities)]

        if df.empty:
            with dashboard_placeholder.container():
                st.warning("No data for selected cities yet.")
            return

        # Get Latest Metrics per City
        latest_df = df.sort_values("time").groupby("name").last().reset_index()

        with dashboard_placeholder.container():
            # --- Metrics Row ---
            st.subheader("Current Conditions")
            
            if not latest_df.empty:
                cols = st.columns(len(latest_df))
                for idx, row in latest_df.iterrows():
                    with cols[min(idx, len(cols)-1)]:
                        st.metric(
                            label=row['name'],
                            value=f"{row['avg_temp']:.1f} °C",
                            delta=f"Min: {row['min_temp']:.1f} / Max: {row['max_temp']:.1f}"
                        )
            
            st.markdown("---")

            # --- Charts Row ---
            col1, col2 = st.columns([2, 1])
            
            with col1:
                st.subheader("🌡️ Temperature Trends")
                fig_line = px.line(
                    df, 
                    x="time", 
                    y="avg_temp", 
                    color="name", 
                    markers=True,
                    labels={"avg_temp": "Temperature (°C)", "time": "Time"},
                    title="Average Temperature (Windowed)"
                )
                fig_line.update_layout(xaxis_title=None)
                st.plotly_chart(fig_line, use_container_width=True, key=f"temp_chart_{time.time()}")
                
            with col2:
                st.subheader("💨 Maximum Wind Gusts")
                # Top cities by wind speed
                top_wind = df.sort_values("max_wind_speed", ascending=False).head(10)
                fig_bar = px.bar(
                    top_wind, 
                    x="name", 
                    y="max_wind_speed",
                    color="max_wind_speed",
                    color_continuous_scale="Reds",
                    labels={"max_wind_speed": "Wind Speed (m/s)"},
                    title="High Wind Alerts (Latest)"
                )
                st.plotly_chart(fig_bar, use_container_width=True, key=f"wind_chart_{time.time()}")

            # Raw Data Expander
            with st.expander("View Raw Data"):
                st.dataframe(df.sort_values("time", ascending=False).head(50))

    else:
        # Empty State
        with dashboard_placeholder.container():
            st.info("Waiting for data stream... (Ensure Producer and Spark App are running)")
            st.spinner("Listening for Kafka messages on `weather_aggregates`...")

# --- Main Loop ---
while True:
    update_dashboard()
    time.sleep(1) # Refresh interval