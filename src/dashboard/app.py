import sqlalchemy as sa
import streamlit as st
import plotly.graph_objects as go
import pandas as pd
from datetime import datetime, timedelta

INTERVAL_OPTIONS = {
        "Hourly": "hour",
        "Daily": "day",
        "Weekly": "week",
        "Monthly": "month",
}

interval_label = st.selectbox(
        "Select time interval",
        options=list(INTERVAL_OPTIONS.keys()),
        index=0,
)

interval = INTERVAL_OPTIONS[interval_label]

RANGE_OPTIONS = {
    "Last 24 hours": timedelta(hours=24),
    "Last 7 days": timedelta(days=7),
    "Last 30 days": timedelta(days=30),
    "Last 90 days": timedelta(days=90),
}

range_label = st.selectbox(
        "Time range", 
        list(RANGE_OPTIONS.keys()),
        index=0
)
delta = RANGE_OPTIONS[range_label]

end_time = datetime.utcnow()
start_time = end_time - delta

with st.expander("Custom range"):
    start_time = st.date_input("Start date")
    end_time = st.date_input("End date")

engine = sa.create_engine(st.secrets["db_url"])

def build_candles_query(
        interval: str,
        start_time: datetime,
        end_time: datetime
) -> str:
    return f"""
    SELECT
        date_trunc('{interval}', time) AS time_interval,
        (ARRAY_AGG(price ORDER BY time ASC))[1]  AS open,
        MAX(price)                               AS high,
        MIN(price)                               AS low,
        (ARRAY_AGG(price ORDER BY time DESC))[1] AS close
    FROM trades
    WHERE time BETWEEN '{start_time}' AND '{end_time}'
    GROUP BY time_interval
    ORDER BY time_interval;
    """

@st.cache_data(ttl=3600)
def load_candles(interval, start_time, end_time):
    query = build_candles_query(interval, start_time, end_time)
    return pd.read_sql(query, engine)

df = load_candles(interval, start_time, end_time)

fig = go.Figure(
        data=[
            go.Candlestick(
                x=df["time_interval"],
                open=df["open"],
                high=df["high"],
                low=df["low"],
                close=df["close"]
                )
        ]
)

st.subheader(f"{interval_label} Candlesticks")
st.plotly_chart(fig)
