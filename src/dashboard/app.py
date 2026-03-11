import os
import sqlalchemy as sa
import streamlit as st
import plotly.graph_objects as go
import pandas as pd

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

engine = sa.create_engine(st.secrets["db_url"])

def build_candles_query(interval: str) -> str:
    return f"""
    SELECT
        date_trunc('{interval}', time) AS time_interval,
        (ARRAY_AGG(price ORDER BY time ASC))[1]  AS open,
        MAX(price)                               AS high,
        MIN(price)                               AS low,
        (ARRAY_AGG(price ORDER BY time DESC))[1] AS close
    FROM trades
    WHERE time BETWEEN '2025-12-01' AND '2025-12-08'
    GROUP BY time_interval
    ORDER BY time_interval;
    """

@st.cache_data(ttl=3600)
def load_candles(interval):
    query = build_candles_query(interval)
    return pd.read_sql(query, engine)

df = load_candles(interval)

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
