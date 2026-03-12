import sqlalchemy as sa
import streamlit as st
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import pandas as pd
from datetime import datetime, date, timedelta

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

range_mode = st.radio(
    "Time range",
    options=["Preset", "Custom"],
    horizontal=True,
)

now = datetime.now()

if range_mode == "Preset":
    PRESETS = {
        "Last 24 hours": timedelta(hours=24),
        "Last 7 days": timedelta(days=7),
        "Last 30 days": timedelta(days=30),
    }

    label = st.selectbox("Preset range", PRESETS)
    delta = PRESETS[label]

    start_time = now - delta
    end_time = now

else:
    col1, col2 = st.columns(2)

    with col1:
        start_date = st.date_input("Start date")

    with col2:
        end_date = st.date_input("End date")

    start_time = datetime.combine(start_date, datetime.min.time())
    end_time = datetime.combine(end_date, datetime.max.time())

if start_time >= end_time:
    st.error("Start time must be before end time")
    st.stop()

MAX_RANGE = timedelta(days=180)
if end_time - start_time > MAX_RANGE:
    st.warning("Selected range is very large and may be slow.")

engine = sa.create_engine(st.secrets["db_url"])

def build_candles_query(
        interval: str,
        start_time: datetime | date,
        end_time: datetime | date
) -> str:
    return f"""
    SELECT
        date_trunc('{interval}', time) AS time_interval,
        (ARRAY_AGG(price ORDER BY time ASC))[1]  AS open,
        MAX(price)                               AS high,
        MIN(price)                               AS low,
        (ARRAY_AGG(price ORDER BY time DESC))[1] AS close,
        SUM(quantity)                            AS volume
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

figure = make_subplots(rows=2, cols=1, shared_xaxes=True, row_heights=[0.7, 0.3])

figure.add_trace(
        go.Candlestick(
            x=df.index,
            open=df.open, 
            high=df.high, 
            low=df.low,
            close=df.close,
            name='Price',
            increasing_line_color='#26a69a',
            decreasing_line_color='#ef5350'
        ),
        row=1, 
        col=1
)

green_volume_df = df[df['close'] > df['open']]
red_volume_df = df[df['close'] < df['open']]

figure.add_trace(
        go.Bar(
            x=red_volume_df.index,
            y=red_volume_df.volume,
            showlegend=False,
            marker_color='#ef5350'
        ),
        row=2,
        col=1
)

figure.add_trace(
        go.Bar(
            x=green_volume_df.index,
            y=green_volume_df.volume,
            showlegend=False,
            marker_color='#26a69a'
        ),
        row=2,
        col=1
)

figure.update(layout_xaxis_rangeslider_visible=False)
figure.update_layout(title=f'BTC/USDT', yaxis_title=f'Price')
figure.update_yaxes(title_text=f'Volume', row=2, col=1)
figure.update_xaxes(title_text='Date', row=2)

st.subheader(f"{interval_label} Candlesticks")
st.plotly_chart(figure)


