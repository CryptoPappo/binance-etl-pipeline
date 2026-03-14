import sqlalchemy as sa
import streamlit as st
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import pandas as pd
import numpy as np
import datetime as dt
from collections.abc import Iterator

range_mode = st.radio(
    "Time range",
    options=["Preset", "Custom"],
    horizontal=True,
)

now = dt.datetime.now(dt.UTC)

if range_mode == "Preset":
    PRESETS = {
        "Last 1 hour": dt.timedelta(hours=1),
        "Last 4 hours": dt.timedelta(hours=4),
        "Last 12 hours": dt.timedelta(hours=12),
        "Last 24 hours": dt.timedelta(hours=24),
    }

    label = st.selectbox("Preset range", PRESETS)
    delta = PRESETS[label]

    start_time = now - delta
    end_time = now

else:
    col1, col2 = st.columns(2)

    with col1:
        start_date = st.datetime_input("Start date")

    with col2:
        end_date = st.datetime_input("End date")

    start_time = dt.datetime.combine(start_date, dt.datetime.min.time())
    end_time = dt.datetime.combine(end_date, dt.datetime.max.time())

if start_time >= end_time:
    st.error("Start time must be before end time")
    st.stop()

MAX_RANGE = dt.timedelta(days=90)
if end_time - start_time > MAX_RANGE:
    st.warning("Selected range is very large and may be slow.")

engine = sa.create_engine(st.secrets["db_url"])

def read_trades_in_chunks(
        engine: sa.Engine,
        start_time: dt.datetime,
        end_time: dt.datetime,
        chunk_size: int = 100000
) -> Iterator[pd.DataFrame]:
    query = sa.text("""
        SELECT
            time,
            CASE
                WHEN order_type = 'Buy'  THEN  1
                WHEN order_type = 'Sell' THEN -1
            END AS sign
        FROM trades
        WHERE time BETWEEN :start_time AND :end_time
        ORDER BY time
    """)
    with engine.connect() as conn:
        for chunk in pd.read_sql(
            query,
            conn,
            params={
                "start_time": start_time,
                "end_time": end_time,
            },
            chunksize=chunk_size,
        ):
            yield chunk

def update_autocorr(signs, buffer, sums, counts):
    n = len(signs)
    for i in range(n):
        valid = buffer != 0
        sums[valid] += signs[i] * buffer[valid]
        counts[valid] += 1
        buffer[:-1] = buffer[1:]
        buffer[-1] = signs[i]

@st.cache_data(ttl=3600)
def load_sign_correlations(engine, start_time, end_time):
    k_max = 100
    buffer = np.zeros(k_max, dtype=np.int8)
    sums = np.zeros(k_max)
    counts = np.zeros(k_max)

    for chunk in read_trades_in_chunks(engine, start, end):
        signs = chunk["sign"].to_numpy(dtype=np.int8)
        update_autocorr(signs, buffer, sums, counts)

    autocorr = sums / counts
    df = pd.DataFrame(
            {
                "lag": np.arange(1, len(k_max)+1),
                "autocorrelation": autocorr
            }
    )
    return df

df_sign = load_sign_correlations(engine, start_time, end_time)
st.write(df_sign)

figure = make_subplots()

figure.add_trace(
        go.Scatter(
            x=df_sign.lag,
            y=df_sign.autocorrelation,
            mode="markers",
            marker_color="red"
        )
)

figure.update(layout_xaxis_rangeslider_visible=False)
figure.update_layout(title="BTC/USDT")
figure.update_yaxes(title_text="Correlation")
figure.update_xaxes(title_text="Lag")

st.subheader("Trade Sign Autocorrelation")
st.plotly_chart(figure)

