import sqlalchemy as sa
import streamlit as st
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import pandas as pd
import numpy as np
import datetime as dt
from collections.abc import Iterator

CHUNK_SIZE = 1000000

range_mode = st.radio(
    "Time range",
    options=["Preset", "Custom"],
    horizontal=True,
)

now = dt.datetime.now(dt.UTC).replace(minute=0, second=0, microsecond=0)

if range_mode == "Preset":
    PRESETS = {
        "Last 6 hours": dt.timedelta(hours=6),
        "Last 12 hours": dt.timedelta(hours=12),
        "Last 24 hours": dt.timedelta(hours=24),
        "Last 48 hours": dt.timedelta(hours=48)
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

MAX_RANGE = dt.timedelta(days=14)
if end_time - start_time > MAX_RANGE:
    st.warning("Selected range is very large and may be slow.")

st.sidebar.header("Correlation settings")

k_max = st.sidebar.slider(
    "Max lag",
    min_value=1,
    max_value=500,
    value=100
)

r_len = st.sidebar.number_input(
    "Trades per return",
    min_value=1,
    value=100,
    step=1
)

engine = sa.create_engine(st.secrets["db_url"])

def read_trades_in_chunks(
        start_time: dt.datetime,
        end_time: dt.datetime
) -> Iterator[pd.DataFrame]:
    query = sa.text(f"""
        SELECT
            time,
            price,
            CASE
                WHEN order_type = 'Buy'  THEN  1
                WHEN order_type = 'Sell' THEN -1
            END AS sign,
            quantity
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
            chunksize=CHUNK_SIZE,
        ):
            yield chunk

@st.cache_data(ttl=3600)
def load_correlations(start_time, end_time, k_max, r_len):
    autocorr_sign = np.zeros(k_max)
    autocorr_size = np.zeros(k_max)
    autocorr_cross = np.zeros(k_max)
    autocorr_returns = np.zeros(k_max)
    counts = np.zeros(k_max)
    counts_ret = np.zeros(k_max)
    signs = np.empty(CHUNK_SIZE+k_max, dtype=np.float32)
    sizes = np.empty(CHUNK_SIZE+k_max, dtype=np.float32)
    returns = np.empty(CHUNK_SIZE+k_max-r_len, dtype=np.float32)
    signs[:k_max] *= 0.0
    sizes[:k_max] *= 0.0
    returns[:k_max] *= 0.0
    start = 0
    for chunk in read_trades_in_chunks(start_time, end_time):
        n = len(chunk)
        n_ret = n - r_len
        signs[k_max:n+k_max] = chunk["sign"].to_numpy(dtype=np.float32, copy=False)
        sizes[k_max:n+k_max] = signs[k_max:n+k_max] * chunk["quantity"].to_numpy(dtype=np.float32, copy=False)
        if n_ret > 0:
            returns[k_max:n_ret+k_max] = np.abs(
                    np.log(
                        np.divide(
                            chunk["price"].to_numpy(dtype=np.float32, copy=False)[r_len:],
                            chunk["price"].to_numpy(dtype=np.float32, copy=False)[:-r_len]
                        )
                    )
            )

        for i in range(1, k_max+1):
            autocorr_sign[i-1] += np.dot(signs[i:n+k_max], signs[:n+k_max-i])
            autocorr_size[i-1] += np.dot(sizes[i:n+k_max], sizes[:n+k_max-i])
            autocorr_cross[i-1] += np.dot(signs[i:n+k_max], sizes[:n+k_max-i])
            if n_ret > k_max:
                autocorr_returns[i-1] += np.dot(returns[i:n_ret+k_max], returns[:n_ret+k_max-i])
            counts[i-1] += n + start*k_max - i
            counts_ret[i-1] += n + start*k_max - i - r_len

        signs[:k_max] = signs[n:n+k_max]
        sizes[:k_max] = sizes[n:n+k_max]
        if n_ret > 0:
            returns[:k_max] = returns[n_ret:n_ret+k_max]
        start = 1

        del chunk

    df = pd.DataFrame(
            {
                "lag": np.arange(1, k_max+1),
                "autocorr_sign": autocorr_sign / counts,
                "autocorr_size": autocorr_size / counts,
                "autocorr_cross": autocorr_cross / counts,
                "autocorr_returns": autocorr_returns / counts_ret
            }
    )
    return df

if st.button("Run analysis"):
    df = load_correlations(start_time, end_time, k_max, r_len)

    figure = make_subplots()

    figure.add_trace(
            go.Scatter(
                x=df.lag,
                y=df.autocorr_sign,
                mode="markers",
                marker_color="red"
            )
    )

    figure.update(layout_xaxis_rangeslider_visible=False)
    figure.update_layout(title="BTC/USDT")
    figure.update_yaxes(title_text="Correlation")
    figure.update_xaxes(title_text="Lag")
    figure.update_xaxes(type="log")
    figure.update_yaxes(type="log")

    st.subheader("Trade Sign Autocorrelation")
    st.plotly_chart(figure)

    figure = make_subplots()

    figure.add_trace(
            go.Scatter(
                x=df.lag,
                y=df.autocorr_size,
                mode="markers",
                marker_color="red"
            )
    )

    figure.update(layout_xaxis_rangeslider_visible=False)
    figure.update_layout(title="BTC/USDT")
    figure.update_yaxes(title_text="Correlation")
    figure.update_xaxes(title_text="Lag")
    figure.update_xaxes(type="log")
    figure.update_yaxes(type="log")

    st.subheader("Trade Size Autocorrelation")
    st.plotly_chart(figure)

    figure = make_subplots()

    figure.add_trace(
            go.Scatter(
                x=df.lag,
                y=df.autocorr_cross,
                mode="markers",
                marker_color="red"
            )
    )

    figure.update(layout_xaxis_rangeslider_visible=False)
    figure.update_layout(title="BTC/USDT")
    figure.update_yaxes(title_text="Correlation")
    figure.update_xaxes(title_text="Lag")
    figure.update_xaxes(type="log")
    figure.update_yaxes(type="log")

    st.subheader("Trade Sign-Size Cross-Correlation")
    st.plotly_chart(figure)

    figure = make_subplots()

    figure.add_trace(
            go.Scatter(
                x=df.lag,
                y=df.autocorr_returns,
                mode="markers",
                marker_color="red"
            )
    )

    figure.update(layout_xaxis_rangeslider_visible=False)
    figure.update_layout(title="BTC/USDT")
    figure.update_yaxes(title_text="Correlation")
    figure.update_xaxes(title_text="Lag")
    figure.update_xaxes(type="log")
    figure.update_yaxes(type="log")

    st.subheader("Absolute Returns Autocorrelation")
    st.plotly_chart(figure)
