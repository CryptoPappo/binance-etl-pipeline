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

st.sidebar.header("Bins/Correlation settings")

bins_size = st.sidebar.slider(
    "Number of bins",
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

def get_bins(
        start_time: dt.datetime,
        end_time: dt.datetime,
        r_len: int,
        bins: int
) -> dict[str, np.ndarray]:
    query = sa.text(f"""
        WITH cte AS (
            SELECT
                (LEAD(EXTRACT(EPOCH FROM time), 1) OVER (ORDER BY time) - EXTRACT(EPOCH FROM time)) * 1000 AS time_dif,
                ABS(LN(LEAD(price, :r_len) OVER (ORDER BY time) / price)) AS returns,
                quantity
            FROM trades
            WHERE time BETWEEN :start_time AND :end_time
        )
        SELECT
            MIN(time_dif) AS time_dif_min,
            MAX(time_dif) AS time_dif_max,
            MIN(returns) AS returns_min,
            MAX(returns) AS returns_max,
            MIN(quantity) AS quantity_min,
            MAX(quantity) AS quantity_max
        FROM cte;
    """)
    
    with engine.connect() as conn:
        df = pd.read_sql(
            query,
            conn,
            params={
                "r_len": r_len,
                "start_time": start_time,
                "end_time": end_time,
            }
        )
        return {
                "time_dif": np.linspace(df.time_dif_min[0], df.time_dif_max[0], bins+1),
                "returns": np.linspace(df.returns_min[0], df.returns_max[0], bins+1),
                "quantity": np.linspace(df.quantity_min[0], df.quantity_max[0], bins+1),
        }

def read_trades_in_chunks(
        start_time: dt.datetime,
        end_time: dt.datetime
) -> Iterator[pd.DataFrame]:
    query = sa.text(f"""
        SELECT
            EXTRACT(EPOCH FROM time) * 1000 AS seconds,
            price,
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
def load_histograms(start_time, end_time, r_len, bins_size):
    bins = get_bins(start_time, end_time, r_len, bins_size)

    counts_time = np.zeros(bins_size)
    counts_qty = np.zeros(bins_size)
    counts_ret = np.zeros(bins_size)

    time_dif = np.empty(CHUNK_SIZE-1, dtype=np.float32)
    returns = np.empty(CHUNK_SIZE-r_len, dtype=np.float32)

    hist_time = np.empty(bins_size)
    hist_qty = np.empty(bins_size)
    hist_ret = np.empty(bins_size)
    for chunk in read_trades_in_chunks(start_time, end_time):
        n = len(chunk)
        n_ret = n - r_len

        if n_ret > 0:
            returns[:n_ret] = np.abs(
                    np.log(
                        np.divide(
                            chunk["price"].to_numpy(dtype=np.float32, copy=False)[r_len:],
                            chunk["price"].to_numpy(dtype=np.float32, copy=False)[:-r_len]
                        )
                    )
            )
            hist_ret[:], _ = np.histogram(
                    returns[:n_ret],
                    bins=bins["returns"]
            )
            counts_ret[:] += hist_ret[:]

        time_dif[:n-1] = np.subtract(
                chunk["seconds"].to_numpy(dtype=np.float64, copy=False)[1:],
                chunk["seconds"].to_numpy(dtype=np.float64, copy=False)[:-1]
        )

        hist_time[:], _ = np.histogram(
                time_dif[:n-1],
                bins=bins["time_dif"]
        )
        hist_qty[:], _ = np.histogram(
                chunk["quantity"].to_numpy(dtype=np.float32, copy=False),
                bins=bins["quantity"]
        )

        counts_time[:] += hist_time[:]
        counts_qty[:] += hist_qty[:]

        del chunk

    df = pd.DataFrame(
            {
                "time_dif": counts_time,
                "quantity": counts_qty,
                "returns": counts_ret,
            }
    )
    return df, bins

if st.button("Run analysis"):
    df, bins = load_histograms(start_time, end_time, r_len, bins_size)

    figure = make_subplots()

    bins_time = 0.5 * (bins["time_dif"][:-1] + bins["time_dif"][1:])
    figure.add_trace(
            go.Scatter(
                x=bins_time,
                y=df.time_dif,
                mode="markers",
                marker_color="red"
            )
    )

    figure.update(layout_xaxis_rangeslider_visible=False)
    figure.update_layout(title="BTC/USDT")
    figure.update_yaxes(title_text="Count")
    figure.update_xaxes(title_text="Time Difference ms")
    figure.update_xaxes(type="log")
    figure.update_yaxes(type="log")

    st.subheader("Trade Time Difference Histogram")
    st.plotly_chart(figure)

    figure = make_subplots()
    
    bins_qty = 0.5 * (bins["quantity"][:-1] + bins["quantity"][1:])
    figure.add_trace(
            go.Scatter(
                x=bins_qty,
                y=df.quantity,
                mode="markers",
                marker_color="red"
            )
    )

    figure.update(layout_xaxis_rangeslider_visible=False)
    figure.update_layout(title="BTC/USDT")
    figure.update_yaxes(title_text="Count")
    figure.update_xaxes(title_text="Trade Size")
    figure.update_xaxes(type="log")
    figure.update_yaxes(type="log")

    st.subheader("Trade Size Histogram")
    st.plotly_chart(figure)

    figure = make_subplots()
    
    bins_ret = 0.5 * (bins["returns"][:-1] + bins["returns"][1:])
    figure.add_trace(
            go.Scatter(
                x=bins_ret,
                y=df.returns,
                mode="markers",
                marker_color="red"
            )
    )

    figure.update(layout_xaxis_rangeslider_visible=False)
    figure.update_layout(title="BTC/USDT")
    figure.update_yaxes(title_text="Count")
    figure.update_xaxes(title_text="Returns")
    figure.update_xaxes(type="log")
    figure.update_yaxes(type="log")

    st.subheader("Absolute Logarithmic Returns Histogram")
    st.plotly_chart(figure)
