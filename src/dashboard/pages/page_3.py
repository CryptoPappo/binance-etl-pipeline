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

engine = sa.create_engine(st.secrets["db_url"])

def get_bins(
        start_time: dt.datetime,
        end_time: dt.datetime,
        k_max: int,
        bins: int
) -> dict[str, np.ndarray]:
    query = sa.text(f"""
        WITH cte AS (
            SELECT
                EXTRACT(EPOCH FROM (LEAD(time, 1) OVER (ORDER BY time) - time)) AS time_dif,
                ABS(LN(LEAD(price, {k_max}) OVER (ORDER BY time) / price)) AS returns,
                CASE 
                    WHEN order_type = 'Buy' THEN quantity
                    WHEN order_type = 'Sell' THEN -quantity
                END AS signed_qty
            FROM trades
            WHERE time BETWEEN :start_time AND :end_time
        )
        SELECT 
            MAX(time_dif) AS time_dif_max,
            MAX(returns) AS returns_max,
            MIN(signed_qty) AS signed_qty_min,
            MAX(signed_qty) AS signed_qty_max
        FROM cte;
    """)
    
    with engine.connect() as conn:
        df = pd.read_sql(
            query,
            conn,
            params={
                "start_time": start_time,
                "end_time": end_time,
            }
        )
        qty_max = max(df["signed_qty_min"].abs()[0], df["signed_qty_max"][0])
        return {
                "time_dif": np.linspace(0.0, df["time_dif_max"], bins),
                "returns": np.linspace(0.0, df["returns_max"], bins),
                "sells_qty": np.linspace(df["signed_qty_min"], 0.0, bins),
                "buys_qty": np.linspace(0.0, df["signed_qty_max"], bins),
                "quantity": np.linspace(0.0, qty_max, bins)
        }

def read_trades_in_chunks(
        start_time: dt.datetime,
        end_time: dt.datetime
) -> Iterator[pd.DataFrame]:
    query = sa.text(f"""
        SELECT
            EXTRACT(EPOCH FROM time) AS seconds,
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
def load_histograms(start_time, end_time):
    k_max = 100
    bins_size = 100
    bins = get_bins(start_time, end_time, k_max, bins_size)
    st.write(bins)
    counts_time = np.zeros(bins_size)
    counts_sign = np.zeros(bins_size)

    time_dif = np.empty(CHUNK_SIZE-1, dtype=np.float32)
    signed_qty = np.empty(CHUNK_SIZE, dtype=np.float32)

    hist_time = np.empty(bins_size)
    hist_sign = np.empty(bins_size)
    for chunk in read_trades_in_chunks(start_time, end_time):
        n = len(chunk)
        time_dif[:n] = np.subtract(
                chunk["seconds"].to_numpy(dtype=np.float32, copy=False)[1:],
                chunk["seconds"].to_numpy(dtype=np.float32, copy=False)[:-1]
        )
        signed_qty[:n] = np.multiply(
                chunk["sign"].to_numpy(dtype=np.float32, copy=False),
                chunk["quantity"].to_numpy(dtype=np.float32, copy=False)
        )

        hist_time[:], _ = np.histogram(time_dif[:n], bins=bins["time_dif"])
        hist_sign[:], _ = np.histogram(signed_qty[:n], bins=bins["signed_qty"])

        counts_time += hist_time
        counts_sign += hist_sign

        del chunk

    df = pd.DataFrame(
            {
                "bins": bins,
                "time_dif": counts_time,
                "signed_qty": counts_sign
            }
    )
    return df

if st.button("Run analysis"):
    df = load_histograms(start_time, end_time)

    figure = make_subplots()

    figure.add_trace(
            go.Bar(
                x=df.bins["time_dif"],
                y=df.time_dif,
                marker_color="#26a69a"
            )
    )

    figure.update(layout_xaxis_rangeslider_visible=False)
    figure.update_layout(title="BTC/USDT")
    figure.update_yaxes(title_text="Count")
    figure.update_xaxes(title_text="Time Difference")
    figure.update_xaxes(type="log")
    figure.update_yaxes(type="log")

    st.subheader("Trade Time Difference Histogram")
    st.plotly_chart(figure)


