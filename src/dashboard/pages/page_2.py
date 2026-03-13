import sqlalchemy as sa
import streamlit as st
import plotly.graph_objects as go
import pandas as pd
from datetime import datetime, timedelta

range_mode = st.radio(
    "Time range",
    options=["Preset", "Custom"],
    horizontal=True,
)

now = datetime.now()

if range_mode == "Preset":
    PRESETS = {
        "Last 1 hour": timedelta(hours=1),
        "Last 4 hours": timedelta(hours=4),
        "Last 12 hours": timedelta(hours=12),
        "Last 24 hours": timedelta(hours=24),
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

    start_time = datetime.combine(start_date, datetime.min.time())
    end_time = datetime.combine(end_date, datetime.max.time())

if start_time >= end_time:
    st.error("Start time must be before end time")
    st.stop()

MAX_RANGE = timedelta(days=90)
if end_time - start_time > MAX_RANGE:
    st.warning("Selected range is very large and may be slow.")

engine = sa.create_engine(st.secrets["db_url"])

def build_sign_correlations_query(
        start_time: datetime,
        end_time: datetime
) -> str:
    return f"""
    WITH signed_trades AS (
        SELECT
            time,
            CASE
                WHEN order_type = 'Buy'  THEN  1
                WHEN order_type = 'Sell' THEN -1
            END AS sign
        FROM trades
        WHERE time BETWEEN '{start_time}' AND '{end_time}'
    )
    SELECT
        lag,
        AVG(sign * sign_lag) AS autocorrelation
    FROM (
        SELECT
            sign,
            LAG(sign, lag) OVER (ORDER BY time) AS sign_lag,
            lag
        FROM signed_trades,
             generate_series(1, 100) AS lag
    ) t
    WHERE sign_lag IS NOT NULL
    GROUP BY lag
    ORDER BY lag;
    """

@st.cache_data(ttl=3600)
def load_sign_correlations(start_time, end_time):
    query = build_sign_correlations_query(start_time, end_time)
    return pd.read_sql(query, engine)

df_sign = load_sign_correlations(start_time, end_time)

figure = go.Scatter(
        x=df_sign.lag,
        y=df_sign.autocorrelation,
        marker_color="red"
)

figure.update(layout_xaxis_rangeslider_visible=False)
figure.update_layout(title="BTC/USDT")
figure.update_yaxes(title_text="Correlation")
figure.update_xaxes(title_text="Lag")

st.subheader("Trade Sign Autocorrelation")
st.plotly_chart(figure)

