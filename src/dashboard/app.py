import os
import sqlalchemy as sa
import streamlit as st
import plotly.graph_objects as go
import pandas as pd

engine = sa.create_engine(st.secrets["db_url"])

script_dir = os.path.dirname(__file__)
sql_dir = os.path.join(script_dir, "../../sql/candlesticks.sql")
with open(sql_dir) as f:
    sql = f.read()

params = {
        "interval": "hour",
        "start_time": "2025-12-01 00:00:00",
        "end_time": "2025-12-02 00:00:00"
}
df = pd.read_sql(sa.text(sql), engine, params=params)
fig = go.Figure(
        data=[
            go.Candlestick(
                x=df["time"],
                open=df["open"],
                high=df["high"],
                low=df["low"],
                close=df["close"]
                )
        ]
)

st.plotly_chart(fig)
