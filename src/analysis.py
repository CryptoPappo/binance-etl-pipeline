import yaml
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.engine.base import Engine
import matplotlib.pyplot as plt
import matplotlib.dates as mpdates
import mplfinance as mpf
with open("/root/binance-etl-pipeline/config/config.yaml") as f:
    config = yaml.safe_load(f)
db_url = config["database"]["url"]

def load_db(query: str, engine: Engine) -> pd.DataFrame:
    df = pd.read_sql(query, engine)

    return df

def plot_candlesticks(start_time: str, end_time: str, interval: str = 'day',
                      show: bool = True, savefig: bool = False, path: str = ""):
    engine = create_engine(db_url)
    open_query = f"""
    SELECT date, price AS open
    FROM (
        SELECT 
            date_trunc('{interval}', time) AS date,
            price,
            ROW_NUMBER() OVER (PARTITION BY date_trunc('{interval}', time)
            ORDER BY time ASC) AS row_number
        FROM trades
        WHERE time BETWEEN '{start_time}' AND '{end_time}'
    ) t
    WHERE t.row_number = 1;
    """
    open_df = pd.read_sql(open_query, engine)
    
    close_query = f"""
    SELECT date, price AS close
    FROM (
        SELECT 
            date_trunc('{interval}', time) AS date,
            price,
            ROW_NUMBER() OVER (PARTITION BY date_trunc('{interval}', time)
            ORDER BY time DESC) AS row_number
        FROM trades
        WHERE time BETWEEN '{start_time}' AND '{end_time}'
    ) t
    WHERE t.row_number = 1;
    """
    close_df = pd.read_sql(close_query, engine)
    df = pd.merge(open_df, close_df, on="date", how="inner")

    high_low_query = f"""
    SELECT date_trunc('{interval}', time) AS date, MIN(price) AS low, MAX(price) AS high
    FROM trades
    WHERE time BETWEEN '{start_time}' AND '{end_time}'
    GROUP BY date_trunc('{interval}', time);
    """
    high_low_df = pd.read_sql(high_low_query, engine)
    df = pd.merge(df, high_low_df, on="date", how="inner")

    vol_query = f"""
    SELECT date_trunc('{interval}', time) AS date, SUM(quantity) AS volume
    FROM trades
    WHERE time BETWEEN '{start_time}' AND '{end_time}' 
    GROUP BY date_trunc('{interval}', time);
    """
    vol_df = pd.read_sql(vol_query, engine)
    df = pd.merge(df, vol_df, on="date", how="inner")
    df = df.set_index("date")
    df = df[["open", "high", "low", "close", "volume"]]
    
    if savefig:
        mpf.plot(df, type="candle", style="yahoo", figsize=(14, 7), volume=True, savefig=path,
                 title=f"Prices between {start_time} and {end_time} at {interval} interval")

    if show:    
        mpf.plot(df, type="candle", style="yahoo", figsize=(14, 7), volume=True,
                 title=f"Prices between {start_time} and {end_time} at {interval} interval")

    return df
