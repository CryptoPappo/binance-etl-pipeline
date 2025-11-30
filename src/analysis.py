import yaml
import pathlib
from typing import Tuple, Union
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.engine.base import Engine
import mplfinance as mpf
import matplotlib.pyplot as plt
with open("/root/binance-etl-pipeline/config/config.yaml") as f:
    config = yaml.safe_load(f)
db_url = config["database"]["url"]

def load_db(query: str, engine: Engine) -> pd.DataFrame:
    df = pd.read_sql(query, engine)

    return df

def plot_candlesticks(start_time: str, end_time: str, interval: str = 'day',
                      mav: Union[int, Tuple[int], None] = None,  show: bool = True, 
                      savefig: bool = False, path: str = ""):
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
        path = pathlib.Path(path) / f"price_{start_time.replace(' ', '_').replace(':', '-')}_{end_time.replace(' ', '_').replace(':', '-')}_{interval}.pdf"
        mpf.plot(df, type="candle", style="yahoo", figsize=(14, 7), volume=True, savefig=path,
                 mav=mav, title=f"Prices between {start_time} and {end_time} at {interval} interval")

    if show:    
        mpf.plot(df, type="candle", style="yahoo", figsize=(14, 7), volume=True,
                 mav=mav, title=f"Prices between {start_time} and {end_time} at {interval} interval")

def plot_buy_sell_ratio(start_time: str, end_time: str, interval: str = 'day',
                        show: bool = True, savefig: bool = False, path: str = ""):
    engine = create_engine(db_url)
    buy_vol_query = f"""
    SELECT date, SUM(buy_qty) AS buy_volume
    FROM (
        SELECT date_trunc('{interval}', time) AS date,
            CASE 
                WHEN order_type = 'Buy' THEN quantity
                ELSE 0
            END AS buy_qty
        FROM trades
        WHERE time BETWEEN '{start_time}' AND '{end_time}'
    ) b
    GROUP BY date;
    """
    buy_df = pd.read_sql(buy_vol_query, engine)

    sell_vol_query = f"""
    SELECT date, SUM(sell_qty) AS sell_volume
    FROM (
        SELECT date_trunc('{interval}', time) AS date,
            CASE 
                WHEN order_type = 'Sell' THEN quantity
                ELSE 0
            END AS sell_qty
        FROM trades
        WHERE time BETWEEN '{start_time}' AND '{end_time}'
    ) s
    GROUP BY date;
    """
    sell_df = pd.read_sql(sell_vol_query, engine)

    df = pd.merge(buy_df, sell_df, on="date", how="inner")
    df["buy_sell_ratio"] = df["buy_volume"] / df["sell_volume"]
    
    plt.figure(figsize=(10, 6))
    plt.plot(df["date"], df["buy_sell_ratio"], color="blue", linewidth=1.5)
    plt.title(f"Buy/Sell ratio between {start_time} \n and {end_time} at {interval} interval")
    plt.xlabel("Date")
    plt.ylabel("Buy/Sell ratio")
    plt.tight_layout()

    if savefig:
        path += f"/buy_sell_ratio_{start_time.replace(' ', '_').replace(':', '-')}_{end_time.replace(' ', '_').replace(':', '-')}_{interval}.pdf"
        plt.savefig(path, format="pdf")

    if show:
        plt.show()
