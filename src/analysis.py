import yaml
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.engine.base import Engine
import matplotlib.pyplot as plt
import matplotlib.dates as mpdates
from mplfinance.original_flavor import candlestick_ohlc
with open("/root/binance-etl-pipeline/config/config.yaml") as f:
    config = yaml.safe_load(f)
db_url = config["database"]["url"]

def load_db(query: str, engine: Engine) -> pd.DataFrame:
    df = pd.read_sql(query, engine)

    return df

def plot_candlesticks(start_time: str, end_time: str, interval: str = 'day',
                      show: bool = True, savefig: bool = False,
                      path: str = ""):
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
    df = pd.merge(open_df, close_df, on='date', how='inner')

    high_low_query = f"""
    SELECT date_trunc('{interval}', time) AS date, MIN(price) AS low, MAX(price) AS high
    FROM trades
    WHERE time BETWEEN '{start_time}' AND '{end_time}'
    GROUP BY date_trunc('{interval}', time);
    """
    high_low_df = pd.read_sql(high_low_query, engine)
    df = pd.merge(df, high_low_df, on='date', how='inner')
    df = df[["date", "open", "high", "low", "close"]]
    df["date"] = df["date"].map(mpdates.date2num)

    fig, ax = plt.subplots()
    
    width = 0.6
    if interval == "hour":
        width /= 24
    elif interval == "minute":
        width /= 24*60
    elif interval == "second":
        width /= 24*60*60

    candlestick_ohlc(ax, df.values, width=width, colorup="green", colordown="red")
    
    ax.set_xlabel("Date")
    ax.set_ylabel("Price")
    plt.title(f"Prices between {start_time} and {end_time} \n at {interval} interval")

    date_format = mpdates.DateFormatter("%Y-%m-%d")
    ax.xaxis.set_major_formatter(date_format)
    fig.autofmt_xdate()

    fig.tight_layout()

    if show:
        plt.show()

    if savefig:
        plt.savefig(f"{path}price_{start_time[:10]}_{end_time[:10]}_{interval}.pdf", 
                    format="pdf")

    return df
