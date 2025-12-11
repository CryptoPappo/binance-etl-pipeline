import yaml
import pathlib
from typing import Tuple, Union
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.engine.base import Engine
import mplfinance as mpf
import matplotlib.pyplot as plt
import numpy as np
from scipy.optimize import curve_fit
with open("/root/binance-etl-pipeline/config/config.yaml") as f:
    config = yaml.safe_load(f)
db_url = config["database"]["url"]

def load_db(query: str, engine: Engine) -> pd.DataFrame:
    df = pd.read_sql(query, engine)

    return df

def plot_candlesticks(start_time: str, end_time: str, interval: str = "day",
                      mav: Union[int, Tuple[int], None] = None,  show: bool = True, 
                      savefig: bool = False, path: str = "", format: str = "pdf"):
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
    df = df.sort_values(by="date")
    df = df.set_index("date")
    df = df[["open", "high", "low", "close", "volume"]]
    
    if savefig:
        path = pathlib.Path(path) / f"price_{start_time.replace(' ', '_').replace(':', '-')}_{end_time.replace(' ', '_').replace(':', '-')}_{interval}.{format}"
        mpf.plot(df, type="candle", style="yahoo", figsize=(14, 7), volume=True, savefig=path,
                 mav=mav, title=f"Prices between {start_time} and {end_time} at {interval} interval")

    if show:    
        mpf.plot(df, type="candle", style="yahoo", figsize=(14, 7), volume=True,
                 mav=mav, title=f"Prices between {start_time} and {end_time} at {interval} interval")

def plot_returns(start_time: str, end_time: str, interval: str = "day",
                 bins: int = 5, log: bool = True, show: bool = True, 
                 savefig: bool = False, path: str = "", format: str = "pdf"):
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
    df = df.sort_values(by="date")
    df["returns"] = np.log(df["close"]/df["open"])
    counts, bins = np.histogram(df["returns"].abs(), bins=bins)

    plt.figure(figsize=(10, 6))
    if log:
        plt.loglog(bins[1:], counts, "o", color="black")
    else:
        plt.plot(bins[1:], counts, "o", color="black")
    plt.title(f"Returns histogram between {start_time} \n and {end_time} at {interval} interval")
    plt.tight_layout()

    if savefig:
        path += f"/returns_{start_time.replace(' ', '_').replace(':', '-')}_{end_time.replace(' ', '_').replace(':', '-')}_{interval}.{format}"
        plt.savefig(path, format=format)

    if show:
        plt.show()  

def plot_volume_histogram(start_time: str, end_time: str, interval: str = "day",
                          bins: int = 5, log: bool = True, show: bool = True, 
                          savefig: bool = False, path: str = "", format: str = "pdf"):
    engine = create_engine(db_url) 
    vol_query = f"""
    SELECT date_trunc('{interval}', time) AS date, SUM(quantity) AS volume
    FROM trades
    WHERE time BETWEEN '{start_time}' AND '{end_time}' 
    GROUP BY date_trunc('{interval}', time);
    """
    df = pd.read_sql(vol_query, engine)
    counts, bins = np.histogram(df["volume"], bins=bins)

    plt.figure(figsize=(10, 6))
    if log:
        plt.loglog(bins[1:], counts, "o", color="black")
    else:
        plt.plot(bins[1:], counts, "o", color="black")
    plt.title(f"Volume histogram between {start_time} \n and {end_time} at {interval} interval")
    plt.tight_layout()

    if savefig:
        path += f"/volume_{start_time.replace(' ', '_').replace(':', '-')}_{end_time.replace(' ', '_').replace(':', '-')}_{interval}.{format}"
        plt.savefig(path, format=format)

    if show:
        plt.show()
 

def plot_price_by_qty(start_time: str, end_time: str, percentile: float = 0.99,
                      show: bool = True, savefig: bool = False, path: str = "",
                      format: str = "png", markersize: int = 2):
    engine = create_engine(db_url)
    query = f"""
    SELECT time, price, order_type
    FROM trades
    WHERE time BETWEEN '{start_time}' AND '{end_time}' AND
        quantity >= (
            SELECT percentile_cont({percentile}) WITHIN GROUP (ORDER BY quantity ASC)
            FROM trades
            WHERE time BETWEEN '{start_time}' AND '{end_time}'
        )
    ORDER BY time;
    """
    df = pd.read_sql(query, engine)
    buy_mask = df["order_type"] == "Buy"
    sell_mask = df["order_type"] == "Sell"

    plt.figure(figsize=(10, 6))
    plt.plot(df.loc[buy_mask, "time"], df.loc[buy_mask, "price"], "o", color="green",
             ms=markersize, fillstyle="none")
    plt.plot(df.loc[sell_mask, "time"], df.loc[sell_mask, "price"], "o", color="red",
             ms=markersize, fillstyle="none")
    plt.title(f"Price for trade sizes over {percentile} percentile between {start_time} \n and {end_time}")
    plt.xlabel("Date")
    plt.ylabel("Price")
    plt.tight_layout()

    if savefig:
        path += f"/price_by_qty_{start_time.replace(' ', '_').replace(':', '-')}_{end_time.replace(' ', '_').replace(':', '-')}.{format}"
        plt.savefig(path, format=format)

    if show:
        plt.show()   

def plot_price_by_abs_qty(start_time: str, end_time: str, quantity: float = 2.0,
                          show: bool = True, savefig: bool = False, path: str = "",
                          format: str = "png", markersize: int = 2):
    engine = create_engine(db_url)
    query = f"""
    SELECT time, price, order_type
    FROM trades
    WHERE time BETWEEN '{start_time}' AND '{end_time}' AND
        quantity >= {quantity}
    ORDER BY time;
    """
    df = pd.read_sql(query, engine)
    buy_mask = df["order_type"] == "Buy"
    sell_mask = df["order_type"] == "Sell"

    plt.figure(figsize=(10, 6))
    plt.plot(df.loc[buy_mask, "time"], df.loc[buy_mask, "price"], "o", color="green",
             ms=markersize, fillstyle="none")
    plt.plot(df.loc[sell_mask, "time"], df.loc[sell_mask, "price"], "o", color="red",
             ms=markersize, fillstyle="none")
    plt.title(f"Price for trade sizes over {quantity} between {start_time} \n and {end_time}")
    plt.xlabel("Date")
    plt.ylabel("Price")
    plt.tight_layout()

    if savefig:
        path += f"/price_by_abs_qty_{start_time.replace(' ', '_').replace(':', '-')}_{end_time.replace(' ', '_').replace(':', '-')}.{format}"
        plt.savefig(path, format=format)

    if show:
        plt.show()   


def plot_buy_sell_ratio(start_time: str, end_time: str, interval: str = "day",
                        show: bool = True, savefig: bool = False, path: str = "",
                        format: str = "pdf"):
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
    df = df.sort_values(by="date")
    
    plt.figure(figsize=(10, 6))
    plt.plot(df["date"], df["buy_sell_ratio"], color="blue", linewidth=1.5)
    plt.title(f"Buy/Sell ratio between {start_time} \n and {end_time} at {interval} interval")
    plt.xlabel("Date")
    plt.ylabel("Buy/Sell ratio")
    plt.tight_layout()

    if savefig:
        path += f"/buy_sell_ratio_{start_time.replace(' ', '_').replace(':', '-')}_{end_time.replace(' ', '_').replace(':', '-')}_{interval}.{format}"
        plt.savefig(path, format=format)

    if show:
        plt.show()

def plot_boxplot(start_time: str, end_time: str, interval: str = "day", 
                 upper_quant: float = 1.0, log: bool = False, show: bool = True, 
                 savefig: bool = False, path: str = "", format: str = "png"):
    engine = create_engine(db_url)
    query = f"""
    SELECT date_trunc('{interval}', time) AS date, quantity
    FROM trades
    WHERE time BETWEEN '{start_time}' AND '{end_time}'
    ORDER BY time;
    """
    df = pd.read_sql(query, engine)
    
    if interval == "day":
        frequency = "D"
    elif interval == "hour":
        frequency = "h"
    elif interval == "minute":
        frequency = "min"
    elif interval == "second":
        frequency = "s"
    else:
        raise Exception(f"Invalid interval. (interval={interval})")
   
    df["date"] = df["date"].dt.floor(frequency)
    max_value = df["quantity"].quantile(upper_quant)
    df["quantity"] = df["quantity"].clip(upper=max_value)
    if log:
        df["quantity"] = np.log(df["quantity"])

    fig, ax = plt.subplots(figsize=(10, 6)) 
    boxplot = df.boxplot(by="date", column=["quantity"], ax=ax, rot=45)
    fig_ = boxplot.get_figure()
    fig_.suptitle("")
    ax.set_title(f"Boxplot of trades between {start_time} \n and {end_time} at {interval} interval")
    ax.set_xlabel("Date")
    ax.set_ylabel("Trades size")
    plt.tight_layout()

    if savefig:
        path += f"/boxplot_{start_time.replace(' ', '_').replace(':', '-')}_{end_time.replace(' ', '_').replace(':', '-')}_{interval}.{format}"
        plt.savefig(path, format=format)
    
    if show:
        plt.show()

def plot_histogram(start_time: str, end_time: str, bins: int = 100, 
                   upper_quant: float = 1.0, log: bool = False, show: bool = True, 
                   savefig: bool = False, path : str = "", format: str = "pdf"):
    engine = create_engine(db_url)
    query = f"""
    SELECT time AS date, quantity
    FROM trades
    WHERE time BETWEEN '{start_time}' AND '{end_time}';
    """
    df = pd.read_sql(query, engine)
    if log:
        df["quantity"] = np.log(df["quantity"])

    fig, ax = plt.subplots(figsize=(10, 6))
    ax.set_title(f"Histogram of trades between {start_time} \n and {end_time}")
    _ = ax.hist(df["quantity"], bins=bins, density=True, range=(0, df["quantity"].quantile(upper_quant)))
    plt.tight_layout()

    if savefig:
        path += f"/histogram_{start_time.replace(' ', '_').replace(':', '-')}_{end_time.replace(' ', '_').replace(':', '-')}.{format}"
        plt.savefig(path, format=format)
    
    if show:
        plt.show()

def plot_buy_sell_histogram(start_time: str, end_time: str, bins: int = 100,
                            lower_quant: float = 0.0, upper_quant: float = 1.0,
                            show: bool = True, savefig: bool = False, path : str = "",
                            format: str = "pdf"):
    engine = create_engine(db_url)
    query = f"""
    SELECT time AS date,
        CASE
            WHEN order_type = 'Buy' THEN quantity
            ELSE -quantity
        END AS quantity
    FROM trades
    WHERE time BETWEEN '{start_time}' AND '{end_time}';
    """
    df = pd.read_sql(query, engine)
    buy_df = df[df["quantity"] >= 0.0]
    sell_df = df[df["quantity"] <= 0.0]
    df = df.sort_values(by="date")

    fig, ax = plt.subplots(figsize=(10, 6))
    ax.set_title(f"Histogram of trades between {start_time} \n and {end_time}")
    _ = ax.hist(buy_df["quantity"], color="green", bins=bins, density=True, range=(0, buy_df["quantity"].quantile(upper_quant)))
    _ = ax.hist(sell_df["quantity"], color="red", bins=bins, density=True, range=(sell_df["quantity"].quantile(lower_quant), 0))
    plt.tight_layout()

    if savefig:
        path += f"/buy_sell_histogram_{start_time.replace(' ', '_').replace(':', '-')}_{end_time.replace(' ', '_').replace(':', '-')}.{format}"
        plt.savefig(path, format=format)
    
    if show:
        plt.show()

def plot_vol_delta(start_time: str, end_time: str, show: bool = True,
                   savefig: bool = False, path : str = "",
                   format: str = "pdf"):
    engine = create_engine(db_url)
    query = f"""
    SELECT time AS date,
        CASE
            WHEN order_type = 'Buy' THEN quantity
            ELSE -quantity
        END AS quantity
    FROM trades
    WHERE time BETWEEN '{start_time}' AND '{end_time}'
    ORDER BY time;
    """
    df = pd.read_sql(query, engine)
    df["cvd"] = df["quantity"].cumsum()

    fig, ax = plt.subplots(figsize=(10, 6))
    ax.set_title(f"Cumulative volume delta between {start_time} \n and {end_time}")
    _ = ax.plot(df["date"], df["cvd"], color="black", linewidth=1.5)
    plt.tight_layout()

    if savefig:
        path += f"/cvd_{start_time.replace(' ', '_').replace(':', '-')}_{end_time.replace(' ', '_').replace(':', '-')}.{format}"
        plt.savefig(path, format=format)
    
    if show:
        plt.show()

def plot_tick_imbalance(start_time: str, end_time: str, show: bool = True,
                        savefig: bool = False, path : str = "", format: str = "pdf"):
    engine = create_engine(db_url)
    query = f"""
    SELECT time AS date,
        CASE
            WHEN order_type = 'Buy' THEN 1
            ELSE -1
        END AS quantity
    FROM trades
    WHERE time BETWEEN '{start_time}' AND '{end_time}'
    ORDER BY time;
    """
    df = pd.read_sql(query, engine)
    df["tick_imbalance"] = df["quantity"].cumsum()

    fig, ax = plt.subplots(figsize=(10, 6))
    ax.set_title(f"Tick imbalance between {start_time} \n and {end_time}")
    _ = ax.plot(df["date"], df["tick_imbalance"], color="black", linewidth=1.5)
    plt.tight_layout()

    if savefig:
        path += f"/tick_imbalance_{start_time.replace(' ', '_').replace(':', '-')}_{end_time.replace(' ', '_').replace(':', '-')}.{format}"
        plt.savefig(path, format=format)
    
    if show:
        plt.show()

def plot_correlation_funcs(start_time: str, end_time: str, corr_distance: int = 100,
                           log: bool = False, show: bool = True, savefig: bool = False, 
                           path : str = "", format: str = "pdf"):
    engine = create_engine(db_url)
    query = f"""
    SELECT time AS date, quantity, 
        CASE 
            WHEN order_type = 'Buy' THEN 1
            ELSE -1
        END signed_qty
    FROM trades
    WHERE time BETWEEN '{start_time}' AND '{end_time}'
    ORDER BY time;
    """
    df = pd.read_sql(query, engine)
    corr_size = np.ones(corr_distance)
    corr_sign = np.ones(corr_distance)
    x = np.arange(0, corr_distance)
    
    for i in range(1, corr_distance):
        corr_size[i] = df["quantity"].autocorr(lag=i)
        corr_sign[i] = df["signed_qty"].autocorr(lag=i)

    fig, ax = plt.subplots(2, 1, figsize=(10, 6), sharex=True)
    fig.suptitle(f"Correlation functions between {start_time} \n and {end_time}")
    ax[0].set_title("Trade size correlation function")
    ax[1].set_title("Trade sign correlation function")

    if log:
        _ = ax[0].loglog(x[1:], corr_size[1:], "o", color="blue")
        _ = ax[1].loglog(x[1:], corr_sign[1:], "o", color="orange")
    else:
        _ = ax[0].plot(x, corr_size, "o", color="blue")
        _ = ax[1].plot(x, corr_sign, "o", color="orange")

    if savefig:
        path += f"/corr_funcs_{start_time.replace(' ', '_').replace(':', '-')}_{end_time.replace(' ', '_').replace(':', '-')}.{format}"
        plt.savefig(path, format=format)
    
    if show:
        plt.show()

def autocorr_lag(x: pd.Series, max_lag: int = 100) -> np.ndarray:
    autocorr = np.zeros(max_lag)
    autocorr[0] = 1.0
    for lag in range(1, max_lag):
        if len(x) - 1 > lag:
            autocorr[lag] = x.autocorr(lag=lag)
        else:
            break

    return autocorr

def correlation_func(x, exp, corr_length):
    return x**(-exp)*np.exp(-x/corr_length)

def fit_func(x, y):
    popt, pcov = curve_fit(correlation_func, x[1:], y[1:])
    err = np.sqrt(np.diag(pcov))
    
    return np.array([popt[1], err[1]])

def plot_correlation_length(start_time: str, end_time: str, corr_distance: int = 100,
                            interval: str = "day", show: bool = True, savefig: bool = False,
                            path : str = "", format: str = "pdf"):
    engine = create_engine(db_url)
    query = f"""
    SELECT date_trunc('{interval}', time) AS date, 
        CASE
            WHEN order_type = 'Buy' THEN 1
            ELSE -1
        END AS signed_qty
    FROM trades
    WHERE time BETWEEN '{start_time}' AND '{end_time}'
    ORDER BY time;
    """
    df = pd.read_sql(query, engine)
    corr_df = df.groupby("date")["signed_qty"].apply(lambda x: autocorr_lag(x, corr_distance))
    x = np.arange(0, corr_distance)
    corr_df = corr_df.apply(lambda y: fit_func(x, y))
    corr_err = np.array([corr_df.values[i] for i in range(corr_df.size)])
    time = corr_df.index.to_numpy()

    fig, ax = plt.subplots(figsize=(10, 6))
    ax.set_title(f"Correlation length between {start_time} \n and {end_time}")
    ax.set_xlabel("Date")
    ax.set_ylabel("Correlation length")
    _ = ax.plot(time, corr_err[:,0], "o", color="black", linestyle=None)
    _ = ax.errorbar(time, corr_err[:,0], yerr=corr_err[:,1], color="black", linestyle=None)

    if savefig:
        path += f"/corr_length_{start_time.replace(' ', '_').replace(':', '-')}_{end_time.replace(' ', '_').replace(':', '-')}_{interval}.{format}"
        plt.savefig(path, format=format)

    if show:
        plt.show()
