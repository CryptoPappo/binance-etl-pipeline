import requests
from datetime import datetime
import pandas as pd

def extract(symbol: str, start_time: int):
    url = f"https://api.binance.com/api/v3/aggTrades?symbol={symbol}&startTime={start_time}&limit=1000" 
    data = process_call(url)
    start_time = data[-1]["T"]
    end_time = datetime.now().timestamp() * 1000
    tradeId = data[-1]["a"] + 1
    df = pd.DataFrame(data)

    while start_time < end_time:
        url = f"https://api.binance.com/api/v3/aggTrades?symbol={symbol}&fromId={tradeId}&limit=1000" 
        data = process_call(url)
        df = pd.concat([df, pd.DataFrame(data)], ignore_index=True)
        start_time = data[-1]["T"]
        tradeId = data[-1]["a"] + 1

    return df

def process_call(url: str):
    response = requests.get(url)
    response.raise_for_status()
    data = response.json()

    return data
