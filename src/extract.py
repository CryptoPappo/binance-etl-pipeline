import requests
from datetime import datetime
import pandas as pd

def extract(symbol: str, start_time: int):
    end_time = datetime.now().timestamp() * 1000
    df = pd.DataFrame()

    while start_time < end_time:
        url = f"https://api.binance.com/api/v3/aggTrades?symbol={symbol}&startTime={start_time}&limit=1000" 
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()
        df = pd.concat([df, pd.DataFrame(data)], ignore_index=True)
        start_time = data[-1]["T"]

    return df
