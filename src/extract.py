from typing import Tuple, Union
import requests
import time
import pandas as pd
from utils import get_logger
logger = get_logger("extract")

def extract(symbol: str, start_time: int, tradeId: Union[int, None]) -> pd.DataFrame:
    logger.info(f"Extracting trades for {symbol} from timestamp {start_time}")    
    if tradeId is None:
        url = f"https://api.binance.com/api/v3/aggTrades?symbol={symbol}&startTime={start_time}&limit=1000" 
    else:
        url = f"https://api.binance.com/api/v3/aggTrades?symbol={symbol}&fromId={tradeId}&limit=1000" 

    df, start_time, tradeId = process_call(url, pd.DataFrame())
    end_time = time.time() * 1000

    while start_time < end_time:
        logger.info(f"Extracting trades for {symbol} from timestamp {start_time}")
        url = f"https://api.binance.com/api/v3/aggTrades?symbol={symbol}&fromId={tradeId}&limit=1000" 
        df, start_time, tradeId = process_call(url, df)

    return df

def process_call(url: str, df: pd.DataFrame) -> Tuple[pd.DataFrame, int, int]:
    response = requests.get(url)
    response.raise_for_status()
    data = response.json()

    df = pd.concat([df, pd.DataFrame(data)], ignore_index=True)
    last_time = data[-1]["T"]
    last_tradeId = data[-1]["a"] + 1

    return df, last_time, last_tradeId
