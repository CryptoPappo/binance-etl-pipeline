import pytest
import os
import sys
import time
import pandas as pd
import responses

current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.abspath(os.path.join(current_dir, "..", "src"))
sys.path.append(parent_dir)

from extract import extract, _process_call

def mock_agg_trade(trade_id, time):
    return {
        "a": trade_id,      # Aggregate tradeId
        "p": "0.01633102",  # Price
        "q": "4.70443515",  # Quantity
        "f": 27781,         # First tradeId
        "l": 27781,         # Last tradeId
        "T": time,          # Timestamp
        "m": True,          # Was the buyer the maker?
        "M": True           # Was the trade the best price match?
    }
    

@responses.activate
def test_process_call_single_page():
    """Test that _process_call builds df, returns last timestamp and next trade id"""
    url = "https://api.binance.com/api/v3/aggTrades?symbol=BTCUSDT&starTime=1000&limit=1000"

    mock_data = [
        mock_agg_trade(trade_id=1, time=1000),
        mock_agg_trade(trade_id=2, time=2000)
    ]

    responses.add(
        responses.GET,
        url,
        json=mock_data,
        status=200
    )

    df, last_time, next_trade_id = _process_call(url, pd.DataFrame())

    assert len(df) == 2
    assert last_time == 2000
    assert next_trade_id == 3


@responses.activate
def test_extract_paginates_until_end_time():
    """
    Test extract() stops once start_time >= end_time.
    We simulate two API pages being fetched.
    """

    symbol = "BTCUSDT"
    start_time = 1000
    end_time = 4000  

    # Initial request uses startTime=...
    url_first = f"https://api.binance.com/api/v3/aggTrades?symbol={symbol}&startTime={start_time}&limit=1000"

    # Then extract() continues using fromId=...
    url_second = f"https://api.binance.com/api/v3/aggTrades?symbol={symbol}&fromId=3&limit=1000"
    url_third  = f"https://api.binance.com/api/v3/aggTrades?symbol={symbol}&fromId=5&limit=1000"

    responses.add(
        responses.GET,
        url_first,
        json=[
            mock_agg_trade(trade_id=1, time=1000),
            mock_agg_trade(trade_id=2, time=2000),
        ],
        status=200
    )

    responses.add(
        responses.GET,
        url_second,
        json=[
            mock_agg_trade(trade_id=3, time=2500),
            mock_agg_trade(trade_id=4, time=3000),
        ],
        status=200
    )

    responses.add(
        responses.GET,
        url_third,
        json=[
            mock_agg_trade(trade_id=5, time=5000),
        ],
        status=200
    )

    df = extract(symbol, start_time, end_time=end_time)

    assert len(df) == 5
    assert list(df["a"]) == [1, 2, 3, 4, 5]


@responses.activate
def test_extract_uses_trade_id_if_provided():
    """
    If trade_id is provided, extract() must NOT use startTime.
    It must call the URL with fromId.
    """
    symbol = "BTCUSDT"
    start_time = 0
    trade_id = 100

    url_expected = f"https://api.binance.com/api/v3/aggTrades?symbol={symbol}&fromId={trade_id}&limit=1000"

    responses.add(
        responses.GET,
        url_expected,
        json=[mock_agg_trade(trade_id=100, time=1234)],
        status=200
    )

    df = extract(symbol, start_time, trade_id=trade_id, end_time=1000)

    assert len(df) == 1
    assert df.iloc[0]["a"] == 100
