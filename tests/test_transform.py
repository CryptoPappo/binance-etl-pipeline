import os
import sys
import pandas as pd

current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.abspath(os.path.join(current_dir, "..", "src"))
sys.path.append(parent_dir)

from transform import transform

def test_convert_trades_to_dataframe():
    raw = [
      {
        "a": 26129,         # Aggregate tradeId
        "p": "0.01633102",  # Price
        "q": "4.70443515",  # Quantity
        "f": 27781,         # First tradeId
        "l": 27781,         # Last tradeId
        "T": 1498793709153, # Timestamp
        "m": True,          # Was the buyer the maker?
        "M": True           # Was the trade the best price match?
      },
      {
        "a": 26130,         # Aggregate tradeId
        "p": "0.01633102",  # Price
        "q": "4.70443515",  # Quantity
        "f": 27781,         # First tradeId
        "l": 27781,         # Last tradeId
        "T": 1498793709153, # Timestamp
        "m": False,          # Was the buyer the maker?
        "M": True           # Was the trade the best price match?
      },
      {
        "a": 26130,         # Aggregate tradeId
        "p": "0.01633102",  # Price
        "q": "4.70443515",  # Quantity
        "f": 27781,         # First tradeId
        "l": 27781,         # Last tradeId
        "T": 1498793709153, # Timestamp
        "m": False,          # Was the buyer the maker?
        "M": True           # Was the trade the best price match?
      }
    ] 
    raw = pd.DataFrame(raw)
    df = transform(raw)

    assert not df.duplicated(subset=["trade_id"]).any()
    assert df["trade_id"][0] == 26129
    assert df["price"][0] == 0.01633102
    assert df["quantity"][0] == 4.70443515
    assert df["quote_qty"][0] == 0.01633102 * 4.70443515
    assert df["time"][0] == pd.to_datetime(1498793709153, unit="ms")
    assert df["order_type"][0] == "Sell"
    assert df["order_type"][1] == "Buy"
    assert len(df.columns) == 6

