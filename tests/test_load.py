import pytest
import os
import sys
from sqlalchemy import create_engine
import pandas as pd

current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.abspath(os.path.join(current_dir, "..", "src"))
sys.path.append(parent_dir)

from load import load

def make_sample_df():
    return pd.DataFrame({
        "trade_id": [12345, 12346],
        "price": [40000.0, 40010.0],
        "quantity": [0.001, 0.002],
        "quote_qty": [40.0, 80.02],
        "time": pd.to_datetime([1609459200000, 1609459260000], unit="ms"),
        "order_type": ["Buy", "Sell"]
    })

def test_load_trades_into_sqlite(tmp_path):
    engine = create_engine("sqlite:///:memory:")
    df = make_sample_df()
    load(df, engine)
    result = pd.read_sql("SELECT * FROM trades", engine)
    
    assert len(result) == 2
    assert result["trade_id"].tolist() == [12345, 12346]
    assert pytest.approx(result["price"].tolist(), rel=1e-6) == [40000.0, 40010.0]

