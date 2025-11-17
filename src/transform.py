import pandas as pd

def transform(df: pd.DataFrame) -> pd.DataFrame:
    df["p"] = pd.to_numeric(df["p"], errors="coerce")
    df["q"] = pd.to_numeric(df["q"], errors="coerce")
    df["m"] = df["m"].map({True: "Sell", False: "Buy"}).astype("category")
    df["T"] = pd.to_datetime(df["T"], unit="ms")
    df["quote_qty"] = df["q"] * df["p"]
    df = df.loc[:, ["a", "p", "q", "quote_qty", "T", "m"]]
    df = df.rename(columns={"a": "trade_id", "p": "price", "q": "quantity", "T": "time", "m": "order_type"})
    df = df.drop_duplicates(subset=["trade_id"])

    return df
    

