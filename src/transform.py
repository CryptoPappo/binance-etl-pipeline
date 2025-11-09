import pandas as pd

def transform(df: pd.DataFrame):
    df["p"] = pd.to_numeric(df["p"], errors="coerce")
    df["q"] = pd.to_numeric(df["q"], errors="coerce")
    df["m"] = df["m"].map({True: "Sell", False: "Buy"}).astype("category")
    df = df.loc[:, ["a", "p", "q", "T", "m"]]
    df = df.rename(columns={"a": "Trade_id", "p": "Price", "q": "Quantity", "T": "Timestamp", "m": "Order_type"})

    return df
    

