import pandas as pd
from sqlalchemy import create_engine

def load(df, db_url):
    engine = create_engine(db_url)
    df.to_sql("trades", engine, if_exists="append", index=False)
