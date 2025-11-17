import pandas as pd
from sqlalchemy import create_engine
from utils import get_logger
logger = get_logger("load")

def load(df: pd.DataFrame, db_url: str):
    logger.info(f"Loading {len(df)} rows into the database")
    engine = create_engine(db_url)
    df.to_sql("trades", engine, if_exists="append", index=False)
