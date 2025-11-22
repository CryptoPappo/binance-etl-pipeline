import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.engine.base import Engine
from utils import get_logger
logger = get_logger("load")

def load(df: pd.DataFrame, engine: Engine):
    """

    Load aggregated trades data into PostgreSQL database.


    Args:
        df: Pandas dataframe of the transformed aggregated trades.
        engine: SQLAlchemy engine used to connect to the database.
    """
    logger.info(f"Loading {len(df)} rows into the database")
    df.to_sql("trades", engine, if_exists="append", index=False)
